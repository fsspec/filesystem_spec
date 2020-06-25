import asyncio
import functools
import inspect
import re
import sys
import threading

from .utils import other_paths

# this global variable holds whether this thread is running async or not
thread_state = threading.local()
private = re.compile("_[^_]")


def sync(loop, func, *args, callback_timeout=None, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """

    e = threading.Event()
    main_tid = threading.get_ident()
    result = [None]
    error = [False]

    async def f():
        try:
            if main_tid == threading.get_ident():
                raise RuntimeError("sync() called from thread of running loop")
            await asyncio.sleep(0)
            thread_state.asynchronous = True
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = asyncio.wait_for(future, callback_timeout)
            result[0] = await future
        except Exception as exc:
            error[0] = sys.exc_info()
        finally:
            thread_state.asynchronous = False
            e.set()

    asyncio.run_coroutine_threadsafe(f(), loop=loop)
    if callback_timeout is not None:
        if not e.wait(callback_timeout):
            raise TimeoutError("timed out after %s s." % (callback_timeout,))
    else:
        while not e.is_set():
            e.wait(10)
    if error[0]:
        typ, exc, tb = error[0]
        raise exc.with_traceback(tb)
    else:
        return result[0]


async def _run_as_coroutine(func, *args, **kwargs):
    return func(*args, **kwargs)


def sync_wrapper(func, obj=None):
    """Given a function, make so can be called in async or bocking contexts

    Leave obj=None if defining within a class. Pass the instance if attaching
    as an attribute of the instance.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or args[0]
        loop = self.loop
        # second condition below triggers if this is running in the thread of the
        # event loop *during* the call to sync(), i.e., while running
        # asynchronously
        if self.asynchronous or getattr(thread_state, "asynchronous", False):
            if inspect.iscoroutinefunction(func):
                # directly make awaitable and return is
                return func(*args, **kwargs)
            else:
                # make awaitable which then calls the blocking function
                return _run_as_coroutine(func, *args, **kwargs)
        else:
            if inspect.iscoroutinefunction(func):
                # run the awaitable on the loop
                return sync(loop, func, *args, **kwargs)
            else:
                # just call the blocking function
                return func(*args, **kwargs)

    return wrapper


def async_wrapper(func):
    """Run a sync function on the event loop"""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def get_loop():
    """Create a running loop in another thread"""
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever)
    t.daemon = True
    t.start()
    return loop


# these methods should be implemented as async by any async-able backend
async_methods = ['_ls', '_expand_path', '_info', '_isfile', '_isdir',
                 '_exists', '_walk', '_glob', '_find', '_du', '_cat', '_get_file'
                 '_put_file', '_rm_file']


class AsyncFileSystem:
    async_impl = True

    async def _rm(self, path, recursive=False):
        path = await self._expand_path(path, recursive=recursive)
        await asyncio.gather(
            *[
                self._rm_file(p)
                for p in path
            ]
        )

    async def _mcat(self, paths, recursive=False):
        """Fetch multiple paths' contents"""
        paths = await self._expand_path(paths, recursive=recursive)
        out = await asyncio.gather(
            *[asyncio.ensure_future(self._cat(path), loop=self.loop) for path in paths]
        )
        return {k: v for k, v in zip(paths, out)}

    async def _put(self, lpath, rpath, recursive=False, **kwargs):
        """Copy file(s) from local.

        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        Calls put_file for each source.
        """
        from .implementations.local import make_path_posix, LocalFileSystem

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        rpaths = other_paths(lpaths, rpath)

        await asyncio.gather(
            *[
                self._put_file(lpath, rpath, **kwargs)
                for lpath, rpath in zip(lpaths, rpaths)
            ]
        )

    async def _get(self, rpath, lpath, recursive=False, **kwargs):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        Calls get_file for each source.
        """
        from fsspec.implementations.local import make_path_posix

        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)
        rpaths = await self._expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        await asyncio.gather(
            *[
                self._get_file(rpath, lpath, **kwargs)
                for lpath, rpath in zip(lpaths, rpaths)
            ]
        )


def make_sync_methods(obj):
    """Populate sync and async methods for obj

    Uses the methods specified in async_methods (to be implemented) and AsyncFileSystem
    (where default implementations are available)
    """

    for method in async_methods + dir(AsyncFileSystem):
        if private.match(method):
            if inspect.iscoroutinefunction(getattr(obj, method, None)):
                setattr(obj, method[1:], sync_wrapper(getattr(obj, method), obj=obj))
            elif hasattr(obj, method[1:]) and inspect.ismethod(getattr(obj, method[1:])):
                setattr(obj, method, async_wrapper(getattr(obj, method[1:])))
