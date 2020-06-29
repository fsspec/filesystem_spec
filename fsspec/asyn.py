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
        except Exception:
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


def maybe_sync(func, self, *args, **kwargs):
    """Make function call into coroutine or maybe run

    If we are running async, returns the coroutine object so that it can be awaited;
    otherwise runs it on the loop (if is a coroutine already) or directly. Will guess
    we are running async if either "self" has an attribute asynchronous which is True,
    or thread_state does (this gets set in ``sync()`` itself, to avoid nesting loops).
    """
    loop = self.loop
    # second condition below triggers if this is running in the thread of the
    # event loop *during* the call to sync(), i.e., while running
    # asynchronously
    if getattr(self, "asynchronous", False) or getattr(
        thread_state, "asynchronous", False
    ):
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
        return maybe_sync(func, self, *args, **kwargs)

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
async_methods = [
    "_ls",
    "_cat_file",
    "_get_file",
    "_put_file",
    "_rm_file",
    "_cp_file",
    "_pipe_file",
]
# these methods could be overridden, but have default sync versions which rely on _ls
default_async_methods = [
    "_expand_path",
    "_info",
    "_isfile",
    "_isdir",
    "_exists",
    "_walk",
    "_glob",
    "_find",
    "_du",
]


class AsyncFileSystem:
    """Async file operations, default implementations

    Passes bulk operations to asyncio.gather for concurrent operartion.
    """

    async_impl = True

    async def _rm(self, path, recursive=False):
        """Delete files
        """
        path = await self._expand_path(path, recursive=recursive)
        await asyncio.gather(*[self._rm_file(p) for p in path])

    async def _copy(self, path1, path2, recursive=False, **kwargs):
        """ Copy within two locations in the filesystem"""
        paths = await self.expand_path(path1, recursive=recursive)
        path2 = other_paths(paths, path2)
        await asyncio.gather(
            *[self._cp_file(p1, p2, **kwargs) for p1, p2 in zip(paths, path2)]
        )

    async def _pipe(self, path, value=None, **kwargs):
        """Set contents of files
        """
        if isinstance(path, str):
            path = {path: value}
        await asyncio.gather(
            *[self._pipe_file(k, v, **kwargs) for k, v in path.items()]
        )

    async def _cat(self, path, recursive=False, **kwargs):
        """Get contents of files
        """
        paths = await self._expand_path(path, recursive=recursive)
        out = await asyncio.gather(
            *[
                asyncio.ensure_future(self._cat_file(path, **kwargs), loop=self.loop)
                for path in paths
            ]
        )
        if len(paths) > 1 or isinstance(path, list) or paths[0] != path:
            return {k: v for k, v in zip(paths, out)}
        else:
            return out[0]

    async def _put(self, lpath, rpath, recursive=False, **kwargs):
        """copy local files to remote
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


def mirror_sync_methods(obj):
    """Populate sync and async methods for obj

    For each method will create a sync version if the name refers to an async method
    (coroutine) and there is no override in the child class; will create an async
    method for the corresponding sync method if there is no implementation.

    Uses the methods specified in
    - async_methods: the set that an implementation is expected to provide
    - default_async_methods: that can be derived from their sync version ini AbstractFileSystem
    - AsyncFileSystem: async-specific default implementations
    """
    from fsspec import AbstractFileSystem

    for method in async_methods + default_async_methods + dir(AsyncFileSystem):
        smethod = method[1:]
        if private.match(method):
            if inspect.iscoroutinefunction(getattr(obj, method, None)) and getattr(
                obj, smethod, False
            ).__func__ is getattr(AbstractFileSystem, smethod):
                setattr(obj, smethod, sync_wrapper(getattr(obj, method), obj=obj))
            elif hasattr(obj, smethod) and inspect.ismethod(getattr(obj, smethod)):
                setattr(obj, method, async_wrapper(getattr(obj, smethod)))
