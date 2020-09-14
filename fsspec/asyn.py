import asyncio
import functools
import inspect
import re
import os
import sys
import threading

from .utils import other_paths
from .spec import AbstractFileSystem

# this global variable holds whether this thread is running async or not
thread_state = threading.local()
private = re.compile("_[^_]")


def _run_until_done(coro):
    """execute coroutine, when already in the event loop"""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    asyncio.tasks._unregister_task(task)
    current_task = asyncio.tasks._current_tasks.get(loop)
    assert task == current_task
    del asyncio.tasks._current_tasks[loop]
    runner = loop.create_task(coro)
    while not runner.done():
        loop._run_once()
    asyncio.tasks._current_tasks[loop] = task
    return runner.result()


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
            # run coroutine while pausing this one (because we are within async)
            return _run_until_done(func(*args, **kwargs))
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
    # This is not currently used
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
# the sync methods below all call expand_path, which in turn may call walk or glob
# (if passed paths with glob characters, or for recursive=True, respectively)
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


class AsyncFileSystem(AbstractFileSystem):
    """Async file operations, default implementations

    Passes bulk operations to asyncio.gather for concurrent operation.

    Implementations that have concurrent batch operations and/or async methods
    should inherit from this class instead of AbstractFileSystem. Docstrings are
    copied from the un-underscored method in AbstractFileSystem, if not given.
    """

    # note that methods do not have docstring here; they will be copied
    # for _* methods and inferred for overridden methods.

    async_impl = True

    def __init__(self, *args, asynchronous=False, loop=None, **kwargs):
        self.asynchronous = asynchronous
        self.loop = loop or get_loop()
        super().__init__(*args, **kwargs)

    async def _rm(self, path, recursive=False, **kwargs):
        await asyncio.gather(*[self._rm_file(p, **kwargs) for p in path])

    def rm(self, path, recursive=False, **kwargs):
        path = self.expand_path(path, recursive=recursive)
        sync(self.loop, self._rm, path, **kwargs)

    async def _copy(self, paths, path2, **kwargs):
        await asyncio.gather(
            *[self._cp_file(p1, p2, **kwargs) for p1, p2 in zip(paths, path2)]
        )

    def copy(self, path1, path2, recursive=False, **kwargs):
        paths = self.expand_path(path1, recursive=recursive)
        path2 = other_paths(paths, path2)
        sync(self.loop, self._copy, paths, path2, **kwargs)

    async def _pipe(self, path, value=None, **kwargs):
        if isinstance(path, str):
            path = {path: value}
        await asyncio.gather(
            *[self._pipe_file(k, v, **kwargs) for k, v in path.items()]
        )

    async def _cat(self, paths, **kwargs):
        return await asyncio.gather(
            *[
                asyncio.ensure_future(self._cat_file(path, **kwargs), loop=self.loop)
                for path in paths
            ]
        )

    def cat(self, path, recursive=False, **kwargs):
        paths = self.expand_path(path, recursive=recursive)
        out = sync(self.loop, self._cat, paths, **kwargs)
        if (
            len(paths) > 1
            or isinstance(path, list)
            or paths[0] != self._strip_protocol(path)
        ):
            return {k: v for k, v in zip(paths, out)}
        else:
            return out[0]

    async def _put(self, lpaths, rpaths, **kwargs):
        return await asyncio.gather(
            *[
                self._put_file(lpath, rpath, **kwargs)
                for lpath, rpath in zip(lpaths, rpaths)
            ]
        )

    def put(self, lpath, rpath, recursive=False, **kwargs):
        from .implementations.local import make_path_posix, LocalFileSystem

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        rpaths = other_paths(lpaths, rpath)
        sync(self.loop, self._put, lpaths, rpaths, **kwargs)

    async def _get(self, rpaths, lpaths, **kwargs):
        return await asyncio.gather(
            *[
                self._get_file(rpath, lpath, **kwargs)
                for lpath, rpath in zip(lpaths, rpaths)
            ]
        )

    def get(self, rpath, lpath, recursive=False, **kwargs):
        from fsspec.implementations.local import make_path_posix

        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)
        rpaths = self.expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        [os.makedirs(os.path.dirname(lp), exist_ok=True) for lp in lpaths]
        return sync(self.loop, self._get, rpaths, lpaths)


def mirror_sync_methods(obj):
    """Populate sync and async methods for obj

    For each method will create a sync version if the name refers to an async method
    (coroutine) and there is no override in the child class; will create an async
    method for the corresponding sync method if there is no implementation.

    Uses the methods specified in
    - async_methods: the set that an implementation is expected to provide
    - default_async_methods: that can be derived from their sync version in
      AbstractFileSystem
    - AsyncFileSystem: async-specific default coroutines
    """
    from fsspec import AbstractFileSystem

    for method in async_methods + default_async_methods + dir(AsyncFileSystem):
        if not method.startswith("_"):
            continue
        smethod = method[1:]
        if private.match(method):
            isco = inspect.iscoroutinefunction(getattr(obj, method, None))
            unsync = getattr(getattr(obj, smethod, False), "__func__", None)
            is_default = unsync is getattr(AbstractFileSystem, smethod, "")
            if isco and is_default:
                mth = sync_wrapper(getattr(obj, method), obj=obj)
                setattr(obj, smethod, mth)
                if not mth.__doc__:
                    mth.__doc__ = getattr(
                        getattr(AbstractFileSystem, smethod, None), "__doc__", ""
                    )
            elif (
                hasattr(obj, smethod)
                and inspect.ismethod(getattr(obj, smethod))
                and not hasattr(obj, method)
            ):
                setattr(obj, method, async_wrapper(getattr(obj, smethod)))
