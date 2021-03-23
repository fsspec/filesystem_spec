import asyncio
import functools
import inspect
import os
import re
import threading

from .spec import AbstractFileSystem
from .utils import is_exception, other_paths

private = re.compile("_[^_]")
lock = threading.Lock()


def _run_until_done(loop, coro):
    """execute coroutine, when already in the event loop"""
    # raise Nested
    with lock:
        task = asyncio.current_task(loop=loop)
        if task:
            asyncio.tasks._unregister_task(task)
        asyncio.tasks._current_tasks.pop(loop, None)
    runner = loop.create_task(coro)
    try:
        while not runner.done():
            try:
                loop._run_once()
            except (IndexError, RuntimeError):
                pass
    finally:
        if task:
            with lock:
                asyncio.tasks._current_tasks[loop] = task
    return runner.result()


def sync(loop, func, *args, callback_timeout=None, **kwargs):
    """
    Make loop run coroutine until it returns. Runs in this thread
    """
    coro = func(*args, **kwargs)
    if loop.is_running():
        result = _run_until_done(loop, coro)
    else:
        result = loop.run_until_complete(coro)
    return result


def maybe_sync(func, self, *args, **kwargs):
    """Make function call into coroutine or maybe run

    If we are running async, run coroutine on current loop until done;
    otherwise runs it on the loop (if is a coroutine already) or directly. Will guess
    we are running async if either "self" has an attribute asynchronous which is True,
    or thread_state does (this gets set in ``sync()`` itself, to avoid nesting loops).
    """
    loop = self.loop
    try:
        loop0 = asyncio.get_event_loop()
    except RuntimeError:
        loop0 = None
    if loop0 is not None and loop0.is_running():
        if inspect.iscoroutinefunction(func):
            # run coroutine while pausing this one (because we are within async)
            return _run_until_done(loop, func(*args, **kwargs))
        else:
            # make awaitable which then calls the blocking function
            raise NotImplementedError()
    else:
        if inspect.iscoroutinefunction(func):
            # run the awaitable on the loop
            return sync(loop, func, *args, **kwargs)
        else:
            # just call the blocking function
            return func(*args, **kwargs)


def sync_wrapper(func, obj=None):
    """Given a function, make so can be called in async or blocking contexts

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
    """Get/Create an event loop to run in this thread"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
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
        self._loop = threading.local()
        self._pid = os.getpid()
        if loop is not None:
            self._loop.loop = loop
        super().__init__(*args, **kwargs)

    @property
    def loop(self):
        if os.getpid() != self._pid:
            raise RuntimeError("This class is not fork-safe")
        if not hasattr(self._loop, "loop"):
            self._loop.loop = get_loop()
        return self._loop.loop

    async def _rm(self, path, recursive=False, **kwargs):
        await asyncio.gather(*[self._rm_file(p, **kwargs) for p in path])

    def rm(self, path, recursive=False, **kwargs):
        path = self.expand_path(path, recursive=recursive)
        maybe_sync(self._rm, self, path, **kwargs)

    async def _copy(self, paths, path2, **kwargs):
        return await asyncio.gather(
            *[self._cp_file(p1, p2, **kwargs) for p1, p2 in zip(paths, path2)],
            return_exceptions=True,
        )

    def copy(
        self, path1, path2, recursive=False, on_error=None, maxdepth=None, **kwargs
    ):
        if on_error is None and recursive:
            on_error = "ignore"
        elif on_error is None:
            on_error = "raise"

        paths = self.expand_path(path1, maxdepth=maxdepth, recursive=recursive)
        path2 = other_paths(paths, path2)
        result = maybe_sync(self._copy, self, paths, path2, **kwargs)

        for ex in filter(is_exception, result):
            if on_error == "ignore" and isinstance(ex, FileNotFoundError):
                continue
            raise ex

    async def _pipe(self, path, value=None, **kwargs):
        if isinstance(path, str):
            path = {path: value}
        await asyncio.gather(
            *[self._pipe_file(k, v, **kwargs) for k, v in path.items()]
        )

    async def _cat(self, paths, **kwargs):
        return await asyncio.gather(
            *[self._cat_file(path, **kwargs) for path in paths],
            return_exceptions=True,
        )

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        paths = self.expand_path(path, recursive=recursive)
        out = maybe_sync(self._cat, self, paths, **kwargs)
        if on_error == "raise":
            ex = next(filter(is_exception, out), False)
            if ex:
                raise ex
        if (
            len(paths) > 1
            or isinstance(path, list)
            or paths[0] != self._strip_protocol(path)
        ):
            return {
                k: v
                for k, v in zip(paths, out)
                if on_error != "omit" or not is_exception(v)
            }
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
        from .implementations.local import LocalFileSystem, make_path_posix

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        rpaths = other_paths(lpaths, rpath)
        maybe_sync(self._put, self, lpaths, rpaths, **kwargs)

    async def _get(self, rpaths, lpaths, **kwargs):
        dirs = [os.path.dirname(lp) for lp in lpaths]
        [os.makedirs(d, exist_ok=True) for d in dirs]
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
