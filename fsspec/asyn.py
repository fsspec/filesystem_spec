import asyncio
import asyncio.events
import functools
import inspect
import os
import re
import sys
import threading
from contextlib import contextmanager
from glob import has_magic

from .callbacks import _DEFAULT_CALLBACK
from .exceptions import FSTimeoutError
from .spec import AbstractFileSystem
from .utils import PY36, is_exception, other_paths

private = re.compile("_[^_]")


async def _runner(event, coro, result, timeout=None):
    timeout = timeout if timeout else None  # convert 0 or 0.0 to None
    if timeout is not None:
        coro = asyncio.wait_for(coro, timeout=timeout)
    try:
        result[0] = await coro
    except Exception as ex:
        result[0] = ex
    finally:
        event.set()


if PY36:
    grl = asyncio.events._get_running_loop
else:
    grl = asyncio.events.get_running_loop


def sync(loop, func, *args, timeout=None, **kwargs):
    """
    Make loop run coroutine until it returns. Runs in other thread
    """
    timeout = timeout if timeout else None  # convert 0 or 0.0 to None
    # NB: if the loop is not running *yet*, it is OK to submit work
    # and we will wait for it
    if loop is None or loop.is_closed():
        raise RuntimeError("Loop is not running")
    try:
        loop0 = grl()
        if loop0 is loop:
            raise NotImplementedError("Calling sync() from within a running loop")
    except RuntimeError:
        pass
    coro = func(*args, **kwargs)
    result = [None]
    event = threading.Event()
    asyncio.run_coroutine_threadsafe(_runner(event, coro, result, timeout), loop)
    while True:
        # this loops allows thread to get interrupted
        if event.wait(1):
            break
        if timeout is not None:
            timeout -= 1
            if timeout < 0:
                raise FSTimeoutError

    return_result = result[0]
    if isinstance(return_result, asyncio.TimeoutError):
        # suppress asyncio.TimeoutError, raise FSTimeoutError
        raise FSTimeoutError from return_result
    elif isinstance(return_result, BaseException):
        raise return_result
    else:
        return return_result


iothread = [None]  # dedicated fsspec IO thread
loop = [None]  # global event loop for any non-async instance
lock = threading.Lock()  # for setting exactly one thread


def sync_wrapper(func, obj=None):
    """Given a function, make so can be called in async or blocking contexts

    Leave obj=None if defining within a class. Pass the instance if attaching
    as an attribute of the instance.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = obj or args[0]
        return sync(self.loop, func, *args, **kwargs)

    return wrapper


@contextmanager
def _selector_policy():
    original_policy = asyncio.get_event_loop_policy()
    try:
        if (
            sys.version_info >= (3, 8)
            and os.name == "nt"
            and hasattr(asyncio, "WindowsSelectorEventLoopPolicy")
        ):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        yield
    finally:
        asyncio.set_event_loop_policy(original_policy)


def get_running_loop():
    if hasattr(asyncio, "get_running_loop"):
        return asyncio.get_running_loop()
    else:
        loop = asyncio._get_running_loop()
        if loop is None:
            raise RuntimeError("no running event loop")
        else:
            return loop


def get_loop():
    """Create or return the default fsspec IO loop

    The loop will be running on a separate thread.
    """
    if loop[0] is None:
        with lock:
            # repeat the check just in case the loop got filled between the
            # previous two calls from another thread
            if loop[0] is None:
                with _selector_policy():
                    loop[0] = asyncio.new_event_loop()
                th = threading.Thread(target=loop[0].run_forever, name="fsspecIO")
                th.daemon = True
                th.start()
                iothread[0] = th
    return loop[0]


@contextmanager
def fsspec_loop():
    """Temporarily switch the current event loop to the fsspec's
    own loop, and then revert it back after the context gets
    terinated.
    """
    try:
        original_loop = get_running_loop()
    except RuntimeError:
        original_loop = None

    fsspec_loop = get_loop()
    try:
        asyncio._set_running_loop(fsspec_loop)
        yield fsspec_loop
    finally:
        asyncio._set_running_loop(original_loop)


try:
    import resource
except ImportError:
    resource = None
    ResourceError = OSError
else:
    ResourceEror = resource.error

_DEFAULT_BATCH_SIZE = 128
_NOFILES_DEFAULT_BATCH_SIZE = 1280


def _get_batch_size(nofiles=False):
    from fsspec.config import conf

    if nofiles:
        if "nofiles_gather_batch_size" in conf:
            return conf["nofiles_gather_batch_size"]
    else:
        if "gather_batch_size" in conf:
            return conf["gather_batch_size"]
    if nofiles:
        return _NOFILES_DEFAULT_BATCH_SIZE
    if resource is None:
        return _DEFAULT_BATCH_SIZE

    try:
        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ImportError, ValueError, ResourceError):
        return _DEFAULT_BATCH_SIZE

    if soft_limit == resource.RLIM_INFINITY:
        return -1
    else:
        return soft_limit // 8


async def _run_coros_in_chunks(
    coros,
    batch_size=None,
    callback=_DEFAULT_CALLBACK,
    timeout=None,
    return_exceptions=False,
    nofiles=False,
):
    """Run the given coroutines in  chunks.

    Parameters
    ----------
    coros: list of coroutines to run
    batch_size: int or None
        Number of coroutines to submit/wait on simultaneously.
        If -1, then it will not be any throttling. If
        None, it will be inferred from _get_batch_size()
    callback: fsspec.callbacks.Callback instance
        Gets a relative_update when each coroutine completes
    timeout: number or None
        If given, each coroutine times out after this time. Note that, since
        there are multiple batches, the total run time of this function will in
        general be longer
    return_exceptions: bool
        Same meaning as in asyncio.gather
    nofiles: bool
        If inferring the batch_size, does this operation involve local files?
        If yes, you normally expect smaller batches.
    """

    if batch_size is None:
        batch_size = _get_batch_size(nofiles=nofiles)

    if batch_size == -1:
        batch_size = len(coros)

    assert batch_size > 0
    results = []
    for start in range(0, len(coros), batch_size):
        chunk = [
            asyncio.Task(asyncio.wait_for(c, timeout=timeout))
            for c in coros[start : start + batch_size]
        ]
        if callback is not _DEFAULT_CALLBACK:
            [
                t.add_done_callback(
                    lambda *_, **__: callback.call("relative_update", 1)
                )
                for t in chunk
            ]
        results.extend(
            await asyncio.gather(*chunk, return_exceptions=return_exceptions),
        )
    return results


# these methods should be implemented as async by any async-able backend
async_methods = [
    "_ls",
    "_cat_file",
    "_get_file",
    "_put_file",
    "_rm_file",
    "_cp_file",
    "_pipe_file",
    "_expand_path",
    "_info",
    "_isfile",
    "_isdir",
    "_exists",
    "_walk",
    "_glob",
    "_find",
    "_du",
    "_size",
    "_mkdir",
    "_makedirs",
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
    disable_throttling = False

    def __init__(self, *args, asynchronous=False, loop=None, batch_size=None, **kwargs):
        self.asynchronous = asynchronous
        self._pid = os.getpid()
        if not asynchronous:
            self._loop = loop or get_loop()
        else:
            self._loop = None
        self.batch_size = batch_size
        super().__init__(*args, **kwargs)

    @property
    def loop(self):
        if self._pid != os.getpid():
            raise RuntimeError("This class is not fork-safe")
        return self._loop

    async def _rm_file(self, path, **kwargs):
        raise NotImplementedError

    async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
        # TODO: implement on_error
        batch_size = batch_size or self.batch_size
        path = await self._expand_path(path, recursive=recursive)
        return await _run_coros_in_chunks(
            [self._rm_file(p, **kwargs) for p in path],
            batch_size=batch_size,
            nofiles=True,
        )

    async def _cp_file(self, path1, path2, **kwargs):
        raise NotImplementedError

    async def _copy(
        self,
        path1,
        path2,
        recursive=False,
        on_error=None,
        maxdepth=None,
        batch_size=None,
        **kwargs,
    ):
        if on_error is None and recursive:
            on_error = "ignore"
        elif on_error is None:
            on_error = "raise"

        paths = await self._expand_path(path1, maxdepth=maxdepth, recursive=recursive)
        path2 = other_paths(paths, path2)
        batch_size = batch_size or self.batch_size
        coros = [self._cp_file(p1, p2, **kwargs) for p1, p2 in zip(paths, path2)]
        result = await _run_coros_in_chunks(
            coros, batch_size=batch_size, return_exceptions=True, nofiles=True
        )

        for ex in filter(is_exception, result):
            if on_error == "ignore" and isinstance(ex, FileNotFoundError):
                continue
            raise ex

    async def _pipe(self, path, value=None, batch_size=None, **kwargs):
        if isinstance(path, str):
            path = {path: value}
        batch_size = batch_size or self.batch_size
        return await _run_coros_in_chunks(
            [self._pipe_file(k, v, **kwargs) for k, v in path.items()],
            batch_size=batch_size,
            nofiles=True,
        )

    async def _process_limits(self, url, start, end):
        """Helper for "Range"-based _cat_file"""
        size = None
        suff = False
        if start is not None and start < 0:
            # if start is negative and end None, end is the "suffix length"
            if end is None:
                end = -start
                start = ""
                suff = True
            else:
                size = size or (await self._info(url))["size"]
                start = size + start
        elif start is None:
            start = 0
        if not suff:
            if end is not None and end < 0:
                if start is not None:
                    size = size or (await self._info(url))["size"]
                    end = size + end
            elif end is None:
                end = ""
            if isinstance(end, int):
                end -= 1  # bytes range is inclusive
        return "bytes=%s-%s" % (start, end)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        raise NotImplementedError

    async def _cat(
        self, path, recursive=False, on_error="raise", batch_size=None, **kwargs
    ):
        paths = await self._expand_path(path, recursive=recursive)
        coros = [self._cat_file(path, **kwargs) for path in paths]
        batch_size = batch_size or self.batch_size
        out = await _run_coros_in_chunks(
            coros, batch_size=batch_size, nofiles=True, return_exceptions=True
        )
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

    async def _cat_ranges(
        self, paths, starts, ends, max_gap=None, batch_size=None, **kwargs
    ):
        # TODO: on_error
        if max_gap is not None:
            # use utils.merge_offset_ranges
            raise NotImplementedError
        if not isinstance(paths, list):
            raise TypeError
        if not isinstance(starts, list):
            starts = [starts] * len(paths)
        if not isinstance(ends, list):
            ends = [starts] * len(paths)
        if len(starts) != len(paths) or len(ends) != len(paths):
            raise ValueError
        coros = [
            self._cat_file(p, start=s, end=e, **kwargs)
            for p, s, e in zip(paths, starts, ends)
        ]
        batch_size = batch_size or self.batch_size
        return await _run_coros_in_chunks(coros, batch_size=batch_size, nofiles=True)

    async def _put_file(self, lpath, rpath, **kwargs):
        raise NotImplementedError

    async def _put(
        self,
        lpath,
        rpath,
        recursive=False,
        callback=_DEFAULT_CALLBACK,
        batch_size=None,
        **kwargs,
    ):
        """Copy file(s) from local.

        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        The put_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit .
        """
        from .implementations.local import LocalFileSystem, make_path_posix

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        rpaths = other_paths(
            lpaths, rpath, exists=isinstance(rpath, str) and await self._isdir(rpath)
        )

        is_dir = {l: os.path.isdir(l) for l in lpaths}
        rdirs = [r for l, r in zip(lpaths, rpaths) if is_dir[l]]
        file_pairs = [(l, r) for l, r in zip(lpaths, rpaths) if not is_dir[l]]

        await asyncio.gather(*[self._makedirs(d, exist_ok=True) for d in rdirs])
        batch_size = batch_size or self.batch_size

        coros = []
        callback.call("set_size", len(file_pairs))
        for lfile, rfile in file_pairs:
            callback.branch(lfile, rfile, kwargs)
            coros.append(self._put_file(lfile, rfile, **kwargs))

        return await _run_coros_in_chunks(
            coros, batch_size=batch_size, callback=callback
        )

    async def _get_file(self, rpath, lpath, **kwargs):
        raise NotImplementedError

    async def _get(
        self, rpath, lpath, recursive=False, callback=_DEFAULT_CALLBACK, **kwargs
    ):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        The get_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit .
        """
        from fsspec.implementations.local import make_path_posix

        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)
        rpaths = await self._expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        [os.makedirs(os.path.dirname(lp), exist_ok=True) for lp in lpaths]
        batch_size = kwargs.pop("batch_size", self.batch_size)

        coros = []
        callback.lazy_call("set_size", len, lpaths)
        for lpath, rpath in zip(lpaths, rpaths):
            callback.branch(rpath, lpath, kwargs)
            coros.append(self._get_file(rpath, lpath, **kwargs))
        return await _run_coros_in_chunks(
            coros, batch_size=batch_size, callback=callback
        )

    async def _isfile(self, path):
        try:
            return (await self._info(path))["type"] == "file"
        except:  # noqa: E722
            return False

    async def _isdir(self, path):
        try:
            return (await self._info(path))["type"] == "directory"
        except IOError:
            return False

    async def _size(self, path):
        return (await self._info(path)).get("size", None)

    async def _sizes(self, paths, batch_size=None):
        batch_size = batch_size or self.batch_size
        return await _run_coros_in_chunks(
            [self._size(p) for p in paths], batch_size=batch_size
        )

    async def _exists(self, path):
        try:
            await self._info(path)
            return True
        except FileNotFoundError:
            return False

    async def _info(self, path, **kwargs):
        raise NotImplementedError

    async def _ls(self, path, detail=True, **kwargs):
        raise NotImplementedError

    async def _walk(self, path, maxdepth=None, **kwargs):
        path = self._strip_protocol(path)
        full_dirs = {}
        dirs = {}
        files = {}

        detail = kwargs.pop("detail", False)
        try:
            listing = await self._ls(path, detail=True, **kwargs)
        except (FileNotFoundError, IOError):
            if detail:
                yield path, {}, {}
            else:
                yield path, [], []
            return

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            pathname = info["name"].rstrip("/")
            name = pathname.rsplit("/", 1)[-1]
            if info["type"] == "directory" and pathname != path:
                # do not include "self" path
                full_dirs[pathname] = info
                dirs[name] = info
            elif pathname == path:
                # file-like with same name as give path
                files[""] = info
            else:
                files[name] = info

        if detail:
            yield path, dirs, files
        else:
            yield path, list(dirs), list(files)

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                return

        for d in full_dirs:
            async for _ in self._walk(d, maxdepth=maxdepth, detail=detail, **kwargs):
                yield _

    async def _glob(self, path, **kwargs):
        import re

        ends = path.endswith("/")
        path = self._strip_protocol(path)
        indstar = path.find("*") if path.find("*") >= 0 else len(path)
        indques = path.find("?") if path.find("?") >= 0 else len(path)
        indbrace = path.find("[") if path.find("[") >= 0 else len(path)

        ind = min(indstar, indques, indbrace)

        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            root = path
            depth = 1
            if ends:
                path += "/*"
            elif await self._exists(path):
                if not detail:
                    return [path]
                else:
                    return {path: await self._info(path)}
            else:
                if not detail:
                    return []  # glob of non-existent returns empty
                else:
                    return {}
        elif "/" in path[:ind]:
            ind2 = path[:ind].rindex("/")
            root = path[: ind2 + 1]
            depth = None if "**" in path else path[ind2 + 1 :].count("/") + 1
        else:
            root = ""
            depth = None if "**" in path else path[ind + 1 :].count("/") + 1

        allpaths = await self._find(
            root, maxdepth=depth, withdirs=True, detail=True, **kwargs
        )
        # Escape characters special to python regex, leaving our supported
        # special characters in place.
        # See https://www.gnu.org/software/bash/manual/html_node/Pattern-Matching.html
        # for shell globbing details.
        pattern = (
            "^"
            + (
                path.replace("\\", r"\\")
                .replace(".", r"\.")
                .replace("+", r"\+")
                .replace("//", "/")
                .replace("(", r"\(")
                .replace(")", r"\)")
                .replace("|", r"\|")
                .replace("^", r"\^")
                .replace("$", r"\$")
                .replace("{", r"\{")
                .replace("}", r"\}")
                .rstrip("/")
                .replace("?", ".")
            )
            + "$"
        )
        pattern = re.sub("[*]{2}", "=PLACEHOLDER=", pattern)
        pattern = re.sub("[*]", "[^/]*", pattern)
        pattern = re.compile(pattern.replace("=PLACEHOLDER=", ".*"))
        out = {
            p: allpaths[p]
            for p in sorted(allpaths)
            if pattern.match(p.replace("//", "/").rstrip("/"))
        }
        if detail:
            return out
        else:
            return list(out)

    async def _du(self, path, total=True, maxdepth=None, **kwargs):
        sizes = {}
        # async for?
        for f in await self._find(path, maxdepth=maxdepth, **kwargs):
            info = await self._info(f)
            sizes[info["name"]] = info["size"]
        if total:
            return sum(sizes.values())
        else:
            return sizes

    async def _find(self, path, maxdepth=None, withdirs=False, **kwargs):
        path = self._strip_protocol(path)
        out = dict()
        detail = kwargs.pop("detail", False)
        # async for?
        async for _, dirs, files in self._walk(path, maxdepth, detail=True, **kwargs):
            if withdirs:
                files.update(dirs)
            out.update({info["name"]: info for name, info in files.items()})
        if not out and (await self._isfile(path)):
            # walk works on directories, but find should also return [path]
            # when path happens to be a file
            out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        else:
            return {name: out[name] for name in names}

    async def _expand_path(self, path, recursive=False, maxdepth=None):
        if isinstance(path, str):
            out = await self._expand_path([path], recursive, maxdepth)
        else:
            # reduce depth on each recursion level unless None or 0
            maxdepth = maxdepth if not maxdepth else maxdepth - 1
            out = set()
            path = [self._strip_protocol(p) for p in path]
            for p in path:  # can gather here
                if has_magic(p):
                    bit = set(await self._glob(p))
                    out |= bit
                    if recursive:
                        out |= set(
                            await self._expand_path(
                                list(bit), recursive=recursive, maxdepth=maxdepth
                            )
                        )
                    continue
                elif recursive:
                    rec = set(await self._find(p, maxdepth=maxdepth, withdirs=True))
                    out |= rec
                if p not in out and (recursive is False or (await self._exists(p))):
                    # should only check once, for the root
                    out.add(p)
        if not out:
            raise FileNotFoundError(path)
        return list(sorted(out))

    async def _mkdir(self, path, create_parents=True, **kwargs):
        pass  # not necessary to implement, may not have directories

    async def _makedirs(self, path, exist_ok=False):
        pass  # not necessary to implement, may not have directories


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

    for method in async_methods + dir(AsyncFileSystem):
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


class FSSpecCoroutineCancel(Exception):
    pass


def _dump_running_tasks(
    printout=True, cancel=True, exc=FSSpecCoroutineCancel, with_task=False
):
    import traceback

    if PY36:
        raise NotImplementedError("Do not call this on Py 3.6")

    tasks = [t for t in asyncio.tasks.all_tasks(loop[0]) if not t.done()]
    if printout:
        [task.print_stack() for task in tasks]
    out = [
        {
            "locals": task._coro.cr_frame.f_locals,
            "file": task._coro.cr_frame.f_code.co_filename,
            "firstline": task._coro.cr_frame.f_code.co_firstlineno,
            "linelo": task._coro.cr_frame.f_lineno,
            "stack": traceback.format_stack(task._coro.cr_frame),
            "task": task if with_task else None,
        }
        for task in tasks
    ]
    if cancel:
        for t in tasks:
            cbs = t._callbacks
            t.cancel()
            asyncio.futures.Future.set_exception(t, exc)
            asyncio.futures.Future.cancel(t)
            [cb[0](t) for cb in cbs]  # cancels any dependent concurrent.futures
            try:
                t._coro.throw(exc)  # exits coro, unless explicitly handled
            except exc:
                pass
    return out
