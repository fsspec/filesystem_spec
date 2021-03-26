import asyncio
import functools
import inspect
import os
import re
import threading
from glob import has_magic

from .spec import AbstractFileSystem
from .utils import is_exception, other_paths

private = re.compile("_[^_]")
lock = threading.Lock()


def sync(loop, func, *args, callback_timeout=None, **kwargs):
    """
    Make loop run coroutine until it returns. Runs in this thread
    """
    coro = func(*args, **kwargs)
    if loop.is_running():
        raise NotImplementedError
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
        # TEMPORARY - to be removed
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
    """Get/Create an event loop to run in this thread

    If a loop was previously set, but has since closed, set a new one.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None
    if loop is None or loop.is_closed():
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
            # override and add here if you need to do any setup when instance is used
            # from a new thread
            self._loop.loop = get_loop()
        return self._loop.loop

    async def _rm_file(self, path, **kwargs):
        raise NotImplementedError

    async def _rm(self, path, recursive=False, **kwargs):
        path = await self._expand_path(path, recursive=recursive)
        await asyncio.gather(*[self._rm_file(p, **kwargs) for p in path])

    async def _copy(
        self, path1, path2, recursive=False, on_error=None, maxdepth=None, **kwargs
    ):
        if on_error is None and recursive:
            on_error = "ignore"
        elif on_error is None:
            on_error = "raise"

        paths = await self._expand_path(path1, maxdepth=maxdepth, recursive=recursive)
        path2 = other_paths(paths, path2)
        result = await asyncio.gather(
            *[self._cp_file(p1, p2, **kwargs) for p1, p2 in zip(paths, path2)],
            return_exceptions=True,
        )

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

    async def _cat(self, path, recursive=False, on_error="raise", **kwargs):
        paths = await self._expand_path(path, recursive=recursive)
        out = await asyncio.gather(
            *[self._cat_file(path, **kwargs) for path in paths],
            return_exceptions=True,
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

    async def _put(self, lpath, rpath, recursive=False, **kwargs):
        from .implementations.local import LocalFileSystem, make_path_posix

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        rpaths = other_paths(lpaths, rpath)
        return await asyncio.gather(
            *[
                self._put_file(lpath, rpath, **kwargs)
                for lpath, rpath in zip(lpaths, rpaths)
            ]
        )

    async def _get_file(self, lpath, rpath, **kwargs):
        raise NotImplementedError

    async def _get(self, rpath, lpath, recursive=False, **kwargs):
        from fsspec.implementations.local import make_path_posix

        rpath = self._strip_protocol(rpath)
        lpath = make_path_posix(lpath)
        rpaths = await self._expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        [os.makedirs(os.path.dirname(lp), exist_ok=True) for lp in lpaths]
        return await asyncio.gather(
            *[
                self._get_file(rpath, lpath, **kwargs)
                for lpath, rpath in zip(lpaths, rpaths)
            ]
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

    async def _exists(self, path):
        try:
            await self._info(path)
            return True
        except FileNotFoundError:
            return False

    async def _ls(self, path, **kwargs):
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
            yield [], [], []
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
        if (await self._isfile(path)) and path not in out:
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
            elif (
                hasattr(obj, smethod)
                and inspect.ismethod(getattr(obj, smethod))
                and not hasattr(obj, method)
            ):
                setattr(obj, method, async_wrapper(getattr(obj, smethod)))
