from __future__ import annotations

import inspect
import logging
import os
import uuid

from .asyn import AsyncFileSystem, _run_coros_in_chunks, sync_wrapper
from .callbacks import DEFAULT_CALLBACK
from .core import filesystem, get_filesystem_class, split_protocol, url_to_fs
from .implementations.chained import ChainedFileSystem
from .implementations.dirfs import DirFileSystem
from .implementations.local import LocalFileSystem
from .utils import _escapes_root, check_contained

_generic_fs = {}
logger = logging.getLogger("fsspec.generic")


def _normalize_rsync_root(root: str) -> str:
    if "://" in root:
        scheme, rest = root.split("://", 1)
        if not rest:
            return root
        if not rest.strip("/"):
            return f"{scheme}:///"
        if len(rest) >= 3 and rest[1] == ":" and not rest[2:].strip("/"):
            return f"{scheme}://{rest[:2]}/"
        return f"{scheme}://{rest.rstrip('/')}"
    if not root.strip("/"):
        return "/" if root.startswith("/") else root
    if len(root) >= 3 and root[1] == ":" and not root[2:].strip("/"):
        return f"{root[:2]}/"
    return root.rstrip("/")


def _extract_rsync_rel(path: str, root: str) -> str:
    root_norm = _normalize_rsync_root(root)
    if path in (root, root_norm):
        return ""
    if root_norm.endswith("://"):
        if path.startswith(root_norm):
            return path.removeprefix(root_norm).removeprefix("/")
    else:
        prefix = root_norm if root_norm.endswith("/") else root_norm + "/"
        if path.startswith(prefix):
            return path.removeprefix(prefix)
        if root_norm.endswith(":///") and path == root_norm.removesuffix("/"):
            return ""
    raise ValueError(f"path {path!r} is outside the root {root!r}")


def _join_rsync_url(root: str, rel: str, root_fs=None) -> str:
    root_norm = _normalize_rsync_root(root)
    if not rel:
        return root_norm
    if root_norm.endswith("://"):
        sep = "/" if getattr(root_fs, "root_marker", "") == "/" else ""
        return f"{root_norm}{sep}{rel}"
    return f"{root_norm}{'' if root_norm.endswith('/') else '/'}{rel}"


def _map_rsync_url(path: str, src: str, dst: str, dst_fs=None) -> tuple[str, str]:
    rel = _extract_rsync_rel(path, src)
    target = _join_rsync_url(dst, rel, dst_fs)
    src_norm = _normalize_rsync_root(src)
    if (
        not rel
        and not src_norm.endswith("/")
        and path == src_norm + "/"
        and not target.endswith("/")
    ):
        # Preserve a zero-byte "prefix/" marker object naming the root itself.
        target += "/"
    return rel, target


def _local_dest_mapper(dest_fs):
    """Return a URL-to-local-path mapper if ``dest_fs`` targets LocalFileSystem."""
    transforms = []
    cur = dest_fs
    seen = set()
    while id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, LocalFileSystem):
            break
        if isinstance(cur, DirFileSystem):
            transforms.append(cur._join)
            cur = cur.fs
        elif hasattr(cur, "sync_fs"):
            transforms.append(cur._strip_protocol)
            cur = cur.sync_fs
        elif isinstance(cur, ChainedFileSystem) and hasattr(cur, "fs"):
            transforms.append(cur._strip_protocol)
            cur = cur.fs
        else:
            return None
    else:
        return None

    local_fs = cur

    def to_local(url: str) -> str:
        p = url
        for fn in transforms:
            p = fn(p)
        return local_fs._strip_protocol(p)

    return to_local


def _check_relative_depth(
    source_path: str, rel: str, destination: str, split_backslash: bool
) -> None:
    norm_rel = rel.replace("\\", "/") if split_backslash else rel
    if _escapes_root(norm_rel):
        raise ValueError(
            f"path {source_path!r} would be copied outside the destination "
            f"{destination!r}"
        )


def set_generic_fs(protocol, **storage_options):
    """Populate the dict used for method=="generic" lookups"""
    _generic_fs[protocol] = filesystem(protocol, **storage_options)


def _resolve_fs(url, method, protocol=None, storage_options=None):
    """Pick instance of backend FS"""
    url = url[0] if isinstance(url, (list, tuple)) else url
    protocol = protocol or split_protocol(url)[0]
    storage_options = storage_options or {}
    if method == "default":
        return filesystem(protocol)
    if method == "generic":
        return _generic_fs[protocol]
    if method == "current":
        cls = get_filesystem_class(protocol)
        return cls.current()
    if method == "options":
        fs, _ = url_to_fs(url, **storage_options.get(protocol, {}))
        return fs
    raise ValueError(f"Unknown FS resolution method: {method}")


def rsync(
    source,
    destination,
    delete_missing=False,
    source_field="size",
    dest_field="size",
    update_cond="different",
    inst_kwargs=None,
    fs=None,
    **kwargs,
):
    """Sync files between two directory trees

    (experimental)

    Parameters
    ----------
    source: str
        Root of the directory tree to take files from. This must be a directory, but
        do not include any terminating "/" character
    destination: str
        Root path to copy into. The contents of this location should be
        identical to the contents of ``source`` when done. This will be made a
        directory, and the terminal "/" should not be included.
    delete_missing: bool
        If there are paths in the destination that don't exist in the
        source and this is True, delete them. Otherwise, leave them alone.
    source_field: str | callable
        If ``update_field`` is "different", this is the key in the info
        of source files to consider for difference. Maybe a function of the
        info dict.
    dest_field: str | callable
        If ``update_field`` is "different", this is the key in the info
        of destination files to consider for difference. May be a function of
        the info dict.
    update_cond: "different"|"always"|"never"
        If "always", every file is copied, regardless of whether it exists in
        the destination. If "never", files that exist in the destination are
        not copied again. If "different" (default), only copy if the info
        fields given by ``source_field`` and ``dest_field`` (usually "size")
        are different. Other comparisons may be added in the future.
    inst_kwargs: dict|None
        If ``fs`` is None, use this set of keyword arguments to make a
        GenericFileSystem instance
    fs: GenericFileSystem|None
        Instance to use if explicitly given. The instance defines how to
        to make downstream file system instances from paths.

    Returns
    -------
    dict of the copy operations that were performed, {source: destination}
    """
    fs = fs or GenericFileSystem(**(inst_kwargs or {}))
    source = fs._strip_protocol(source)
    destination = fs._strip_protocol(destination)
    allfiles = fs.find(source, withdirs=True, detail=True)
    if not fs.isdir(source):
        raise ValueError("Can only rsync on a directory")
    dest_fs = _resolve_fs(destination, fs.method, storage_options=fs.st_opts)
    to_local = _local_dest_mapper(dest_fs)
    split_backslash = to_local is not None and os.sep != "/"
    # The names came from the source listing; ".." in one of them would
    # otherwise place the copy above the destination.
    mapped = {}
    for a in allfiles:
        rel, target = _map_rsync_url(a, source, destination, dest_fs)
        _check_relative_depth(a, rel, destination, split_backslash)
        mapped[a] = target
    if to_local is not None:
        check_contained(
            to_local(destination),
            [to_local(target) for target in mapped.values()],
        )
    otherfiles = fs.find(destination, withdirs=True, detail=True)
    dirs = [
        a
        for a, v in allfiles.items()
        if v["type"] == "directory" and mapped[a] not in otherfiles
    ]
    logger.debug(f"{len(dirs)} directories to create")
    if dirs:
        fs.make_many_dirs([mapped[dirn] for dirn in dirs], exist_ok=True)
    allfiles = {a: v for a, v in allfiles.items() if v["type"] == "file"}
    logger.debug(f"{len(allfiles)} files to consider for copy")
    to_delete = []
    if delete_missing:
        src_fs = _resolve_fs(source, fs.method, storage_options=fs.st_opts)
        to_delete = [
            o
            for o, v in otherfiles.items()
            if v["type"] == "file"
            and _map_rsync_url(o, destination, source, src_fs)[1] not in allfiles
        ]
    for k, v in allfiles.copy().items():
        otherfile = mapped[k]
        if otherfile in otherfiles:
            if update_cond == "always":
                allfiles[k] = otherfile
            elif update_cond == "never":
                allfiles.pop(k)
            elif update_cond == "different":
                inf1 = source_field(v) if callable(source_field) else v[source_field]
                v2 = otherfiles[otherfile]
                inf2 = dest_field(v2) if callable(dest_field) else v2[dest_field]
                if inf1 != inf2:
                    # details mismatch, make copy
                    allfiles[k] = otherfile
                else:
                    # details match, don't copy
                    allfiles.pop(k)
        else:
            # file not in target yet
            allfiles[k] = otherfile
    logger.debug(f"{len(allfiles)} files to copy")
    if allfiles:
        source_files, target_files = zip(*allfiles.items())
        fs.cp(source_files, target_files, **kwargs)
    logger.debug(f"{len(to_delete)} files to delete")
    if delete_missing and to_delete:
        fs.rm(to_delete)
    return allfiles


class GenericFileSystem(AsyncFileSystem):
    """Wrapper over all other FS types

    <experimental!>

    This implementation is a single unified interface to be able to run FS operations
    over generic URLs, and dispatch to the specific implementations using the URL
    protocol prefix.

    Note: instances of this FS are always async, even if you never use it with any async
    backend.
    """

    protocol = "generic"  # there is no real reason to ever use a protocol with this FS

    def __init__(self, default_method="default", storage_options=None, **kwargs):
        """

        Parameters
        ----------
        default_method: str (optional)
            Defines how to configure backend FS instances. Options are:
            - "default": instantiate like FSClass(), with no
              extra arguments; this is the default instance of that FS, and can be
              configured via the config system
            - "generic": takes instances from the `_generic_fs` dict in this module,
              which you must populate before use. Keys are by protocol
            - "options": expects storage_options, a dict mapping protocol to
              kwargs to use when constructing the filesystem
            - "current": takes the most recently instantiated version of each FS
        """
        self.method = default_method
        self.st_opts = storage_options
        super().__init__(**kwargs)

    def _parent(self, path):
        fs = _resolve_fs(path, self.method, storage_options=self.st_opts)
        return fs.unstrip_protocol(fs._parent(path))

    def _strip_protocol(self, path):
        # normalization only
        fs = _resolve_fs(path, self.method, storage_options=self.st_opts)
        return fs.unstrip_protocol(fs._strip_protocol(path))

    async def _find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        fs = _resolve_fs(path, self.method, storage_options=self.st_opts)
        if fs.async_impl:
            out = await fs._find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=True, **kwargs
            )
        else:
            out = fs.find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=True, **kwargs
            )
        result = {}
        for k, v in out.items():
            v = v.copy()  # don't corrupt target FS dircache
            name = fs.unstrip_protocol(k)
            v["name"] = name
            result[name] = v
        if detail:
            return result
        return list(result)

    async def _info(self, url, **kwargs):
        fs = _resolve_fs(url, self.method, storage_options=self.st_opts)
        if fs.async_impl:
            out = await fs._info(url, **kwargs)
        else:
            out = fs.info(url, **kwargs)
        out = out.copy()  # don't edit originals
        out["name"] = fs.unstrip_protocol(out["name"])
        return out

    async def _ls(
        self,
        url,
        detail=True,
        **kwargs,
    ):
        fs = _resolve_fs(url, self.method, storage_options=self.st_opts)
        if fs.async_impl:
            out = await fs._ls(url, detail=True, **kwargs)
        else:
            out = fs.ls(url, detail=True, **kwargs)
        out = [o.copy() for o in out]  # don't edit originals
        for o in out:
            o["name"] = fs.unstrip_protocol(o["name"])
        if detail:
            return out
        else:
            return [o["name"] for o in out]

    async def _cat_file(
        self,
        url,
        **kwargs,
    ):
        fs = _resolve_fs(url, self.method, storage_options=self.st_opts)
        if fs.async_impl:
            return await fs._cat_file(url, **kwargs)
        else:
            return fs.cat_file(url, **kwargs)

    async def _pipe_file(
        self,
        path,
        value,
        **kwargs,
    ):
        fs = _resolve_fs(path, self.method, storage_options=self.st_opts)
        if fs.async_impl:
            return await fs._pipe_file(path, value, **kwargs)
        else:
            return fs.pipe_file(path, value, **kwargs)

    async def _rm(self, url, **kwargs):
        urls = url
        if isinstance(urls, str):
            urls = [urls]
        fs = _resolve_fs(urls[0], self.method, storage_options=self.st_opts)
        # Validate the entire batch before removing anything, as in _copy.
        if any(
            _resolve_fs(u, self.method, storage_options=self.st_opts) is not fs
            for u in urls[1:]
        ):
            raise ValueError("Multiple backends are not supported")
        if fs.async_impl:
            await fs._rm(urls, **kwargs)
        else:
            fs.rm(url, **kwargs)

    async def _makedirs(self, path, exist_ok=False):
        logger.debug("Make dir %s", path)
        fs = _resolve_fs(path, self.method, storage_options=self.st_opts)
        if fs.async_impl:
            await fs._makedirs(path, exist_ok=exist_ok)
        else:
            fs.makedirs(path, exist_ok=exist_ok)

    def rsync(self, source, destination, **kwargs):
        """Sync files between two directory trees

        See `func:rsync` for more details.
        """
        rsync(source, destination, fs=self, **kwargs)

    async def _cp_file(
        self,
        url,
        url2,
        blocksize=2**20,
        callback=DEFAULT_CALLBACK,
        tempdir: str | None = None,
        **kwargs,
    ):
        fs = _resolve_fs(url, self.method, storage_options=self.st_opts)
        fs2 = _resolve_fs(url2, self.method, storage_options=self.st_opts)
        if fs is fs2:
            # pure remote
            if fs.async_impl:
                return await fs._copy(url, url2, **kwargs)
            else:
                return fs.copy(url, url2, **kwargs)
        await copy_file_op(fs, [url], fs2, [url2], tempdir, 1, on_error="raise")

    async def _make_many_dirs(self, urls, exist_ok=True):
        fs = _resolve_fs(urls[0], self.method, storage_options=self.st_opts)
        if fs.async_impl:
            coros = [fs._makedirs(u, exist_ok=exist_ok) for u in urls]
            await _run_coros_in_chunks(coros)
        else:
            for u in urls:
                fs.makedirs(u, exist_ok=exist_ok)

    make_many_dirs = sync_wrapper(_make_many_dirs)

    async def _copy(
        self,
        path1: list[str],
        path2: list[str],
        recursive: bool = False,
        on_error: str = "ignore",
        maxdepth: int | None = None,
        batch_size: int | None = None,
        tempdir: str | None = None,
        **kwargs,
    ):
        # TODO: special case for one FS being local, which can use get/put
        # TODO: special case for one being memFS, which can use cat/pipe
        if recursive:
            raise NotImplementedError("Please use fsspec.generic.rsync")
        path1 = [path1] if isinstance(path1, str) else path1
        path2 = [path2] if isinstance(path2, str) else path2

        fs = _resolve_fs(path1, self.method, storage_options=self.st_opts)
        fs2 = _resolve_fs(path2, self.method, storage_options=self.st_opts)

        # Validate both entire batches before copying any files.
        for paths, backend, side in (
            (path1, fs, "source"),
            (path2, fs2, "destination"),
        ):
            if any(
                _resolve_fs(path, self.method, storage_options=self.st_opts)
                is not backend
                for path in paths
            ):
                raise ValueError(f"Multiple {side} backends are not supported")

        if fs is fs2:
            if fs.async_impl:
                return await fs._copy(path1, path2, **kwargs)
            else:
                return fs.copy(path1, path2, **kwargs)

        await copy_file_op(
            fs, path1, fs2, path2, tempdir, batch_size, on_error=on_error
        )


async def copy_file_op(
    fs1, url1, fs2, url2, tempdir=None, batch_size=20, on_error="ignore"
):
    import tempfile

    with tempfile.TemporaryDirectory(dir=tempdir) as tempdir:
        coros = [
            _copy_file_op(
                fs1,
                u1,
                fs2,
                u2,
                os.path.join(tempdir, uuid.uuid4().hex),
            )
            for u1, u2 in zip(url1, url2)
        ]
        out = await _run_coros_in_chunks(
            coros, batch_size=batch_size, return_exceptions=True
        )
    if on_error == "return":
        return out
    elif on_error == "raise":
        for o in out:
            if isinstance(o, Exception):
                raise o


async def _copy_file_op(fs1, url1, fs2, url2, local, on_error="ignore"):
    if fs1.async_impl:
        await fs1._get_file(url1, local)
    else:
        fs1.get_file(url1, local)
    if fs2.async_impl:
        await fs2._put_file(local, url2)
    else:
        fs2.put_file(local, url2)
    os.unlink(local)
    logger.debug("Copy %s -> %s; done", url1, url2)


async def maybe_await(cor):
    if inspect.iscoroutine(cor):
        return await cor
    else:
        return cor
