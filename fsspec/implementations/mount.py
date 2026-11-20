import os
import shutil

from ..callbacks import DEFAULT_CALLBACK
from ..spec import AbstractFileSystem
from ..utils import isfilelike, stringify_path


def _normalize(path):
    """Absolute, "/"-separated form of ``path``, without "." or ".." parts"""
    parts = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/" + "/".join(parts)


def _is_within(parent, path):
    """Whether normalized ``path`` is ``parent`` or lies below it"""
    return path == parent or path.startswith(parent + "/")


def _depth(path):
    """Number of parts in a normalized path; the root has none"""
    return 0 if path == "/" else path.count("/")


def _join(root, relpath):
    if not relpath:
        return root
    if not root:
        return relpath
    return root.rstrip("/") + "/" + relpath


class MountFileSystem(AbstractFileSystem):
    """Several filesystems presented as a single tree, each at its own directory

    Every path is absolute, starting from ``/``. A path inside a mount point is
    handled by the filesystem mounted there, with the mount point standing for
    the root that filesystem was mounted from. The directories above the mount
    points exist only virtually: they can be listed and walked, but not written
    to or removed.

    Mount points cannot contain each other, and the root cannot be one.
    Operations that stay on a single mounted filesystem are passed to it
    directly, so, for example, copying or moving within one filesystem uses its
    own implementation, while copies between filesystems stream the data from
    one to the other.

    This implementation is synchronous. Asynchronous filesystems can be mounted
    and are used through their blocking methods.

    Examples
    --------
    >>> fs = MountFileSystem(
    ...     {
    ...         "/data": "s3://bucket/dataset",
    ...         "/scratch": "/tmp/scratch",
    ...     }
    ... )  # doctest: +SKIP
    >>> fs.ls("/", detail=False)  # doctest: +SKIP
    ['/data', '/scratch']
    >>> fs.cp("/data/part-0.parquet", "/scratch/")  # doctest: +SKIP
    """

    protocol = "mount"
    root_marker = "/"
    # mounts can change after creation, so instances must not be shared
    cachable = False

    def __init__(self, mounts=None, **storage_options):
        """
        Parameters
        ----------
        mounts: dict, optional
            Mapping of mount point to what is mounted there, in any form that
            :meth:`mount` accepts: a filesystem instance, a URL, or a
            ``(filesystem, root)`` pair.
        """
        super().__init__(**storage_options)
        self._mounts = {}
        for path, target in (mounts or {}).items():
            if isinstance(target, (tuple, list)):
                self.mount(path, *target)
            else:
                self.mount(path, target)

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path).removeprefix("mount://")
        return _normalize(path)

    # Mount management

    def mount(self, path, fs, root=None, **storage_options):
        """Mount a filesystem at a directory of this one

        Parameters
        ----------
        path: str
            The mount point. It cannot be the root, and cannot contain or lie
            inside another mount point.
        fs: AbstractFileSystem or str
            The filesystem to mount, or a URL to open with
            :func:`fsspec.core.url_to_fs`, whose path becomes the root. URLs may
            be chained, e.g. ``"simplecache::s3://bucket/data"``.
        root: str, optional
            The path within ``fs`` that appears at the mount point. Defaults to
            the root of ``fs``. Only accepted together with a filesystem
            instance.
        storage_options:
            Passed on to the filesystem when ``fs`` is a URL.
        """
        point = self._strip_protocol(path)
        if point == self.root_marker:
            raise ValueError("Cannot mount a filesystem at the root")
        if isinstance(fs, str):
            if root is not None:
                raise ValueError(
                    "A root cannot be given with a URL; include it in the URL"
                )
            from ..core import url_to_fs

            fs, root = url_to_fs(fs, **storage_options)
        elif storage_options:
            raise TypeError("Storage options can only be given when mounting a URL")
        if not isinstance(fs, AbstractFileSystem):
            raise TypeError(
                f"Expected a filesystem or a URL to mount, got {type(fs).__name__}"
            )
        if fs is self:
            raise ValueError("Cannot mount a filesystem inside itself")
        for existing in self._mounts:
            if _is_within(existing, point) or _is_within(point, existing):
                raise ValueError(
                    f"Mount point {point!r} overlaps the mount at {existing!r}"
                )
        root = fs.root_marker if root is None else fs._strip_protocol(root)
        self._mounts[point] = (fs, root)
        self._update_storage_options()

    def unmount(self, path):
        """Remove the mount at ``path``, returning the filesystem mounted there"""
        point = self._strip_protocol(path)
        try:
            fs, _ = self._mounts.pop(point)
        except KeyError:
            raise ValueError(f"No filesystem is mounted at {point!r}") from None
        self._update_storage_options()
        return fs

    @property
    def mounts(self):
        """Mapping of each mount point to the filesystem mounted there"""
        return {point: fs for point, (fs, _) in self._mounts.items()}

    def _update_storage_options(self):
        # Keep the options in step with the mounts, including ones added after
        # creation, so that pickling and to_dict() rebuild the same tree.
        if hasattr(self, "storage_options"):
            self.storage_options["mounts"] = {
                point: (fs, root) for point, (fs, root) in self._mounts.items()
            }
            # mounts is the only positional argument, now superseded
            self.storage_args = ()

    # Path resolution

    def _resolve(self, path):
        """Find the mount holding ``path``

        Returns ``(mount point, filesystem, root, path on that filesystem)``,
        or ``None`` if ``path`` is not at or below any mount point.
        """
        path = self._strip_protocol(path)
        for point, (fs, root) in self._mounts.items():
            if _is_within(point, path):
                return point, fs, root, _join(root, path[len(point) + 1 :])
        return None

    def _is_virtual_dir(self, path):
        """Whether normalized ``path`` is the root or a directory above a mount"""
        return path == self.root_marker or any(
            point.startswith(path + "/") for point in self._mounts
        )

    def _virtual_children(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted(
            {
                prefix + point[len(prefix) :].split("/", 1)[0]
                for point in self._mounts
                if point.startswith(prefix)
            }
        )

    def _resolve_file(self, path):
        """Resolve a path to read, which must lie below a mount point"""
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None and path != resolved[0]:
            return resolved
        if resolved is not None or self._is_virtual_dir(path):
            raise IsADirectoryError(path)
        raise FileNotFoundError(path)

    def _resolve_target(self, path):
        """Resolve a path to create, change or remove"""
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None and path != resolved[0]:
            return resolved
        if resolved is not None or self._is_virtual_dir(path):
            raise PermissionError(
                f"Cannot modify {path!r}: it is a mount point or a directory above one"
            )
        raise PermissionError(
            f"Cannot write to {path!r}: it is not inside a mounted filesystem"
        )

    @staticmethod
    def _mount_path(point, fs, root, name):
        """Translate a path on a mounted filesystem back into this filesystem"""
        name = fs._strip_protocol(name).rstrip("/")
        root = root.rstrip("/")
        if name == root:
            return point
        if not root:
            relpath = name.lstrip("/")
        elif name.startswith(root + "/"):
            relpath = name[len(root) + 1 :]
        elif root.startswith("/") and name.startswith(root[1:] + "/"):
            # some filesystems list names without the leading "/" of the root
            relpath = name[len(root) :]
        elif root.startswith("/") and name == root[1:]:
            return point
        else:
            raise ValueError(
                f"{name!r} is outside the root {root!r} of the filesystem "
                f"mounted at {point!r}"
            )
        return f"{point}/{relpath}" if relpath else point

    def _mount_info(self, point, fs, root, name, info):
        info = dict(info)
        info["name"] = self._mount_path(point, fs, root, name)
        return info

    @staticmethod
    def _directory_info(path):
        return {"name": path, "size": 0, "type": "directory"}

    # Listing and inspection

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None:
            point, fs, root, inner = resolved
            listing = fs.ls(inner, detail=detail, **kwargs)
            if detail:
                return [
                    self._mount_info(point, fs, root, entry["name"], entry)
                    for entry in listing
                ]
            return [self._mount_path(point, fs, root, name) for name in listing]
        if not self._is_virtual_dir(path):
            raise FileNotFoundError(path)
        entries = [self._directory_info(name) for name in self._virtual_children(path)]
        return entries if detail else [entry["name"] for entry in entries]

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None:
            point, fs, root, inner = resolved
            if path == point:
                # A mount point is a directory, even where the root of the
                # mounted filesystem has no entry of its own (e.g. a bucket).
                return self._directory_info(point)
            return self._mount_info(point, fs, root, inner, fs.info(inner, **kwargs))
        if self._is_virtual_dir(path):
            return self._directory_info(path)
        raise FileNotFoundError(path)

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None:
            point, fs, root, inner = resolved
            found = self._find_in_mount(
                point, fs, root, inner, maxdepth, withdirs, **kwargs
            )
        elif self._is_virtual_dir(path):
            found = self._find_in_virtual_dir(path, maxdepth, withdirs, **kwargs)
        else:
            found = {}
        names = sorted(found)
        if not detail:
            return names
        return {name: found[name] for name in names}

    def _find_in_mount(self, point, fs, root, inner, maxdepth, withdirs, **kwargs):
        found = fs.find(
            inner, maxdepth=maxdepth, withdirs=withdirs, detail=True, **kwargs
        )
        return {
            info["name"]: info
            for info in (
                self._mount_info(point, fs, root, name, info)
                for name, info in found.items()
            )
        }

    def _find_in_virtual_dir(self, path, maxdepth, withdirs, **kwargs):
        out = {}
        if withdirs and path != self.root_marker:
            out[path] = self._directory_info(path)
        base = _depth(path)
        for point, (fs, root) in self._mounts.items():
            if not point.startswith(path.rstrip("/") + "/"):
                continue
            depth = _depth(point) - base
            if withdirs:
                # the virtual directories on the way down, and the mount point
                parts = point.split("/")
                for level in range(base + 1, _depth(point) + 1):
                    if maxdepth is None or level - base <= maxdepth:
                        name = "/".join(parts[: level + 1])
                        out[name] = self._directory_info(name)
            if maxdepth is not None and maxdepth - depth < 1:
                continue
            below = self._find_in_mount(
                point,
                fs,
                root,
                root,
                None if maxdepth is None else maxdepth - depth,
                withdirs,
                **kwargs,
            )
            out.update(below)
            if withdirs:
                # keep the mount point described as a directory, not by the
                # entry (if any) its root has on the mounted filesystem
                out[point] = self._directory_info(point)
        return out

    def invalidate_cache(self, path=None):
        if path is None:
            targets = list(self._mounts.values())
        else:
            path = self._strip_protocol(path)
            resolved = self._resolve(path)
            if resolved is not None:
                resolved[1].invalidate_cache(resolved[3])
                return
            targets = [
                target
                for point, target in self._mounts.items()
                if point.startswith(path.rstrip("/") + "/")
            ]
        for fs, root in targets:
            fs.invalidate_cache(root if root.strip("/") else None)

    def created(self, path):
        return self._delegate_entry("created", path)

    def modified(self, path):
        return self._delegate_entry("modified", path)

    def sign(self, path, expiration=100, **kwargs):
        point, fs, _, inner = self._resolve_file(path)
        return fs.sign(inner, expiration=expiration, **kwargs)

    def ukey(self, path):
        resolved = self._resolve(path)
        if resolved is None or self._strip_protocol(path) == resolved[0]:
            return super().ukey(path)
        return resolved[1].ukey(resolved[3])

    def checksum(self, path):
        resolved = self._resolve(path)
        if resolved is None or self._strip_protocol(path) == resolved[0]:
            return super().checksum(path)
        return resolved[1].checksum(resolved[3])

    def _delegate_entry(self, method, path):
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is None:
            raise FileNotFoundError(path)
        return getattr(resolved[1], method)(resolved[3])

    # Reading

    def cat_file(self, path, start=None, end=None, **kwargs):
        _, fs, _, inner = self._resolve_file(path)
        return fs.cat_file(inner, start=start, end=end, **kwargs)

    def cat_ranges(
        self, paths, starts, ends, max_gap=None, on_error="return", **kwargs
    ):
        if not isinstance(paths, list):
            raise TypeError("paths must be a list")
        if not isinstance(starts, list):
            starts = [starts] * len(paths)
        if not isinstance(ends, list):
            ends = [ends] * len(paths)
        if len(starts) != len(paths) or len(ends) != len(paths):
            raise ValueError("paths, starts and ends must have the same length")

        out = [None] * len(paths)
        # one request per mounted filesystem, so each can batch its own reads
        groups = {}
        for index, (path, start, end) in enumerate(zip(paths, starts, ends)):
            try:
                _, fs, _, inner = self._resolve_file(path)
            except OSError as exc:
                if on_error == "raise":
                    raise
                out[index] = exc
                continue
            group = groups.setdefault(id(fs), (fs, [], [], [], []))
            group[1].append(index)
            group[2].append(inner)
            group[3].append(start)
            group[4].append(end)

        for fs, indices, inners, group_starts, group_ends in groups.values():
            results = fs.cat_ranges(
                inners,
                group_starts,
                group_ends,
                max_gap=max_gap,
                on_error=on_error,
                **kwargs,
            )
            for index, result in zip(indices, results):
                out[index] = result
        return out

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        if "r" in mode:
            _, fs, _, inner = self._resolve_file(path)
        else:
            _, fs, _, inner = self._resolve_target(path)
        return fs.open(
            inner,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            autocommit=autocommit,
            **kwargs,
        )

    def get_file(self, rpath, lpath, callback=DEFAULT_CALLBACK, outfile=None, **kwargs):
        path = self._strip_protocol(rpath)
        resolved = self._resolve(path)
        if resolved is None or path == resolved[0]:
            if resolved is None and not self._is_virtual_dir(path):
                raise FileNotFoundError(path)
            # a mount point or a directory above one
            if outfile is None and not isfilelike(lpath):
                os.makedirs(lpath, exist_ok=True)
            return None
        _, fs, _, inner = resolved
        if outfile is not None:
            kwargs["outfile"] = outfile
        return fs.get_file(inner, lpath, callback=callback, **kwargs)

    # Writing

    def pipe_file(self, path, value, mode="overwrite", **kwargs):
        _, fs, _, inner = self._resolve_target(path)
        return fs.pipe_file(inner, value, mode=mode, **kwargs)

    def put_file(
        self, lpath, rpath, callback=DEFAULT_CALLBACK, mode="overwrite", **kwargs
    ):
        if os.path.isdir(lpath):
            self.makedirs(rpath, exist_ok=True)
            return None
        _, fs, _, inner = self._resolve_target(rpath)
        return fs.put_file(lpath, inner, callback=callback, mode=mode, **kwargs)

    def touch(self, path, truncate=True, **kwargs):
        _, fs, _, inner = self._resolve_target(path)
        return fs.touch(inner, truncate=truncate, **kwargs)

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None and path != resolved[0]:
            _, fs, _, inner = resolved
            return fs.mkdir(inner, create_parents=create_parents, **kwargs)
        if resolved is not None or self._is_virtual_dir(path):
            raise FileExistsError(path)
        raise PermissionError(
            f"Cannot create {path!r}: it is not inside a mounted filesystem"
        )

    def makedirs(self, path, exist_ok=False):
        path = self._strip_protocol(path)
        resolved = self._resolve(path)
        if resolved is not None and path != resolved[0]:
            _, fs, _, inner = resolved
            return fs.makedirs(inner, exist_ok=exist_ok)
        if resolved is not None or self._is_virtual_dir(path):
            if not exist_ok:
                raise FileExistsError(path)
            return None
        raise PermissionError(
            f"Cannot create {path!r}: it is not inside a mounted filesystem"
        )

    def cp_file(self, path1, path2, **kwargs):
        source = self._strip_protocol(path1)
        resolved = self._resolve(source)
        if resolved is None or source == resolved[0]:
            if resolved is None and not self._is_virtual_dir(source):
                raise FileNotFoundError(source)
            # copying a mount point or a directory above one creates a directory
            self.makedirs(path2, exist_ok=True)
            return None
        _, fs1, _, inner1 = resolved
        _, fs2, _, inner2 = self._resolve_target(path2)
        if fs1 is fs2:
            return fs1.cp_file(inner1, inner2, **kwargs)
        if fs1.isdir(inner1):
            fs2.makedirs(inner2, exist_ok=True)
            return None
        with fs1.open(inner1, "rb") as source_file:
            with fs2.open(inner2, "wb") as target_file:
                shutil.copyfileobj(source_file, target_file)
        return None

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        if isinstance(path1, str) and isinstance(path2, str):
            source = self._resolve(path1)
            target = self._resolve(path2)
            if (
                source is not None
                and target is not None
                and source[1] is target[1]
                and self._strip_protocol(path1) != source[0]
                and self._strip_protocol(path2) != target[0]
            ):
                return source[1].mv(
                    _with_trailing_sep(path1, source[3]),
                    _with_trailing_sep(path2, target[3]),
                    recursive=recursive,
                    maxdepth=maxdepth,
                    **kwargs,
                )
        return super().mv(
            path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs
        )

    def rm_file(self, path):
        _, fs, _, inner = self._resolve_target(path)
        return fs.rm_file(inner)

    def rm(self, path, recursive=False, maxdepth=None):
        paths = path if isinstance(path, list) else [path]
        groups = {}
        for p in paths:
            _, fs, _, inner = self._resolve_target(p)
            groups.setdefault(id(fs), (fs, []))[1].append(inner)
        for fs, inners in groups.values():
            fs.rm(inners, recursive=recursive, maxdepth=maxdepth)

    def rmdir(self, path):
        _, fs, _, inner = self._resolve_target(path)
        return fs.rmdir(inner)


def _with_trailing_sep(original, path):
    # A trailing "/" changes the meaning of copies and moves, so keep it.
    original = stringify_path(original)
    if original.endswith("/") and not path.endswith("/"):
        return path + "/"
    return path
