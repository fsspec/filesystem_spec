from __future__ import annotations

import errno
import ntpath
import os
import time
from typing import TYPE_CHECKING

from fsspec.utils import atomic_write

try:
    import ujson as json
except ImportError:
    if not TYPE_CHECKING:
        import json

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any, Literal, TypeAlias

    from .cached import CachingFileSystem

    Detail: TypeAlias = dict[str, Any]


def _is_within(parent: str, child: str) -> bool:
    parent_norm = os.path.normcase(parent)
    child_norm = os.path.normcase(child)
    try:
        return (
            child_norm != parent_norm
            and os.path.normcase(os.path.commonpath([parent, child])) == parent_norm
        )
    except ValueError:
        return False


class CacheMetadata:
    """Cache metadata.

    All reading and writing of cache metadata is performed by this class,
    accessing the cached files and blocks is not.

    Metadata is stored in a single file per storage directory in JSON format.
    For backward compatibility. No longer supports pickle.
    """

    def __init__(self, storage: list[str], metadata_name: str = "cache"):
        """

        Parameters
        ----------
        storage: list[str]
            Directories containing cached files, must be at least one. Metadata
            is stored in the last of these directories by convention.
        metadata_name: str
            Name of the metadata file inside each storage directory.
        """
        if not storage:
            raise ValueError("CacheMetadata expects at least one storage location")

        self._storage = storage
        self._metadata_name = metadata_name
        self.cached_files: list[Detail] = [{}]

    def _load(self, fn: str) -> Detail:
        """Low-level function to load metadata from specific file"""
        if os.path.islink(fn):
            raise OSError(errno.ELOOP, os.strerror(errno.ELOOP), fn)
        with open(fn, "r") as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict) or not all(
            isinstance(c, dict) for c in loaded.values()
        ):
            raise ValueError(f"Invalid cache metadata format in {fn!r}")
        for c in loaded.values():
            if isinstance(c.get("blocks"), list):
                c["blocks"] = set(c["blocks"])
        return loaded

    def _save(self, metadata_to_save: Detail, fn: str) -> None:
        """Low-level function to save metadata to specific file"""
        with atomic_write(fn, mode="w") as f:
            json.dump(metadata_to_save, f)

    def _scan_locations(
        self, writable_only: bool = False
    ) -> Iterator[tuple[str, str, bool]]:
        """Yield locations (filenames) where metadata is stored, and whether
        writable or not.

        Parameters
        ----------
        writable: bool
            Set to True to only yield writable locations.

        Returns
        -------
        Yields (str, str, bool)
        """
        n = len(self._storage)
        for i, storage in enumerate(self._storage):
            writable = i == n - 1
            if writable_only and not writable:
                continue
            yield os.path.join(storage, self._metadata_name), storage, writable

    def _resolve_cached_file(self, base: str, fn: Any) -> str | None:
        """Resolve ``fn`` within ``base`` if it is a contained single filename."""
        if (
            not isinstance(fn, str)
            or not fn
            or "\x00" in fn
            or fn in {".", ".."}
            or "/" in fn
            or "\\" in fn
            or os.path.isabs(fn)
            or os.path.basename(fn) != fn
            or os.path.splitdrive(fn)[0] != ""
            or ntpath.splitdrive(fn)[0] != ""
            or os.path.normcase(fn) == os.path.normcase(self._metadata_name)
        ):
            return None
        if os.name == "nt" and (
            ":" in fn
            or fn.rstrip(". ") != fn
            or fn.rstrip(". ").casefold() == self._metadata_name.rstrip(". ").casefold()
        ):
            return None

        base_abs = os.path.abspath(base)
        candidate = os.path.join(base, fn)
        candidate_abs = os.path.abspath(candidate)
        metadata_path = os.path.join(base, self._metadata_name)
        metadata_abs = os.path.abspath(metadata_path)
        if (
            os.path.normcase(candidate_abs) == os.path.normcase(metadata_abs)
            or not _is_within(base_abs, candidate_abs)
            or os.path.islink(candidate)
        ):
            return None
        if os.path.lexists(candidate):
            if not os.path.isfile(candidate):
                return None
            if os.path.lexists(metadata_path) and (
                os.path.islink(metadata_path)
                or os.path.samefile(candidate, metadata_path)
            ):
                return None
            base_real = os.path.realpath(base)
            candidate_real = os.path.realpath(candidate)
            metadata_real = os.path.realpath(metadata_path)
            if os.path.normcase(candidate_real) == os.path.normcase(
                metadata_real
            ) or not _is_within(base_real, candidate_real):
                return None

        return candidate

    def _check_file_with_writable(
        self, path: str, cfs: CachingFileSystem | None
    ) -> Literal[False] | tuple[Detail, str, bool]:
        for (_, base, writable), cache in zip(
            self._scan_locations(), self.cached_files
        ):
            if path not in cache:
                continue
            detail = cache[path].copy()
            resolved_fn = self._resolve_cached_file(base, detail.get("fn"))
            if resolved_fn is None:
                continue

            if cfs is not None:
                if cfs.check_files and detail["uid"] != cfs.fs.ukey(path):
                    # Wrong file as determined by hash of file properties
                    continue
                if cfs.expiry and time.time() - detail["time"] > cfs.expiry:
                    # Cached file has expired
                    continue

            if os.path.isfile(resolved_fn):
                return detail, resolved_fn, writable
        return False

    def check_file(
        self, path: str, cfs: CachingFileSystem | None
    ) -> Literal[False] | tuple[Detail, str]:
        """If path is in cache return its details, otherwise return ``False``.

        If the optional CachingFileSystem is specified then it is used to
        perform extra checks to reject possible matches, such as if they are
        too old.
        """
        result = self._check_file_with_writable(path, cfs)
        if not result:
            return False
        detail, fn, _ = result
        return detail, fn

    @staticmethod
    def _prepare_for_save(cached_files: Detail) -> Detail:
        cache = {k: v.copy() for k, v in cached_files.items()}
        for c in cache.values():
            if isinstance(c.get("blocks"), set):
                c["blocks"] = list(c["blocks"])
        return cache

    def clear_expired(self, expiry_time: int) -> tuple[list[str], bool]:
        """Remove expired metadata from the cache.

        Returns names of files corresponding to expired metadata and a boolean
        flag indicating whether the writable cache is empty. Caller is
        responsible for deleting the expired files.
        """
        expired_files = []
        removed_any = False
        writable_storage = self._storage[-1]
        for path, detail in self.cached_files[-1].copy().items():
            if time.time() - detail["time"] > expiry_time:
                fn = detail.get("fn", "")
                if not fn:
                    raise RuntimeError(
                        f"Cache metadata does not contain 'fn' for {path}"
                    )
                resolved_fn = self._resolve_cached_file(writable_storage, fn)
                if resolved_fn is not None:
                    expired_files.append(resolved_fn)
                self.cached_files[-1].pop(path)
                removed_any = True

        cache_path = os.path.join(writable_storage, self._metadata_name)
        if self.cached_files[-1] or (removed_any and os.path.exists(cache_path)):
            self._save(self._prepare_for_save(self.cached_files[-1]), cache_path)

        writable_cache_empty = not self.cached_files[-1]
        return expired_files, writable_cache_empty

    def load(self) -> None:
        """Load all metadata from disk and store in ``self.cached_files``"""
        cached_files = []
        for fn, _, _ in self._scan_locations():
            if os.path.lexists(fn):
                # TODO: consolidate blocks here
                cached_files.append(self._load(fn))
            else:
                cached_files.append({})
        self.cached_files = cached_files or [{}]

    def on_close_cached_file(self, f: Any, path: str) -> None:
        """Perform side-effect actions on closing a cached file.

        The actual closing of the file is the responsibility of the caller.
        """
        # File must be writable, so in self.cached_files[-1]
        c = self.cached_files[-1][path]
        if c["blocks"] is not True and len(c["blocks"]) * f.blocksize >= f.size:
            c["blocks"] = True

    def pop_file(self, path: str) -> str | None:
        """Remove metadata of cached file.

        If path is in the cache, return the filename of the cached file,
        otherwise return ``None``.  Caller is responsible for deleting the
        cached file.
        """
        details = self._check_file_with_writable(path, None)
        if not details:
            if (
                path in self.cached_files[-1]
                and self._resolve_cached_file(
                    self._storage[-1], self.cached_files[-1][path].get("fn")
                )
                is None
            ):
                self.cached_files[-1].pop(path, None)
                metadata_path = os.path.join(self._storage[-1], self._metadata_name)
                if os.path.lexists(metadata_path):
                    self._save_writable(deleted_path=path)
            return None
        _, fn, writable = details
        if writable:
            self.cached_files[-1].pop(path)
            self._save_writable(deleted_path=path)
        else:
            raise PermissionError(
                "Can only delete cached file in last, writable cache location"
            )
        return fn

    def _save_writable(self, deleted_path: str | None = None) -> None:
        for (fn, base, writable), cache in zip(
            self._scan_locations(), self.cached_files
        ):
            if not writable:
                continue

            if os.path.lexists(fn):
                loaded_files = self._load(fn)
                if deleted_path is not None:
                    loaded_files.pop(deleted_path, None)
                cached_files: Detail = {
                    k: c
                    for k, c in loaded_files.items()
                    if self._resolve_cached_file(base, c.get("fn")) is not None
                }
                for k, entry in cache.items():
                    if self._resolve_cached_file(base, entry.get("fn")) is None:
                        continue
                    c = cached_files.get(k)
                    if (
                        c is not None
                        and c.get("fn") == entry.get("fn")
                        and c.get("uid") == entry.get("uid")
                        and (
                            c.get("blocks") is True or isinstance(c.get("blocks"), set)
                        )
                    ):
                        merged = entry.copy()
                        if c["blocks"] is True or entry["blocks"] is True:
                            merged["blocks"] = True
                        else:
                            # self.cached_files[*][*]["blocks"] must continue to
                            # point to the same set object so that updates
                            # performed by MMapCache are propagated back to
                            # self.cached_files.
                            blocks = entry["blocks"]
                            blocks.update(c["blocks"])
                            merged["blocks"] = blocks
                        merged["time"] = max(c.get("time", 0), entry["time"])
                        cached_files[k] = merged
                    else:
                        cached_files[k] = entry
            else:
                cached_files = {
                    k: entry
                    for k, entry in cache.items()
                    if self._resolve_cached_file(base, entry.get("fn")) is not None
                }
            self._save(self._prepare_for_save(cached_files), fn)
            self.cached_files[-1] = cached_files

    def save(self) -> None:
        """Save metadata to disk"""
        self._save_writable()

    def update_file(self, path: str, detail: Detail) -> None:
        """Update metadata for specific file in memory, do not save"""
        self.cached_files[-1][path] = detail
