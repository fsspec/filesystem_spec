from typing import Any, Iterable, Sequence, Union

import fsspec
from fsspec import AbstractFileSystem
from fsspec.core import split_protocol
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import stringify_path


class PrefixBufferedFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        pass


class PrefixFileSystem(AbstractFileSystem):
    """A meta-filesystem to add a prefix and delegate to another filesystem"""

    def __init__(
        self,
        prefix: str,
        fs: AbstractFileSystem,
        *args,
        **storage_options,
    ) -> None:
        """
        Parameters
        ----------
        prefix: str
            The prefix to append to all paths

        fs: fsspec.AbstractFileSystem
            An instantiated filesystem to wrap. All operations are delegated to
            this filesystem after appending the specified prefix
        """
        super().__init__(*args, **storage_options)
        self.prefix = stringify_path(prefix)

        if not self.prefix:
            raise ValueError(f"empty prefix is not a valid prefix")

        self.fs = fs

    def _get_relative_path(self, path: str) -> str:
        if path[: len(self.root_marker)] == self.root_marker:
            return path[len(self.root_marker) :]
        return path

    def _add_fs_prefix(self, path: str) -> Union[str, Sequence[str]]:
        if isinstance(path, str):
            path = stringify_path(path)
            protocol, path = split_protocol(path)

            path = self._get_relative_path(path)

            if self.prefix == self.root_marker:
                path = f"{self.root_marker}{path}"  # don't add twice the same sep
            else:
                path = f"{self.prefix}{self.sep}{path}"

            return protocol + "://" + path if protocol is not None else path
        elif isinstance(path, Iterable):
            return [self._add_fs_prefix(x) for x in path]
        assert False

    def _remove_fs_prefix(self, path: str) -> Union[str, Sequence[str]]:
        if isinstance(path, str):
            path = stringify_path(path)
            protocol, path = split_protocol(path)
            path = path[len(self.prefix) + 1 :]
            return protocol + "://" + path if protocol is not None else path
        elif isinstance(path, Iterable):
            return [self._remove_fs_prefix(x) for x in path]
        assert False

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        path = self._add_fs_prefix(path)
        return self.fs.mkdir(path=path, create_parents=create_parents, **kwargs)

    def makedirs(self, path: str, exist_ok: bool = False):
        path = self._add_fs_prefix(path)
        return self.fs.makedirs(path=path, exist_ok=exist_ok)

    def rmdir(self, path: str):
        path = self._add_fs_prefix(path)
        return self.fs.rmdir(path=path)

    def ls(
        self,
        path: str,
        detail=False,
        **kwargs,
    ) -> Sequence[str]:
        path = self._add_fs_prefix(path)
        ls_out = self.fs.ls(path=path, detail=detail, **kwargs)
        if detail:
            for out in ls_out:
                out["name"] = self._remove_fs_prefix(out["name"])
            return ls_out
        return self._remove_fs_prefix(ls_out)

    def glob(self, path: str, **kwargs):
        path = self._add_fs_prefix(path)
        glob_out = self.fs.glob(path=path, **kwargs)
        return [self._remove_fs_prefix(x) for x in glob_out]

    def info(self, path: str, **kwargs):
        path = self._add_fs_prefix(path)
        return self.fs.info(path=path, **kwargs)

    def cp_file(self, path1: str, path2: str, **kwargs):
        path1 = self._add_fs_prefix(path1)
        path2 = self._add_fs_prefix(path2)
        return self.fs.cp_file(path1, path2, **kwargs)

    def get_file(self, path1: str, path2: str, callback=None, **kwargs):
        path1 = self._add_fs_prefix(path1)
        path2 = self._add_fs_prefix(path2)
        return self.fs.get_file(path1, path2, callback, **kwargs)

    def put_file(self, path1: str, path2: str, callback=None, **kwargs):
        path1 = self._add_fs_prefix(path1)
        path2 = self._add_fs_prefix(path2)
        return self.fs.put_file(path1, path2, callback, **kwargs)

    def mv_file(self, path1: str, path2: str, **kwargs):
        path1 = self._add_fs_prefix(path1)
        path2 = self._add_fs_prefix(path2)
        return self.fs.mv_file(path1, path2, **kwargs)

    def rm_file(self, path: str):
        path = self._add_fs_prefix(path)
        return self.fs.rm_file(path)

    def rm(self, path: str, recursive=False, maxdepth=None):
        path = self._add_fs_prefix(path)
        return self.fs.rm(path, recursive=recursive, maxdepth=maxdepth)

    def touch(self, path: str, **kwargs):
        path = self._add_fs_prefix(path)
        return self.fs.touch(path, **kwargs)

    def created(self, path: str):
        path = self._add_fs_prefix(path)
        return self.fs.created(path)

    def modified(self, path: str):
        path = self._add_fs_prefix(path)
        return self.fs.modified(path)

    def sign(self, path: str, expiration=100, **kwargs):
        path = self._add_fs_prefix(path)
        return self.fs.sign(path, expiration=expiration, **kwargs)

    def cat(
        self,
        path: str,
        recursive: bool = False,
        on_error: str = "raise",
        **kwargs: Any,
    ):
        path = self._add_fs_prefix(path)
        return self.fs.cat(path, recursive=recursive, on_error=on_error, **kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(prefix='{self.prefix}', fs={self.fs})"

    def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        """
        Return a file-like object from the fs

        The file-like object returned ignores an eventual PrefixFileSystem:
            - the ``.path`` attribute is always an absolute path
            - the ``.fs`` attribute, if present, would be the wrapped file-system

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        path: str
            Target file
        mode: str like 'rb', 'w'
            See builtin ``open()``
        block_size: int
            Some indication of buffering - this is a value in bytes
        cache_options : dict, optional
            Extra arguments to pass through to the cache.
        encoding, errors, newline: passed on to TextIOWrapper for text mode
        """
        return self.fs.open(
            self._add_fs_prefix(path),
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            **kwargs,
        )
