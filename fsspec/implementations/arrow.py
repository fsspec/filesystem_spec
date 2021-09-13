from contextlib import closing

from fsspec import AbstractFileSystem
from fsspec.utils import stringify_path


class ArrowFSWrapper(AbstractFileSystem):
    """FSSpec-compatible wrapper of pyarrow.fs.FileSystem.

    Parameters
    ----------
    fs : pyarrow.fs.FileSystem

    """

    # root_marker = "/"
    # protocol = "file"
    # local_file = True

    def __init__(self, fs, **kwargs):
        super().__init__(**kwargs)
        try:
            import pyarrow.fs
        except ImportError:
            raise ImportError("pyarrow required to use the ArrowFSWrapper")

        if not isinstance(fs, pyarrow.fs.FileSystem):
            raise TypeError(
                "'fs' should be an instance of a pyarrow.fs.FileSystem subclass"
            )
        self.fs = fs

    @classmethod
    def _parent(cls, path):
        path = cls._strip_protocol(path).rstrip("/")
        if "/" in path:
            return path.rsplit("/", 1)[0]
        else:
            return cls.root_marker

    @classmethod
    def _strip_protocol(cls, path):
        path = stringify_path(path)
        _, _, path = path.partition("://")
        return path

    def ls(self, path, detail=False, **kwargs):
        from pyarrow.fs import FileSelector

        entries = [
            self._make_entry(entry) for entry in self.get_file_info(FileSelector(path))
        ]
        if detail:
            return entries
        else:
            return [entry["name"] for entry in entries]

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        [info] = self.fs.get_file_info([path])
        return self._make_entry(info)

    def _make_entry(self, info):
        from pyarrow.fs import FileType

        if info.type is FileType.Directory:
            kind = "directory"
        elif info.type is FileType.File:
            kind = "file"
        else:
            kind = "other"

        return {
            "name": info.path,
            "size": info.size,
            "type": kind,
            "mtime": info.mtime,
        }

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1).rstrip("/")
        path2 = self._strip_protocol(path2).rstrip("/")
        self.fs.copy_file(path1, path2)

    def get_file(self, path1, path2, **kwargs):
        return self.cp_file(path1, path2, **kwargs)

    def put_file(self, path1, path2, **kwargs):
        return self.cp_file(path1, path2, **kwargs)

    def mv_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1).rstrip("/")
        path2 = self._strip_protocol(path2).rstrip("/")
        self.fs.move(path1, path2)

    def rm_file(self, path):
        path = self._strip_protocol(path)
        self.fs.delete_file(path)

    def rm(self, path, recursive=False, maxdepth=None):
        path = self._strip_protocol(path).rstrip("/")
        if recursive and self.isdir(path):
            self.fs.delete_dir(path)
        else:
            self.fs.delete_file(path)

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        if mode == "rb":
            stream = self.fs.open_input_stream(path)
        elif mode == "wb":
            stream = self.fs.open_output_stream(path)
        else:
            raise ValueError(f"unsupported mode for Arrow filesystem: {mode!r}")

        return closing(stream)

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        if create_parents:
            self.makedirs(path, exist_ok=True)
        else:
            self.fs.create_dir(path, recursive=False)

    def makedirs(self, path, exist_ok=False):
        path = self._strip_protocol(path)
        self.fs.create_dir(path, recursive=True)

    def rmdir(self, path):
        path = self._strip_protocol(path)
        self.fs.delete_dir(path)
