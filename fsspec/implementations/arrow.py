import datetime
import io
import os
import posixpath
import re
import shutil
import tempfile

from contextlib import closing

from fsspec import AbstractFileSystem
from fsspec.implementations.local import make_path_posix
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

    def ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        paths = [posixpath.join(path, f) for f in os.listdir(path)]
        if detail:
            return [self.info(f) for f in paths]
        else:
            return paths

    def glob(self, path, **kwargs):
        path = self._strip_protocol(path)
        return super().glob(path, **kwargs)

    def info(self, path, **kwargs):
        print("getting info for ", path)
        path = self._strip_protocol(path)
        info = self.fs.get_file_info([path])[0]
        dest = False

        from pyarrow.fs import FileType

        if info.type == FileType.Directory:
            t = "directory"
        elif info.is_file:
            t = "file"
        else:
            t = "other"
        result = {
            "name": path,
            "size": info.size,
            "type": t,
            "created": None,
            "mtime": info.mtime,
        }
        return result

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1).rstrip("/")
        path2 = self._strip_protocol(path2).rstrip("/")
        if self.auto_mkdir:
            self.makedirs(self._parent(path2), exist_ok=True)
        if self.isfile(path1):
            shutil.copyfile(path1, path2)
        else:
            self.mkdirs(path2, exist_ok=True)

    def get_file(self, path1, path2, **kwargs):
        return self.cp_file(path1, path2, **kwargs)

    def put_file(self, path1, path2, **kwargs):
        return self.cp_file(path1, path2, **kwargs)

    def mv_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1).rstrip("/")
        path2 = self._strip_protocol(path2).rstrip("/")
        os.rename(path1, path2)

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

    def touch(self, path, **kwargs):
        path = self._strip_protocol(path)
        # if self.auto_mkdir:
        #     self.makedirs(self._parent(path), exist_ok=True)
        if self.exists(path):
            pass  # os.utime(path, None)
        else:
            self.fs.open_input_file(path).close()

    def created(self, path):
        info = self.info(path=path)
        return datetime.datetime.utcfromtimestamp(info["created"])

    def modified(self, path):
        info = self.info(path=path)
        return datetime.datetime.utcfromtimestamp(info["mtime"])

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
        if path.startswith("file://"):
            path = path[7:]
        path = os.path.expanduser(path)
        return make_path_posix(path)

    def _isfilestore(self):
        # Inheriting from DaskFileSystem makes this False (S3, etc. were)
        # the original motivation. But we are a posix-like file system.
        # See https://github.com/dask/dask/issues/5526
        return True
