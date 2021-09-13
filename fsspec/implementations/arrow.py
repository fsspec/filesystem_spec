import datetime
import io
import os
import posixpath
import re
import shutil
import tempfile

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
        path = self._strip_protocol(path)
        if self.auto_mkdir and "w" in mode:
            self.makedirs(self._parent(path), exist_ok=True)
        return LocalFileOpener(path, mode, fs=self, **kwargs)

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


def make_path_posix(path, sep=os.sep):
    """ Make path generic """
    if isinstance(path, (list, set, tuple)):
        return type(path)(make_path_posix(p) for p in path)
    if re.match("/[A-Za-z]:", path):
        # for windows file URI like "file:///C:/folder/file"
        # or "file:///C:\\dir\\file"
        path = path[1:]
    if path.startswith("\\\\"):
        # special case for windows UNC/DFS-style paths, do nothing,
        # just flip the slashes around (case below does not work!)
        return path.replace("\\", "/")
    if re.match("[A-Za-z]:", path):
        # windows full path like "C:\\local\\path"
        return path.lstrip("\\").replace("\\", "/").replace("//", "/")
    if path.startswith("\\"):
        # windows network path like "\\server\\path"
        return "/" + path.lstrip("\\").replace("\\", "/").replace("//", "/")
    if (
        sep not in path
        and "/" not in path
        or (sep == "/" and not path.startswith("/"))
        or (sep == "\\" and ":" not in path)
    ):
        # relative path like "path" or "rel\\path" (win) or rel/path"
        path = os.path.abspath(path)
        if os.sep == "\\":
            # abspath made some more '\\' separators
            return make_path_posix(path, sep)
    return path


class LocalFileOpener(object):
    def __init__(self, path, mode, autocommit=True, fs=None, **kwargs):
        self.path = path
        self.mode = mode
        self.fs = fs
        self.f = None
        self.autocommit = autocommit
        self.blocksize = io.DEFAULT_BUFFER_SIZE
        self._open()

    def _open(self):
        if self.f is None or self.f.closed:
            if self.autocommit or "w" not in self.mode:
                self.f = open(self.path, mode=self.mode)
            else:
                # TODO: check if path is writable?
                i, name = tempfile.mkstemp()
                os.close(i)  # we want normal open and normal buffered file
                self.temp = name
                self.f = open(name, mode=self.mode)
            if "w" not in self.mode:
                self.details = self.fs.info(self.path)
                self.size = self.details["size"]
                self.f.size = self.size

    def _fetch_range(self, start, end):
        # probably only used by cached FS
        if "r" not in self.mode:
            raise ValueError
        self._open()
        self.f.seek(start)
        return self.f.read(end - start)

    def __setstate__(self, state):
        self.f = None
        loc = state.pop("loc", None)
        self.__dict__.update(state)
        if "r" in state["mode"]:
            self.f = None
            self._open()
            self.f.seek(loc)

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop("f")
        if "r" in self.mode:
            d["loc"] = self.f.tell()
        else:
            if not self.f.closed:
                raise ValueError("Cannot serialise open write-mode local file")
        return d

    def commit(self):
        if self.autocommit:
            raise RuntimeError("Can only commit if not already set to autocommit")
        os.replace(self.temp, self.path)

    def discard(self):
        if self.autocommit:
            raise RuntimeError("Cannot discard if set to autocommit")
        os.remove(self.temp)

    def __fspath__(self):
        # uniquely among fsspec implementations, this is a real, local path
        return self.path

    def __iter__(self):
        return self.f.__iter__()

    def __getattr__(self, item):
        return getattr(self.f, item)

    def __enter__(self):
        self._incontext = True
        return self.f.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self._incontext = False
        self.f.__exit__(exc_type, exc_value, traceback)
