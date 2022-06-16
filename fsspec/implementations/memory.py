from __future__ import absolute_import, division, print_function

import logging
from datetime import datetime
from errno import ENOTEMPTY
from io import BytesIO

from fsspec import AbstractFileSystem

logger = logging.Logger("fsspec.memoryfs")


class MemoryFileSystem(AbstractFileSystem):
    """A filesystem based on a dict of BytesIO objects

    Parameters
    ----------
    global_store : bool (True)
        Whether to use global store, so instances of this class all point to
        the same in memory filesystem. If False, every instance will have its
        own store.
    """

    store = {}  # global
    pseudo_dirs = [""]  # global
    protocol = "memory"
    root_marker = "/"

    def __init__(self, *args, **storage_options):
        super().__init__(*args, **storage_options)
        if not storage_options.get("global_store", True):
            # detach from global
            self.store = {}
            self.pseudo_dirs = [""]

    @classmethod
    def _strip_protocol(cls, path):
        if path.startswith("memory://"):
            path = path[len("memory://") :]
        if "::" in path or "://" in path:
            return path.rstrip("/")
        path = path.lstrip("/").rstrip("/")
        return "/" + path if path else ""

    def ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        if path in self.store:
            # there is a key with this exact name
            return [
                {
                    "name": path,
                    "size": self.store[path].getbuffer().nbytes,
                    "type": "file",
                    "created": self.store[path].created,
                }
            ]
        paths = set()
        starter = path + "/"
        out = []
        for p2 in tuple(self.store):
            if p2.startswith(starter):
                if "/" not in p2[len(starter) :]:
                    # exact child
                    out.append(
                        {
                            "name": p2,
                            "size": self.store[p2].getbuffer().nbytes,
                            "type": "file",
                            "created": self.store[p2].created,
                        }
                    )
                elif len(p2) > len(starter):
                    # implied child directory
                    ppath = starter + p2[len(starter) :].split("/", 1)[0]
                    if ppath not in paths:
                        out = out or []
                        out.append(
                            {
                                "name": ppath,
                                "size": 0,
                                "type": "directory",
                            }
                        )
                        paths.add(ppath)
        for p2 in self.pseudo_dirs:
            if p2.startswith(starter):
                if "/" not in p2[len(starter) :]:
                    # exact child pdir
                    if p2 not in paths:
                        out.append({"name": p2, "size": 0, "type": "directory"})
                        paths.add(p2)
                else:
                    # directory implied by deeper pdir
                    ppath = starter + p2[len(starter) :].split("/", 1)[0]
                    if ppath not in paths:
                        out.append({"name": ppath, "size": 0, "type": "directory"})
                        paths.add(ppath)
        if not out:
            if path in self.pseudo_dirs:
                # empty dir
                return []
            raise FileNotFoundError(path)
        if detail:
            return out
        return sorted([f["name"] for f in out])

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        if path in self.store or path in self.pseudo_dirs:
            raise FileExistsError
        if self._parent(path).strip("/") and self.isfile(self._parent(path)):
            raise NotADirectoryError(self._parent(path))
        if create_parents and self._parent(path).strip("/"):
            try:
                self.mkdir(self._parent(path), create_parents, **kwargs)
            except FileExistsError:
                pass
        if path and path not in self.pseudo_dirs:
            self.pseudo_dirs.append(path)

    def makedirs(self, path, exist_ok=False):
        try:
            self.mkdir(path, create_parents=True)
        except FileExistsError:
            if not exist_ok:
                raise

    def rmdir(self, path):
        path = self._strip_protocol(path)
        if path == "":
            # silently avoid deleting FS root
            return
        if path in self.pseudo_dirs:
            if not self.ls(path):
                self.pseudo_dirs.remove(path)
            else:
                raise OSError(ENOTEMPTY, "Directory not empty", path)
        else:
            raise FileNotFoundError(path)

    def exists(self, path, **kwargs):
        path = self._strip_protocol(path)
        return path in self.store or path in self.pseudo_dirs

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        if path in self.pseudo_dirs or any(
            p.startswith(path + "/") for p in list(self.store) + self.pseudo_dirs
        ):
            return {
                "name": path,
                "size": 0,
                "type": "directory",
            }
        elif path in self.store:
            filelike = self.store[path]
            return {
                "name": path,
                "size": filelike.size
                if hasattr(filelike, "size")
                else filelike.getbuffer().nbytes,
                "type": "file",
                "created": getattr(filelike, "created", None),
            }
        else:
            raise FileNotFoundError(path)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        path = self._strip_protocol(path)
        if path in self.pseudo_dirs:
            raise IsADirectoryError
        parent = path
        while len(parent) > 1:
            parent = self._parent(parent)
            if self.isfile(parent):
                raise FileExistsError(parent)
        if mode in ["rb", "ab", "rb+"]:
            if path in self.store:
                f = self.store[path]
                if mode == "ab":
                    # position at the end of file
                    f.seek(0, 2)
                else:
                    # position at the beginning of file
                    f.seek(0)
                return f
            else:
                raise FileNotFoundError(path)
        if mode == "wb":
            m = MemoryFile(self, path)
            if not self._intrans:
                m.commit()
            return m

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        if self.isfile(path1):
            self.store[path2] = MemoryFile(self, path2, self.store[path1].getbuffer())
        elif self.isdir(path1):
            if path2 not in self.pseudo_dirs:
                self.pseudo_dirs.append(path2)
        else:
            raise FileNotFoundError

    def cat_file(self, path, start=None, end=None, **kwargs):
        path = self._strip_protocol(path)
        try:
            return self.store[path].getvalue()[start:end]
        except KeyError:
            raise FileNotFoundError(path)

    def _rm(self, path):
        path = self._strip_protocol(path)
        try:
            del self.store[path]
        except KeyError as e:
            raise FileNotFoundError from e

    def rm(self, path, recursive=False, maxdepth=None):
        if isinstance(path, str):
            path = self._strip_protocol(path)
        else:
            path = [self._strip_protocol(p) for p in path]
        paths = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        for p in reversed(paths):
            # If the expanded path doesn't exist, it is only because the expanded
            # path was a directory that does not exist in self.pseudo_dirs. This
            # is possible if you directly create files without making the
            # directories first.
            if not self.exists(p):
                continue
            if self.isfile(p):
                self.rm_file(p)
            else:
                self.rmdir(p)


class MemoryFile(BytesIO):
    """A BytesIO which can't close and works as a context manager

    Can initialise with data. Each path should only be active once at any moment.

    No need to provide fs, path if auto-committing (default)
    """

    def __init__(self, fs=None, path=None, data=None):
        self.fs = fs
        self.path = path
        self.created = datetime.utcnow().timestamp()
        if data:
            self.write(data)
            self.size = len(data)
            self.seek(0)

    def __enter__(self):
        return self

    def close(self):
        position = self.tell()
        self.size = self.seek(0, 2)
        self.seek(position)

    def discard(self):
        pass

    def commit(self):
        self.fs.store[self.path] = self
