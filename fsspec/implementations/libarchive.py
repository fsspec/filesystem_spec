from __future__ import print_function, division, absolute_import

from contextlib import contextmanager

import libarchive
from fsspec import AbstractFileSystem, open_files
from fsspec.utils import tokenize, DEFAULT_BLOCK_SIZE
from fsspec.implementations.memory import MemoryFile


class LibArchiveFileSystem(AbstractFileSystem):
    """Read contents of compressed archive as a file-system

    Keeps file object open while instance lives.

    This class is pickleable, but not necessarily thread-safe
    """

    root_marker = ""

    extensions = "7z", "rar"

    def __init__(
        self,
        fo="",
        mode="r",
        target_protocol=None,
        target_options=None,
        block_size=DEFAULT_BLOCK_SIZE,
        **kwargs
    ):
        """
        Parameters
        ----------
        fo: str or file-like
            Contains ZIP, and must exist. If a str, will fetch file using
            `open_files()`, which must return one file exactly.
        mode: str
            Currently, only 'r' accepted
        target_protocol: str (optional)
            If ``fo`` is a string, this value can be used to override the
            FS protocol inferred from a URL
        target_options: dict (optional)
            Kwargs passed when instantiating the target FS, if ``fo`` is
            a string.
        """
        super().__init__(self, **kwargs)
        if mode != "r":
            raise ValueError("Only read from archive files accepted")
        if isinstance(fo, str):
            files = open_files(fo, protocol=target_protocol, **(target_options or {}))
            if len(files) != 1:
                raise ValueError(
                    'Path "{}" did not resolve to exactly'
                    'one file: "{}"'.format(fo, files)
                )
            fo = files[0]
        self.fo = fo.__enter__()  # the whole instance is a context
        # self.arc_reader =
        self.block_size = block_size
        self.dir_cache = None

    @contextmanager
    def _open_archive(self):
        self.fo.seek(0)
        with libarchive.fd_reader(self.fo.fileno(), block_size=self.block_size) as arc:
            yield arc

    @classmethod
    def _strip_protocol(cls, path):
        # file paths are always relative to the archive root
        return super()._strip_protocol(path).lstrip("/")

    def _get_dirs(self):
        fields = {
            "name": "pathname",
            "size": "size",
            "created": "ctime",
            "mode": "mode",
            "uid": "uid",
            "gid": "gid",
            "mtime": "mtime",
        }

        if self.dir_cache is not None:
            return

        self.dir_cache = {}
        list_names = []
        with self._open_archive() as arc:
            for entry in arc:
                if not entry.isdir and not entry.isfile:
                    # Skip symbolic links, fifo entries, etc.
                    continue
                self.dir_cache.update(
                    {
                        dirname
                        + "/": {"name": dirname + "/", "size": 0, "type": "directory"}
                        for dirname in self._all_dirnames(set(entry.name))
                    }
                )
                f = {key: getattr(entry, fields[key]) for key in fields}
                f["type"] = "directory" if entry.isdir else "file"
                list_names.append(entry.name)

                self.dir_cache[f["name"]] = f
        # libarchive does not seem to return an entry for the directories (at least
        # not in all formats), so get the directories names from the files names
        self.dir_cache.update(
            {
                dirname + "/": {"name": dirname + "/", "size": 0, "type": "directory"}
                for dirname in self._all_dirnames(list_names)
            }
        )

    def info(self, path, **kwargs):
        self._get_dirs()
        path = self._strip_protocol(path)
        if path in self.dir_cache:
            return self.dir_cache[path]
        elif path + "/" in self.dir_cache:
            return self.dir_cache[path + "/"]
        else:
            raise FileNotFoundError(path)

    def ls(self, path, detail=False, **kwargs):
        self._get_dirs()
        paths = {}

        for p, f in self.dir_cache.items():
            p = p.rstrip("/")
            if "/" in p:
                root = p.rsplit("/", 1)[0]
            else:
                root = ""
            if root == path.rstrip("/"):
                paths[p] = f
            elif all(
                (a == b)
                for a, b in zip(path.split("/"), [""] + p.strip("/").split("/"))
            ):
                # root directory entry
                ppath = p.rstrip("/").split("/", 1)[0]
                if ppath not in paths:
                    out = {"name": ppath + "/", "size": 0, "type": "directory"}
                    paths[ppath] = out
        out = list(paths.values())
        if detail:
            return out
        else:
            return list(sorted(f["name"] for f in out))

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        path = self._strip_protocol(path)
        if mode != "rb":
            raise NotImplementedError

        data = bytes()
        with self._open_archive() as arc:
            # FIXME? dropwhile would increase performance but less readable
            for entry in arc:
                if entry.pathname != path:
                    continue
                for block in entry.get_blocks(entry.size):
                    data = block
                    break
                else:
                    raise ValueError
        return MemoryFile(fs=self, path=path, data=data)

    def ukey(self, path):
        return tokenize(path, self.fo, self.protocol)

    def _all_dirnames(self, paths):
        """Returns *all* directory names for each path in paths, including intermediate ones.

        Parameters
        ----------
        paths: Iterable of path strings
        """
        if len(paths) == 0:
            return set()

        dirnames = {self._parent(path) for path in paths} - {self.root_marker}
        return dirnames | self._all_dirnames(dirnames)
