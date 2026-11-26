import operator

from fsspec import AbstractFileSystem
from fsspec.utils import tokenize


class AbstractArchiveFileSystem(AbstractFileSystem):
    """
    A generic superclass for implementing Archive-based filesystems.

    Currently, it is shared amongst
    :class:`~fsspec.implementations.zip.ZipFileSystem`,
    :class:`~fsspec.implementations.libarchive.LibArchiveFileSystem` and
    :class:`~fsspec.implementations.tar.TarFileSystem`.
    """

    def __str__(self):
        return f"<Archive-like object {type(self).__name__} at {id(self)}>"

    __repr__ = __str__

    def ukey(self, path):
        return tokenize(path, self.fo, self.protocol)

    def _not_a_file(self, path):
        """The error for opening ``path``, which is not a file member."""
        return IsADirectoryError(path) if self.isdir(path) else FileNotFoundError(path)

    @staticmethod
    def _normalize_path(path):
        path = path.strip("/")
        if (
            "//" not in path
            and "/./" not in path
            and not path.startswith("./")
            and not path.endswith("/.")
            and path != "."
        ):
            return path
        # Keep '..' because it can be part of a literal archive member name.
        return "/".join(part for part in path.split("/") if part not in ("", "."))

    def _all_dirnames(self, paths):
        """Returns *all* directory names for each path in paths, including intermediate
        ones.

        Parameters
        ----------
        paths: Iterable of path strings
        """
        if len(paths) == 0:
            return set()

        dirnames = {self._parent(path) for path in paths} - {self.root_marker}
        return dirnames | self._all_dirnames(dirnames)

    def _all_dirnames_from_normalized(self, paths):
        """Collect parents of relative ZIP and TAR member keys."""
        dirnames = set()
        protocol_prefix = self.protocol + "::"
        for path in paths:
            # A literal protocol prefix is still special to _parent, even
            # after member normalization removes redundant slashes.
            if path.startswith(protocol_prefix):
                return self._all_dirnames(paths)
            parent = path.rpartition("/")[0]
            while parent and parent not in dirnames:
                dirnames.add(parent)
                parent = parent.rpartition("/")[0]
        return dirnames

    def info(self, path, **kwargs):
        self._get_dirs()
        path = self._strip_protocol(path)
        if path in {"", "/"}:
            return {"name": "", "type": "directory", "size": 0}
        if path in self.dir_cache:
            return self.dir_cache[path]
        elif path + "/" in self.dir_cache:
            return self.dir_cache[path + "/"]
        else:
            raise FileNotFoundError(path)

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
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
                    out = {"name": ppath, "size": 0, "type": "directory"}
                    paths[ppath] = out
        if not paths and path.strip("/"):
            # Nothing is listed under ``path``, so it is a file, an empty
            # directory or missing; ``info`` raises FileNotFoundError for the last.
            info = self.info(path)
            if info["type"] != "directory":
                paths[info["name"]] = info
        if detail:
            out = sorted(paths.values(), key=operator.itemgetter("name"))
            return out
        else:
            return sorted(paths)
