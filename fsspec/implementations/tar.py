import copy
import logging
import tarfile
import weakref
from io import BufferedReader

import fsspec
from fsspec.archive import AbstractArchiveFileSystem
from fsspec.compression import compr
from fsspec.utils import infer_compression

typemap = {b"0": "file", b"5": "directory"}

logger = logging.getLogger("tar")


class TarFileSystem(AbstractArchiveFileSystem):
    """Compressed Tar archives as a file-system (read-only)

    Supports the following formats:
    tar.gz, tar.bz2, tar.xz
    """

    root_marker = ""
    protocol = "tar"
    cachable = False

    def __init__(
        self,
        fo="",
        index_store=None,
        target_options=None,
        target_protocol=None,
        compression=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        target_options = target_options or {}

        if isinstance(fo, str):
            fo = fsspec.open(fo, protocol=target_protocol, **target_options).open()

        # Try to infer compression.
        if compression is None:
            name = None

            # Try different ways to get hold of the filename. `fo` might either
            # be a `fsspec.LocalFileOpener`, an `io.BufferedReader` or an
            # `fsspec.AbstractFileSystem` instance.
            try:
                # Amended io.BufferedReader or similar.
                # This uses a "protocol extension" where original filenames are
                # propagated to archive-like filesystems in order to let them
                # infer the right compression appropriately.
                if hasattr(fo, "original"):
                    name = fo.original

                # fsspec.LocalFileOpener
                elif hasattr(fo, "path"):
                    name = fo.path

                # io.BufferedReader
                elif hasattr(fo, "name"):
                    name = fo.name

                # fsspec.AbstractFileSystem
                elif hasattr(fo, "info"):
                    name = fo.info()["name"]

            except Exception as ex:
                logger.warning(
                    f"Unable to determine file name, not inferring compression: {ex}"
                )

            if name is not None:
                compression = infer_compression(name)
                logger.info(f"Inferred compression {compression} from file name {name}")

        if compression is not None:
            # TODO: tarfile already implements compression with modes like "'r:gz'",
            #  but then would seek to offset in the file work?
            fo = compr[compression](fo)

        self._fo_ref = fo
        weakref.finalize(self, fo.close)
        self.fo = fo.__enter__()  # the whole instance is a context
        self.tar = tarfile.TarFile(fileobj=self.fo)
        self.dir_cache = None

        self.index_store = index_store
        self.index = None
        self._index()

    def _index(self):
        # TODO: load and set saved index, if exists
        out = {}
        for ti in self.tar:
            info = ti.get_info()
            info["type"] = typemap.get(info["type"], "file")
            name = ti.get_info()["name"].rstrip("/")
            out[name] = (info, ti.offset_data)

        self.index = out
        # TODO: save index to self.index_store here, if set

    def _get_dirs(self):
        if self.dir_cache is not None:
            return

        # This enables ls to get directories as children as well as files
        self.dir_cache = {
            dirname + "/": {"name": dirname + "/", "size": 0, "type": "directory"}
            for dirname in self._all_dirnames(self.tar.getnames())
        }
        for member in self.tar.getmembers():
            info = member.get_info()
            info["type"] = typemap.get(info["type"], "file")
            self.dir_cache[info["name"]] = info

    def _open(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError("Read-only filesystem implementation")
        details, offset = self.index[path]
        if details["type"] != "file":
            raise ValueError("Can only handle regular files")

        # `LocalFileSystem` offers its resources as `io.BufferedReader`
        # objects, those can't be copied.
        if isinstance(self.fo, BufferedReader):
            newfo = self.fo
        else:
            newfo = copy.copy(self.fo)
        newfo.seek(offset)

        return TarContainedFile(newfo, self.info(path))


class TarContainedFile(object):
    """
    Represent/wrap a TarFileSystem's file object.
    """

    def __init__(self, of, info):
        self.info = info
        self.size = info["size"]
        self.of = of
        self.start = of.tell()
        self.end = self.start + self.size
        self.closed = False

    def tell(self):
        return self.of.tell() - self.start

    def read(self, n=-1):
        if self.closed:
            raise ValueError("file is closed")
        if n < 0:
            n = self.end - self.of.tell()
        if n > self.end - self.tell():
            n = self.end - self.tell()
        if n < 1:
            return b""
        return self.of.read(n)

    def seek(self, to, whence=0):
        if self.closed:
            raise ValueError("file is closed")
        if whence == 0:
            to = min(max(self.start, self.start + to), self.end)
        elif whence == 1:
            to = min(max(self.start, self.tell() + to), self.end)
        elif whence == 2:
            to = min(max(self.start, self.end + to), self.end)
        else:
            raise ValueError("Whence must be (0, 1, 2)")
        self.of.seek(to)

    def close(self):
        self.of.close()
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
