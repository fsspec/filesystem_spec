import copy
import tarfile
import fsspec
from fsspec.compression import compr

typemap = {b"0": "file", b"5": "directory"}


class TarFileSystem(fsspec.AbstractFileSystem):
    def __init__(
        self, fo, index_store=None, storage_options=None, compression=None, **kwargs
    ):
        super().__init__(**kwargs)
        if isinstance(fo, str):
            fo = fsspec.open(fo, **(storage_options or {})).open()
        if compression:
            # TODO: tarfile already implements compression with modes like "'r:gz'",
            #  but then would seek to offset in the file work?
            # TODO: "infer" is not supported here
            fo = compr[compression](fo)
        self.fo = fo
        self.index_store = index_store
        self.index = None
        self._index()

    def _index(self):
        # TODO: load and set saved index, if exists
        fo = tarfile.TarFile(fileobj=self.fo)
        out = {}
        for ti in fo:
            info = ti.get_info()
            info["type"] = typemap[info["type"]]
            out[ti.get_info()["name"]] = (info, ti.offset_data)

        self.index = out
        # TODO: save index to self.index_store here, if set

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        parts = path.rstrip("/").split("/")
        out = []
        for name, (details, _) in self.index.items():
            nparts = name.rstrip("/").split("/")
            if parts == nparts and details["type"] != "directory":
                out = [details]
                break
            if len(nparts) and parts == nparts[:-1]:
                out.append(details)
        if detail:
            return out
        else:
            return [o["name"] for o in out]

    def info(self, path, **kwargs):
        return self.index[path][0]

    def _open(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError("Read Only filesystem implementation")
        details, offset = self.index[path]
        if details["type"] != "file":
            raise ValueError("Can only regilar files")
        newfo = copy.copy(self.fo)
        newfo.seek(offset)
        return TarContainedFile(newfo, self.info(path))


class TarContainedFile(object):
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
