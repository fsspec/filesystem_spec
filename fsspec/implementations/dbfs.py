import base64
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
import requests


class DatabricksFileSystem(AbstractFileSystem):

    def __init__(self, token, instance, **kwargs):
        self.token = token
        self.instance = instance
        self.session = requests.Session()
        self.session.headers.update(self.header)
        super().__init__(**kwargs)

    @property
    def header(self):
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def url(self)
        return f"https://{self.instance}/api/2.0/clusters/list"

    def ls(self, path, detail=True):
        out = self._ls_from_cache(path)
        if not out:
            r = self.session.get(self.url + "/2.0/dbfs/list", json={"path": path})
            r.raise_for_status()
            out = [{"name": o["path"],
                    "type": ["directory", "file"][o["is_dir"]],
                    "size": o["file_size"]}
                   for o in r.json()["files"]]
            self.dircache[path] = out
        if detail:
            return out
        return [o["name"] for o in out]

    def mkdirs(self, path, exist_ok=True):
        if not exist_ok:
            raise NotImplementedError
        r = self.session.post(self.url + "/2.0/dbfs/mkdirs", json={"path": path})
        r.raise_for_status()

    def makedir(self, path, create_parents=True, **kwargs):
        if not create_parents:
            raise NotImplementedError
        self.mkdirs(path)

    def rm(self, path, recursive=False):
        r = self.session.post(self.url + "/2.0/dbfs/delete",
                              json={"path": path, "recursive": recursive})
        r.raise_for_status()

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        if recursive:
            raise NotImplementedError
        r = self.session.post(self.url + "/2.0/dbfs/move",
                              json={"source_path": path1, "destination_path": path2})
        r.raise_for_status()


class DBFile(AbstractBufferedFile):

    DEFAULT_BLOCK_SIZE = 1 * 2 ** 20  # only allowed block size

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs
    ):
        super().__init__(fs,
            path,
            mode=mode,
            block_size=self.DEFAULT_BLOCK_SIZE,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options or {},
            **kwargs
        )

    # these methods could be standalone functions, if you want to go async
    def _initiate_upload(self):
        r = self.fs.session.post(self.fs.url + "2.0/dbfs/create",
                                 json={"path": self.path, "overwrite": True})
        r.raise_for_status
        self.handle = r.json["handle"]

    def _upload_chunk(self, final=False):
        # for data in buffer, in 1MB blocks
        r = self.fs.session.post(self.fs.url + "2.0/dbfs/add-block",
                                 json={"handle": self.path, "data": base64.b64encode(data)})
        r.raise_for_status
        if final:
            r = self.fs.session.post(self.fs.url + "2.0/dbfs/close",
                                     json={"handle": self.path})
            r.raise_for_status

    def _fetch_range(self, start, end):
        r = self.fs.session.get(self.fs.url + "2.0/dbfs/read",
                                json={"path": self.path, "offset": start, "length": end - start})
        r.raise_for_status
        return base64.b64decode(r["data"])
