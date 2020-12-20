import base64
import os
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
import requests


class DatabricksFileSystem(AbstractFileSystem):
    def __init__(self, instance, token, **kwargs):
        self.instance = instance
        self.token = token

        self.session = requests.Session()
        self.session.headers.update(self.header)

        super().__init__(**kwargs)

    @property
    def header(self):
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def url(self):
        return f"https://{self.instance}/api"

    def ls(self, path, detail=True):
        out = self._ls_from_cache(path)
        if not out:
            try:
                r = self.get_from_api("list", json={"path": path})
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 404:
                    raise FileNotFoundError(path)
                raise e
            files = r["files"]
            out = [
                {
                    "name": o["path"],
                    "type": "directory" if o["is_dir"] else "file",
                    "size": o["file_size"],
                }
                for o in files
            ]
            self.dircache[path] = out

        if detail:
            return out
        return [o["name"] for o in out]

    def mkdirs(self, path, exist_ok=True):
        if not exist_ok:
            raise NotImplementedError
        self.post_to_api("mkdirs", json={"path": path})

    def makedirs(self, path, create_parents=True, **kwargs):
        if not create_parents:
            raise NotImplementedError
        self.mkdirs(path, **kwargs)

    def rm(self, path, recursive=False):
        self.post_to_api("delete", json={"path": path, "recursive": recursive})

    def mv(self, path1, path2, recursive=False, maxdepth=None):
        if recursive:
            raise NotImplementedError
        if maxdepth:
            raise NotImplementedError

        self.post_to_api(
            "move",
            json={"source_path": path1, "destination_path": path2},
        )

    def create_handle(self, path, overwrite=True):
        r = self.post_to_api("create", json={"path": path, "overwrite": overwrite})
        return r["handle"]

    def close_handle(self, handle):
        self.post_to_api("close", json={"handle": handle})

    def add_data(self, handle, data):
        data = base64.b64encode(data).decode()
        self.post_to_api(
            "add-block",
            json={"handle": handle, "data": data},
        )

    def get_data(self, path, start, end):
        r = self.get_from_api(
            "read",
            json={"path": path, "offset": start, "length": end - start},
        )
        return base64.b64decode(r["data"])

    def post_to_api(self, endpoint, json):
        r = self.session.post(os.path.join(self.url, "2.0/dbfs", endpoint), json=json)
        r.raise_for_status()

        return r.json()

    def get_from_api(self, endpoint, json):
        r = self.session.get(os.path.join(self.url, "2.0/dbfs", endpoint), json=json)
        r.raise_for_status()

        return r.json()

    def _open(self, path, mode="rb", block_size="default", **kwargs):
        return DBFile(self, path, mode=mode, block_size=block_size, **kwargs)


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
        if block_size is None or block_size == "default":
            block_size = self.DEFAULT_BLOCK_SIZE

        assert (
            block_size == self.DEFAULT_BLOCK_SIZE
        ), f"Only the default block size is allowed, not {block_size}"

        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options or {},
            **kwargs
        )

    # these methods could be standalone functions, if you want to go async
    def _initiate_upload(self):
        self.handle = self.fs.create_handle(self.path)
        print("Handle:", self.handle)

    def _upload_chunk(self, final=False):
        self.buffer.seek(0)
        # TODO: for data in buffer, in 1MB blocks
        print("Writing data")
        self.fs.add_data(handle=self.handle, data=self.buffer.getvalue())
        if final:
            print("Closing handle", self.handle)
            self.fs.close_handle(handle=self.handle)

    def _fetch_range(self, start, end):
        return self.fs.get_data(path=self.path, start=start, end=end)
