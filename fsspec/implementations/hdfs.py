import weakref
from functools import partialmethod

from pyarrow.hdfs import HadoopFileSystem

from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options


def inherits(*methods):
    def client_method(self, method, *args, **kwargs):
        return getattr(self.client, method)(*args, **kwargs)

    def wrapper(cls):
        for method in methods:
            wrapped_method = partialmethod(client_method, method)
            setattr(cls, method, wrapped_method)
        return cls

    return wrapper


@inherits(
    "chmod",
    "chown",
    "user",
    "df",
    "disk_usage",
    "download",
    "driver",
    "exists",
    "extra_conf",
    "get_capacity",
    "get_space_used",
    "host",
    "is_open",
    "kerb_ticket",
    "strip_protocol",
    "mkdir",
    "mv",
    "port",
    "get_capacity",
    "get_space_used",
    "df",
    "chmod",
    "chown",
    "disk_usage",
    "download",
    "upload",
    "_get_kwargs_from_urls",
    "read_parquet",
    "rm",
    "stat",
    "upload",
)
class PyArrowHDFS(AbstractFileSystem):
    """Adapted version of Arrow's HadoopFileSystem

    This is a very simple wrapper over the pyarrow.hdfs.HadoopFileSystem, which
    passes on all calls to the underlying class."""

    protocol = "hdfs"

    def __init__(
        self,
        host="default",
        port=0,
        user=None,
        kerb_ticket=None,
        driver="libhdfs",
        extra_conf=None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        host: str
            Hostname, IP or "default" to try to read from Hadoop config
        port: int
            Port to connect on, or default from Hadoop config if 0
        user: str or None
            If given, connect as this username
        kerb_ticket: str or None
            If given, use this ticket for authentication
        driver: 'libhdfs' or 'libhdfs3'
            Binary driver; libhdfs if the JNI library and default
        extra_conf: None or dict
            Passed on to HadoopFileSystem
        """
        super().__init__(**kwargs)

        self.client = HadoopFileSystem(
            host=host,
            port=port,
            user=user,
            kerb_ticket=kerb_ticket,
            driver=driver,
            extra_conf=extra_conf,
        )
        weakref.finalize(self, lambda: self.client.close())

        self.pars = (host, port, user, kerb_ticket, driver, extra_conf)

    @staticmethod
    def _get_kwargs_from_urls(path):
        ops = infer_storage_options(path)
        out = {}
        if ops.get("host", None):
            out["host"] = ops["host"]
        if ops.get("username", None):
            out["user"] = ops["username"]
        if ops.get("port", None):
            out["port"] = ops["port"]
        return out

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        return ops["path"]

    def __reduce_ex__(self, protocol):
        return PyArrowHDFS, self.pars

    def close(self):
        self.client.close()

    def ls(self, path, detail=True):
        out = self.client.ls(path, detail=True)

        listing = []
        for original_entry in out:
            entry = original_entry.copy()
            entry["type"] = entry["kind"]
            entry["name"] = self._strip_protocol(entry["name"])

        if detail:
            return listing
        else:
            return [entry["name"] for entry in listing]

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        path: str
            Location of file; should start with '/'
        mode: str
        block_size: int
            Hadoop block size, e.g., 2**26
        autocommit: True
            Transactions are not yet implemented for HDFS; errors if not True
        kwargs: dict or None
            Hadoop config parameters

        Returns
        -------
        HDFSFile file-like instance
        """

        return HDFSFile(
            self,
            path,
            mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )


class HDFSFile(object):
    """Wrapper around arrow's HdfsFile

    Allows seek beyond EOF and (eventually) commit/discard
    """

    def __init__(
        self,
        fs,
        path,
        mode,
        block_size,
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs,
    ):
        # TODO: Inherit from AbstractBufferedFile?
        if not autocommit:
            raise NotImplementedError(
                "HDFSFile cannot be opened with 'autocommit=False'."
            )

        self.fs = fs
        self.path = path
        self.mode = mode
        self.block_size = block_size
        self.fh = fs.client.open(path, mode, block_size, **kwargs)
        if self.fh.readable():
            self.seek_size = self.size()

    def seek(self, loc, whence=0):
        if whence == 0 and self.readable():
            loc = min(loc, self.seek_size)
        return self.fh.seek(loc, whence)

    def __getattr__(self, item):
        return getattr(self.fh, item)

    def __reduce_ex__(self, protocol):
        return HDFSFile, (self.fs, self.path, self.mode, self.block_size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
