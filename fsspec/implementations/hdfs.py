import os
import re
import secrets
import shutil
import subprocess
import weakref
from contextlib import suppress
from functools import partial

from pyarrow.hdfs import HadoopFileSystem

from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options


def inherits(*methods):
    def client_getter(name, self):
        return getattr(self.client, name)

    def wrapper(cls):
        for method in methods:
            wrapped_method = partial(client_getter, method)
            setattr(cls, method, property(wrapped_method))
        return cls

    return wrapper


CHECKSUM_REGEX = re.compile(r".*\t.*\t(?P<checksum>.*)")


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
    "port",
    "get_capacity",
    "get_space_used",
    "df",
    "chmod",
    "chown",
    "disk_usage",
    "download",
    "upload",
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
        listing = [
            self._adjust_entry(entry) for entry in self.client.ls(path, detail=True)
        ]

        if detail:
            return listing
        else:
            return [entry["name"] for entry in listing]

    def info(self, path):
        return self._adjust_entry(self.client.info(path))

    def _adjust_entry(self, original_entry):
        entry = original_entry.copy()
        if "type" not in entry:
            entry["type"] = entry["kind"]
        if "name" not in entry:
            entry["name"] = self._strip_protocol(entry["path"])
        return entry

    def cp_file(self, lpath, rpath, **kwargs):
        with self.open(lpath) as lstream:
            tmp_fname = "/".join([self._parent(rpath), f".tmp.{secrets.token_hex(16)}"])
            try:
                with self.open(tmp_fname, "wb") as rstream:
                    shutil.copyfileobj(lstream, rstream)
                self.client.mv(tmp_fname, rpath)
            except BaseException:  # noqa
                with suppress(FileNotFoundError):
                    self.client.rm(tmp_fname)
                raise

    def rm_file(self, path):
        return self.client.rm(path)

    def makedirs(self, path, exist_ok=False):
        if not exist_ok and self.exists(path):
            raise FileExistsError(path)

        return self.client.mkdir(path, create_parents=True)

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

    def checksum(self, path, env=None, **kwargs):
        # PyArrow doesn't natively support retrieving the
        # checksum, so we have to use hadoop fs

        result = self._run_command(f"checksum {self._as_url(path)}", env=env)
        if result is None:
            return None

        match = CHECKSUM_REGEX.match(result)
        if match is None:
            return None

        return match.group("checksum")

    def _run_command(self, cmd, env=None):
        cmd = "hadoop fs -" + cmd
        if self.user:
            cmd = f"HADOOP_USER_NAME={self.user} " + cmd

        # NOTE: close_fds doesn't work with redirected stdin/stdout/stderr.
        # See https://github.com/iterative/dvc/issues/1197.
        close_fds = os.name != "nt"

        executable = os.getenv("SHELL") if os.name != "nt" else None
        p = subprocess.Popen(
            cmd,
            shell=True,
            close_fds=close_fds,
            executable=executable,
            env=env or os.environ,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = p.communicate()

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd, out, err)
        else:
            return out.decode("utf-8")

    def _as_url(self, path):
        netloc = f"{self.host}:{self.port}"
        if self.user:
            netloc = f"{self.user}@{netloc}"
        return f"hdfs://{netloc}/{path.lstrip('/')}"


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
        self.blocksize = self.block_size = block_size
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
