import dask
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
from fsspec import filesystem
from fsspec.core import strip_protocol


def make_instance(cls, args, kwargs):
    inst = cls(*args, **kwargs)
    inst._determine_worker()
    return inst


def _have_distributed_client():
    try:
        from distributed.client import _get_global_client
    except ImportError:
        return False
    return _get_global_client() is not None


def _is_distributed_worker():
    try:
        from distributed._run_local import get_worker
    except ImportError:
        return False
    try:
        get_worker()
        return True
    except ValueError:
        return False


class DaskWorkerFileSystem(AbstractFileSystem):
    """View files accessible to a worker as any other remote file-system

    When instances are run on the worker, uses the real filesystem. When
    run on the client, they call the worker to provide information or data.

    **Warning** this implementation is experimental, and read-only for now.
    """

    cachable = False
    protocol = "dask"

    def __init__(self, target_protocol="file", **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs
        self.protocol = target_protocol
        self._run_local = None
        self.fs = self._determine_worker()

    def _determine_worker(self):
        self._run_local = _is_distributed_worker() or not _have_distributed_client()
        if self._run_local:
            return filesystem(self.protocol, **(self.kwargs or {}))
        else:
            return dask.delayed(self)

    @classmethod
    def _get_kwargs_from_urls(cls, url):
        if isinstance(url, (list, tuple)):
            url = url[0]
        url = cls._strip_protocol(url)
        if "://" in url:
            return {"target_protocol": url.split("://", 1)[0]}
        else:
            return {}

    def __reduce__(self):
        return make_instance, (type(self), self.storage_args, self.storage_options)

    def mkdir(self, *args, **kwargs):
        if self._run_local:
            self.fs.mkdir(*args, **kwargs)
        else:
            self.fs.mkdir(*args, **kwargs).compute()

    def rm(self, *args, **kwargs):
        if self._run_local:
            self.fs.rm(*args, **kwargs)
        else:
            self.fs.rm(*args, **kwargs).compute()

    def copy(self, *args, **kwargs):
        if self._run_local:
            self.fs.copy(*args, **kwargs)
        else:
            self.fs.copy(*args, **kwargs).compute()

    def mv(self, *args, **kwargs):
        if self._run_local:
            self.fs.mv(*args, **kwargs)
        else:
            self.fs.mv(*args, **kwargs).compute()

    def ls(self, *args, **kwargs):
        if self._run_local:
            return self.fs.ls(*args, **kwargs)
        else:
            return self.fs.ls(*args, **kwargs).compute()

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        path = strip_protocol(path)
        if self._run_local:
            return self.fs._open(
                path,
                mode=mode,
                block_size=block_size,
                autocommit=autocommit,
                cache_options=cache_options,
                **kwargs
            )
        else:
            return DaskFile(
                self,
                path,
                mode,
                block_size=block_size,
                autocommit=autocommit,
                cache_options=cache_options,
                **kwargs
            )

    def fetch_range(self, path, mode, start, end):
        if self._run_local:
            with self._open(path, mode) as f:
                f.seek(start)
                return f.read(end - start)
        else:
            return self.fs.fetch_range(path, mode, start, end).compute()


class DaskFile(AbstractBufferedFile):
    def _upload_chunk(self, final=False):
        pass

    def _initiate_upload(self):
        """ Create remote file/upload """
        pass

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        return self.fs.fetch_range(self.path, self.mode, start, end)
