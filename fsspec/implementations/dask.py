import inspect
from distributed.worker import get_worker
from distributed.client import _get_global_client
import dask
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
from fsspec import filesystem

methods_to_forward = ['mkdir', 'mkdirs', 'rmdir', 'ls', 'copy', 'mv', 'rm']
_files_cache = {}


class DaskWorkerFileSystem(AbstractFileSystem):
    """View files accessible to a worker as any other remote file-system

    When instances are run on the worker, uses the real filesystem. When
    run on the client, they call the worker to provide information or data.

    **Warning** this implementation is experimental, and read-only for now.
    """

    def __init__(self, remote_protocol, storage_options=None, **kwargs):
        super().__init__(**kwargs)
        self.protocol = remote_protocol
        self.storage_options = storage_options or {}
        self.worker = None
        self.client = None
        self.fs = None
        self._determine_worker()

    def _determine_worker(self):
        try:
            get_worker()
            self.worker = True
            self.fs = filesystem(self.protocol, **self.storage_options)
        except ValueError:
            self.worker = False
            self.client = _get_global_client()
            self.rfs = dask.delayed(self)

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._determine_worker()

    def __getstate__(self):
        dic = self.__dict__.copy()
        for method in methods_to_forward + ['client', 'fs']:
            dic.pop(method, None)
        return dic

    def mkdir(self, *args, **kwargs):
        if self.worker:
            self.fs.mkdir(*args, **kwargs)
        else:
            self.rfs.mkdir(*args, **kwargs).compute()

    def rm(self, *args, **kwargs):
        if self.worker:
            self.fs.rm(*args, **kwargs)
        else:
            self.rfs.rm(*args, **kwargs).compute()

    def copy(self, *args, **kwargs):
        if self.worker:
            self.fs.copy(*args, **kwargs)
        else:
            self.rfs.copy(*args, **kwargs).compute()

    def mv(self, *args, **kwargs):
        if self.worker:
            self.fs.mv(*args, **kwargs)
        else:
            self.rfs.mv(*args, **kwargs).compute()

    def ls(self, *args, **kwargs):
        if self.worker:
            return self.fs.ls(*args, **kwargs)
        else:
            return self.rfs.ls(*args, **kwargs).compute()

    def _open(self, path, mode='rb', **kwargs):
        if self.worker:
            return self.fs._open(path, mode=mode)
        else:
            return DaskFile(self, path, mode, **kwargs)

    def fetch_range(self, path, mode, start, end):
        if self.worker:
            with self._open(path, mode) as f:
                f.seek(start)
                return f.read(end-start)
        else:
            return self.rfs.fetch_range(path, mode, start, end).compute()


class DaskFile(AbstractBufferedFile):

    def __init__(self, fs, path, mode='rb', block_size='default',
                 autocommit=True, cache_type='bytes', **kwargs):
        super().__init__(fs, path, mode=mode, block_size=block_size,
                         autocommit=autocommit, cache_type=cache_type,
                         **kwargs)

    def _upload_chunk(self, final=False):
        pass

    def _initiate_upload(self):
        """ Create remote file/upload """
        pass

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        return self.fs.fetch_range(self.path, self.mode, start, end)
