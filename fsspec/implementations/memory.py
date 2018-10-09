from __future__ import print_function, division, absolute_import

from io import BytesIO
from fsspec.spec import AbstractFileSystem
import logging
logger = logging.Logger('fsspec.memoryfs')


class MemoryFileSystem(AbstractFileSystem):
    """A filesystem based on a dict of BytesIO objects"""
    store = {}  # global

    def ls(self, path, detail=False):
        if path in self.store:
            out = [{'name': path,
                    'size': self.store[path].getbuffer().nbytes,
                    'type': 'file'}]
        else:
            out = []
            path = path.rstrip('/')
            for p in self.store:
                if p.rsplit('/', 1)[0] == path:
                    out.append({'name': p,
                                'size': self.store[p].getbuffer().nbytes,
                                'type': 'file'})
                elif p.rsplit('/', 2)[0] == path:
                    # implicit directory
                    ppath = p.rsplit('/', 1)[0]
                    out.append({'name': ppath,
                                'size': 0,
                                'type': 'directory'})
        if detail:
            return out
        return [f['name'] for f in out]

    def exists(self, path):
        return path in self.store

    def _open(self, path, mode='rb', **kwargs):
        """Make a file-like object

        Parameters
        ----------
        path: str
            identifier
        mode: str
            normally "rb", "wb" or "ab"
        """
        if mode in ['rb', 'ab', 'rb+']:
            if path in self.store:
                f = self.store[path]
                if mode == 'rb':
                    f.seek(0)
                else:
                    f.seek(0, 2)
                return f
            else:
                raise FileNotFoundError(path)
        if mode == 'wb':
            if not path.startswith('/'):
                logger.warning('New file with path that does not start with'
                               ' "/", will not show up in ls.')
            self.store[path] = MemoryFile()
            return self.store[path]

    def copy(self, path1, path2, **kwargs):
        self.store[path1] = MemoryFile(self.store[path2].getbuffer())

    def cat(self, path):
        return self.store[path].getvalue()

    def _rm(self, path):
        del self.store[path]

    def ukey(self, path):
        return hash(self.store[path])  # internal ID of instance

    def size(self, path):
        """Size in bytes of the file at path"""
        if path not in self.store:
            raise FileNotFoundError(path)
        return self.store[path].getbuffer().nbytes


class MemoryFile(BytesIO):
    """A BytesIO which can't close and works as a context manager"""

    def __enter__(self):
        return self

    def close(self):
        pass
