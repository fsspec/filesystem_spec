import os
import shutil
import tempfile
from fsspec import AbstractFileSystem


class LocalFileSystem(AbstractFileSystem):
    def mkdir(self, path, **kwargs):
        os.mkdir(path, **kwargs)

    def makedirs(self, path, exist_ok=False):
        os.makedirs(path, exist_ok=exist_ok)

    def rmdir(self, path):
        os.rmdir(path)

    def ls(self, path, detail=False):
        paths = [os.path.abspath(os.path.join(path, f))
                 for f in os.listdir(path)]
        if detail:
            return [self.info(f) for f in paths]
        else:
            return paths

    def info(self, path):
        out = os.stat(path, follow_symlinks=False)
        dest = False
        if os.path.isfile(path):
            t = 'file'
        elif os.path.isdir(path):
            t = 'directory'
        elif os.path.islink(path):
            t = 'link'
            dest = os.readlink(path)
        else:
            t = 'other'
        result = {
            'name': path,
            'size': out.st_size,
            'type': t,
            'created': out.st_ctime
        }
        for field in ['mode', 'uid', 'gid', 'mtime']:
            result[field] = getattr(out, 'st_' + field)
        if dest:
            result['destination'] = dest
        return result

    def copy(self, path1, path2, **kwargs):
        """ Copy within two locations in the filesystem"""
        shutil.copyfile(path1, path2)

    get = copy
    put = copy

    def mv(self, path1, path2, **kwargs):
        """ Move file from one location to another """
        os.rename(path1, path2)

    def rm(self, path, recursive=False):
        if recursive:
            shutil.rmtree(path)
        else:
            os.remove(path)

    def _open(self, path, mode='rb', block_size=None, **kwargs):
        return LocalFileOpener(path, mode, **kwargs)

    def touch(self, path, **kwargs):
        """ Create empty file, or update timestamp """
        if self.exists(path):
            os.utime(path, None)
        else:
            open(path, 'a').close()


class LocalFileOpener(object):
    def __init__(self, path, mode, autocommit=True):
        self.path = path
        self.autocommit = autocommit
        if autocommit or 'w' not in mode:
            self.f = open(path, mode=mode)
        else:
            # TODO: check if path is writable?
            i, name = tempfile.mkstemp()
            self.temp = name
            self.f = open(name, mode=mode)

    def commit(self):
        if self.autocommit:
            raise RuntimeError('Can only commit if not already set to '
                               'autocommit')
        os.rename(self.temp, self.path)

    def discard(self):
        if self.autocommit:
            raise RuntimeError('Cannot discard if set to autocommit')
        os.remove(self.temp)

    def __getattr__(self, item):
        return getattr(self.f, item)

    def __enter__(self):
        self._incontext = True
        return self.f

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()
        self._incontext = False
