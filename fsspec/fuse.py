from __future__ import print_function
import os
import stat
from errno import ENOENT, EIO
from fuse import Operations, FuseOSError
import time
import pandas as pd
from fuse import FUSE


def str_to_time(s):
    t = pd.to_datetime(s)
    return t.to_datetime64().view('int64') / 1e9


class FUSEr(Operations):

    def __init__(self, fs, path):
        self.fs = fs
        self.cache = {}
        self.root = path
        self.counter = 0
    
    def getattr(self, path, fh=None):
        path = ''.join([self.root, path.lstrip('/')]).rstrip('/')
        info = self.fs.info(path)
        data = {'st_uid': 1000, 'st_gid': 1000}
        perm = 0o777

        if info['type'] != 'file':
            data['st_mode'] = (stat.S_IFDIR | perm)
            data['st_size'] = 0
            data['st_blksize'] = 0
        else:
            data['st_mode'] = (stat.S_IFREG | perm)
            data['st_size'] = info['size']
            data['st_blksize'] = 5 * 2**20
            data['st_nlink'] = 1
        data['st_atime'] = time.time()
        data['st_ctime'] = time.time()
        data['st_mtime'] = time.time()
        return data

    def readdir(self, path, fh):
        path = ''.join([self.root, path.lstrip('/')])
        files = self.fs.ls(path, False)
        files = [os.path.basename(f.rstrip('/')) for f in files]
        return ['.', '..'] + files
    
    def mkdir(self, path, mode):
        path = ''.join([self.root, path.lstrip('/')])
        self.fs.mkdir(path)
        return 0
    
    def rmdir(self, path):
        path = ''.join([self.root, path.lstrip('/')])
        self.fs.rmdir(path)
        return 0
    
    def read(self, path, size, offset, fh):
        f = self.cache[fh]
        f.seek(offset)
        out = f.read(size)
        return out
    
    def write(self, path, data, offset, fh):
        f = self.cache[fh]
        f.write(data)
        return len(data)
    
    def create(self, path, flags, fi=None):
        if fi is not None:
            print(fi)
        fn = ''.join([self.root, path.lstrip('/')])
        f = self.fs.open(fn, 'wb')
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1
    
    def open(self, path, flags):
        fn = ''.join([self.root, path.lstrip('/')])
        if flags % 2 == 0:
            # read
            mode = 'rb'
        else:
            # write/create
            mode = 'wb'
        self.cache[self.counter] = self.fs.open(fn, mode)
        self.counter += 1
        return self.counter - 1

    def truncate(self, path, length, fh=None):
        fn = ''.join([self.root, path.lstrip('/')])
        if length != 0:
            raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.fs.touch(fn)
    
    def unlink(self, path):
        fn = ''.join([self.root, path.lstrip('/')])
        try:
            self.fs.rm(fn, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)
    
    def release(self, path, fh):
        try:
            if fh in self.cache:
                f = self.cache[fh]
                f.close()
                self.cache.pop(fh)
        except Exception as e:
            print(e)
        return 0
    
    def chmod(self, path, mode):
        raise NotImplementedError


def run(fs, path, mount_point, foreground=True, threads=False):
    """ Mount stuff in local directory """
    FUSE(FUSEr(fs, path),
         mount_point, nothreads=not threads, foreground=foreground)
