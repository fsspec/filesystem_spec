import pickle
import os
import hashlib
import tempfile
import inspect
from fsspec import AbstractFileSystem, filesystem
from fsspec.core import MMapCache


class CachingFileSystem(AbstractFileSystem):
    """Locally caching filesystem, layer over any other FS

    This class implements chunk-wise local storage of remote files, for quick
    access after the initial download. The files are stored in a given
    directory with random hashes for the filenames. If no directory is given,
    a temporary one is used, which should be cleaned up by the OS after the
    process ends. The files themselves as sparse (as implemented in
    MMapCache), so only the data which is accessed takes up space.

    Restrictions:

    - the block-size must be the same for each access of a given file, unless
      all blocks of the file have already been read
    - caching can only be applied to file-systems which produce files
      derived from fsspec.spec.AbstractBufferedFile
    """

    protocol = 'cached'

    def __init__(self, target_protocol=None, cache_storage='TMP',
                 storage_options=None):
        """

        Parameters
        ----------
        target_protocol : str
            Target fielsystem protocol
        cache_storage : str
            Location to store files. If "TMP", this is a temporary directory,
            and will be cleaned up by the OS when this process ends (or later)
        storage_options
            Passed to the instantiation of the FS, if fs is None.
        """
        if cache_storage == "TMP":
            storage = tempfile.mkdtemp()
        else:
            storage = cache_storage
        os.makedirs(storage, exist_ok=True)
        self.protocol = target_protocol
        self.storage = storage
        self.kwargs = storage_options or {}
        self.load_cache()
        self.fs = filesystem(target_protocol, **self.kwargs)
        super().__init__(**self.kwargs)

    def load_cache(self):
        """Read set of stored blocks from file"""
        fn = os.path.join(self.storage, 'cache')
        if os.path.exists(fn):
            with open(fn, 'rb') as f:
                self.cached_files = pickle.load(f)
        else:
            self.cached_files = {}

    def save_cache(self):
        """Save set of stored blocks from file"""
        fn = os.path.join(self.storage, 'cache')
        # TODO: a file lock could be used to ensure file does not change
        #  between re-read and write; but occasional duplicated reads ok.
        if os.path.exists(fn):
            with open(fn, 'rb') as f:
                cached_files = pickle.load(f)
            for k, c in cached_files.items():
                if c['blocks'] is not True:
                    if self.cached_files[k]['blocks'] is True:
                        c['blocks'] = True
                    else:
                        c['blocks'] = c['blocks'].union(
                            self.cached_files[k]['blocks'])
        else:
            cached_files = self.cached_files
        cache = {k: v.copy() for k, v in cached_files.items()}
        for c in cache.values():
            if isinstance(c['blocks'], set):
                c['blocks'] = list(c['blocks'])
        with open(fn + '.temp', 'wb') as f:
            pickle.dump(self.cached_files, f)
        if os.path.exists(fn):
            os.remove(fn)
        os.rename(fn + '.temp', fn)

    def _open(self, path, mode='rb', **kwargs):
        """Wrap the target _open

        If the whole file exists in the cache, just open it locally and
        return that.

        Otherwise, open the file on the target FS, and make it have a mmap
        cache pointing to the location which we determine, in our cache.
        The ``blocks`` instance is shared, so as the mmap cache instance
        updates, so does the entry in our ``cached_files`` attribute.
        We monkey-patch this file, so that when it closes, we call
        ``close_and_update`` to save the state of the blocks.
        """
        path = self._strip_protocol(path)
        if not path.startswith(self.protocol):
            path = self.protocol + "://" + path
        if mode != 'rb':
            return self.fs._open(path, mode=mode, **kwargs)
        if path in self.cached_files:
            detail = self.cached_files[path]
            hash, blocks = detail['fn'], detail['blocks']
            fn = os.path.join(self.storage, hash)
            if blocks is True:
                return open(fn, 'rb')
        else:
            hash = hashlib.sha256(path.encode()).hexdigest()
            fn = os.path.join(self.storage, hash)
            blocks = set()
            detail = {'fn': hash, 'blocks': blocks}
            self.cached_files[path] = detail
        kwargs['cache_type'] = 'none'
        kwargs['mode'] = mode

        # call target filesystems open
        f = self.fs._open(path, **kwargs)
        if 'blocksize' in detail:
            if detail['blocksize'] != f.blocksize:
                raise ValueError('Cached file must be reopened with same block'
                                 'size as original (old: %i, new %i)'
                                 '' % (detail['blocksize'], f.blocksize))
        else:
            detail['blocksize'] = f.blocksize
        f.cache = MMapCache(f.blocksize, f._fetch_range, f.size,
                            fn, blocks)
        close = f.close
        f.close = lambda: self.close_and_update(f, close)
        return f

    def close_and_update(self, f, close):
        """Called when a file is closing, so store the set of blocks"""
        c = self.cached_files[f.path]
        if (c['blocks'] is not True
                and len(['blocks']) * f.blocksize >= f.size):
            c['blocks'] = True
        self.save_cache()
        close()

    def __reduce_ex__(self, *_):
        return CachingFileSystem, (self.protocol, self.storage,
                                   self.kwargs or None)

    def __getattribute__(self, item):
        if item in ['load_cache', '_open', 'save_cache', 'close_and_update',
                    '__init__', '__getattribute__', '__reduce_ex__', 'open',
                    'cat', 'get', 'read_block', 'tail', 'head']:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(CachingFileSystem, item)(
                self, *args, **kw
            )
        if item == '__class__':
            return CachingFileSystem
        d = object.__getattribute__(self, '__dict__')
        fs = d.get('fs', None)  # fs is not immediately defined
        if item in d:
            return d[item]
        elif fs is not None:
            if item in fs.__dict__:
                # attribute of instance
                return fs.__dict__[item]
            # attributed belonging to the target filesystem
            cls = type(fs)
            m = getattr(cls, item)
            if (inspect.isfunction(m) and (not hasattr(m, '__self__')
                                           or m.__self__ is None)):
                # instance method
                return m.__get__(fs, cls)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)


class WholeFileCacheFileSystem(CachingFileSystem):
    protocol = 'wfcached'

    def _open(self, path, mode='rb', **kwargs):
        path = self._strip_protocol(path)
        if not path.startswith(self.protocol):
            path = self.protocol + "://" + path
        if mode != 'rb':
            return self.fs._open(path, mode=mode, **kwargs)
        if path in self.cached_files:
            detail = self.cached_files[path]
            hash, blocks = detail['fn'], detail['blocks']
            fn = os.path.join(self.storage, hash)
            if blocks is True:
                return open(fn, 'rb')
        else:
            hash = hashlib.sha256(path.encode()).hexdigest()
            fn = os.path.join(self.storage, hash)
            blocks = True
            detail = {'fn': hash, 'blocks': blocks}
            self.cached_files[path] = detail
        kwargs['cache_type'] = 'none'
        kwargs['mode'] = mode

        # call target filesystems open
        f = self.fs._open(path, **kwargs)
        with open(fn, 'wb') as f2:
            if f.blocksize and f.size:
                data = True
                while data:
                    data = f.read(f.blocksize)
                    f2.write(data)
            else:
                f2.write(f.read())
        return self._open(path, mode)

    def __reduce_ex__(self, *_):
        return WholeFileCacheFileSystem, (self.protocol, self.storage,
                                   self.kwargs or None)

    def __getattribute__(self, item):
        if item in ['load_cache', '_open', 'save_cache', 'close_and_update',
                    '__init__', '__getattribute__', '__reduce_ex__', 'open',
                    'cat', 'get', 'read_block', 'tail', 'head']:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(WholeFileCacheFileSystem, item)(
                self, *args, **kw
            )
        if item == '__class__':
            return WholeFileCacheFileSystem
        d = object.__getattribute__(self, '__dict__')
        fs = d.get('fs', None)  # fs is not immediately defined
        if item in d:
            return d[item]
        elif fs is not None:
            if item in fs.__dict__:
                # attribute of instance
                return fs.__dict__[item]
            # attributed belonging to the target filesystem
            cls = type(fs)
            m = getattr(cls, item)
            if (inspect.isfunction(m) and (not hasattr(m, '__self__')
                                           or m.__self__ is None)):
                # instance method
                return m.__get__(fs, cls)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)
