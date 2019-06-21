import ujson
import os
import hashlib
import tempfile
import inspect
from fsspec import AbstractFileSystem, filesystem
from fsspec.core import MMapCache


class CachingFileSystem(AbstractFileSystem):
    """Locally caching filesystem, layer over any other FS

    This class implements chunk-wise local storage of remote files, for quick
    access after the initial download
    """

    protocol = 'cached'

    def __init__(self, fs=None, protocol=None, cache_storage='TMP', **kwargs):
        """

        Parameters
        ----------
        fs : fsspec.AbstractFileSystem compatible instance
            If given, just use this instance, and ignore protocol and kwargs
        protocol : str
            Target fielsystem protocol
        cache_storage : str
            Location to store files. If "TMP", this is a temporary directory,
            and will be cleaned up by the OS when this process ends (or later)
        kwargs
            Passed to the instantiation of the FS, if fs is None.
        """
        if cache_storage == "TMP":
            storage = tempfile.mkdtemp()
        else:
            storage = cache_storage
        os.makedirs(storage, exist_ok=True)
        self.storage = storage
        self.load_cache()
        if fs is None:
            fs = filesystem(protocol, **kwargs)
        self.fs = fs
        super().__init__(**kwargs)

    def load_cache(self):
        """Read set of stored blocks from file"""
        fn = os.path.join(self.storage, 'cache.json')
        if os.path.exists(fn):
            with open(fn) as f:
                self.cached_files = ujson.load(f)
                for c in self.cached_files.values():
                    if isinstance(c['blocks'], list):
                        c['blocks'] = set(c['blocks'])
        else:
            self.cached_files = {}

    def save_cache(self):
        """Save set of stored blocks from file"""
        fn = os.path.join(self.storage, 'cache.json')
        cache = {k: v.copy() for k, v in self.cached_files.items()}
        for c in cache.values():
            if isinstance(c['blocks'], set):
                c['blocks'] = list(c['blocks'])
        with open(fn, 'w') as f:
            ujson.dump(self.cached_files, f)

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
        if mode != 'rb':
            return self.fs._open(path, mode=mode, **kwargs)
        if path in self.cached_files:
            detail = self.cached_files[path]
            hash, blocks = detail['fn'], detail['blocks']
            fn = os.path.join(self.storage, hash)
            if blocks is True:
                return open(fn, 'rb')
            else:
                blocks = set(blocks)
        else:
            hash = hashlib.sha256(path.encode()).hexdigest()
            fn = os.path.join(self.storage, hash)
            blocks = set()
            self.cached_files[path] = {'fn': hash, 'blocks': blocks}
        kwargs['cache_type'] = 'none'
        kwargs['mode'] = mode

        # call target filesystems open
        f = self.fs._open(path, **kwargs)
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
        return CachingFileSystem, (self.fs, None, self.storage)

    def __getattribute__(self, item):
        if item in ['load_cache', '_open', 'save_cache', 'close_and_update',
                    '__init__', '__getattribute__', '__reduce_ex__']:
            # all the methods defined in this class
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
                return lambda *args, **kw: m(self, *args, **kw)
            return m  # class method or attribute
        else:
            # attributes of the superclass, while target is being set up
            return super().__getattribute__(item)
