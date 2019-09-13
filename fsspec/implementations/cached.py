import time
import pickle
import logging
import os
import hashlib
import tempfile
import inspect
from fsspec import AbstractFileSystem, filesystem
from fsspec.core import MMapCache
logger = logging.getLogger('fsspec')


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
      derived from fsspec.spec.AbstractBufferedFile ; LocalFileSystem is also
      allowed, for testing
    """

    protocol = ('blockcache', 'cached')

    def __init__(self, target_protocol=None, cache_storage='TMP',
                 cache_check=10, check_files=False,
                 expiry_time=604800, target_options=None):
        """

        Parameters
        ----------
        target_protocol : str
            Target fielsystem protocol
        cache_storage : str or list(str)
            Location to store files. If "TMP", this is a temporary directory,
            and will be cleaned up by the OS when this process ends (or later).
            If a list, each location will be tried in the order given, but
            only the last will be considered writable.
        cache_check : int
            Number of seconds between reload of cache metadata
        check_files : bool
            Whether to explicitly see if the UID of the remote file matches
            the stored one before using. Warning: some file systems such as
            HTTP cannot reliably give a unique hash of the contents of some
            path, so be sure to set this option to False.
        expiry_time : int
            The time in seconds after which a local copy is considered useless.
            Set to falsy to prevent expiry. The default is equivalent to one
            week.
        target_options : dict or Noen
            Passed to the instantiation of the FS, if fs is None.
        """
        if cache_storage == "TMP":
            storage = [tempfile.mkdtemp()]
        else:
            if isinstance(cache_storage, str):
                storage = [cache_storage]
            else:
                storage = cache_storage
        os.makedirs(storage[-1], exist_ok=True)
        self.protocol = target_protocol
        self.storage = storage
        self.kwargs = target_options or {}
        self.cache_check = cache_check
        self.check_files = check_files
        self.expiry = expiry_time
        self.load_cache()
        self.fs = filesystem(target_protocol, **self.kwargs)
        super().__init__(**self.kwargs)

    def __reduce_ex__(self, *_):
        return self.__class__, (
            self.protocol, self.storage, self.cache_check, self.check_files,
            self.expiry, self.kwargs or None)

    def load_cache(self):
        """Read set of stored blocks from file"""
        cached_files = []
        for storage in self.storage:
            fn = os.path.join(storage, 'cache')
            if os.path.exists(fn):
                with open(fn, 'rb') as f:
                    # TODO: consolidate blocks here
                    cached_files.append(pickle.load(f))
            else:
                os.makedirs(storage, exist_ok=True)
                cached_files.append({})
        self.cached_files = cached_files or [{}]
        self.last_cache = time.time()

    def save_cache(self):
        """Save set of stored blocks from file"""
        fn = os.path.join(self.storage[-1], 'cache')
        # TODO: a file lock could be used to ensure file does not change
        #  between re-read and write; but occasional duplicated reads ok.
        cache = self.cached_files[-1]
        if os.path.exists(fn):
            with open(fn, 'rb') as f:
                cached_files = pickle.load(f)
            for k, c in cached_files.items():
                if c['blocks'] is not True:
                    if cache[k]['blocks'] is True:
                        c['blocks'] = True
                    else:
                        c['blocks'] = c['blocks'].union(cache[k]['blocks'])
        else:
            cached_files = cache
        cache = {k: v.copy() for k, v in cached_files.items()}
        for c in cache.values():
            if isinstance(c['blocks'], set):
                c['blocks'] = list(c['blocks'])
        with open(fn + '.temp', 'wb') as f:
            pickle.dump(cache, f)
        if os.path.exists(fn):
            os.remove(fn)
        os.rename(fn + '.temp', fn)

    def _check_cache(self):
        """Reload caches if time elapsed or any disappeared"""
        if not self.cache_check:
            # explicitly told not to bother checking
            return
        timecond = time.time() - self.last_cache > self.cache_check
        existcond = all(os.path.exists(storage) for storage in self.storage)
        if timecond or not existcond:
            self.load_cache()

    def _check_file(self, path):
        """Is path in cache and still valid"""
        self._check_cache()
        for storage, cache in zip(self.storage, self.cached_files):
            if path not in cache:
                continue
            detail = cache[path].copy()
            if self.check_files:
                if detail['uid'] != self.fs.ukey(path):
                    continue
            if self.expiry:
                if detail['time'] - time.time() > self.expiry:
                    continue
            fn = os.path.join(storage, detail['fn'])
            if os.path.exists(fn):
                return detail, fn
        return False, None

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
        detail, fn = self._check_file(path)
        if detail:
            # file is in cache
            hash, blocks = detail['fn'], detail['blocks']
            if blocks is True:
                # stored file is complete
                logger.debug("Opening local copy of %s" % path)
                return open(fn, 'rb')
            # TODO: action where partial file exists in read-only cache
            logger.debug("Opening partially cached copy of %s" % path)
        else:
            hash = hashlib.sha256(path.encode()).hexdigest()
            fn = os.path.join(self.storage[-1], hash)
            blocks = set()
            detail = {'fn': hash, 'blocks': blocks,
                      'time': time.time(),
                      'uid': self.fs.ukey(path)}
            self.cached_files[-1][path] = detail
            logger.debug("Creating local sparse file for %s" % path)
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
        if f.path.startswith(self.protocol):
            path = f.path
        else:
            path = self.protocol + "://" + f.path
        c = self.cached_files[-1][path]
        if (c['blocks'] is not True
                and len(['blocks']) * f.blocksize >= f.size):
            c['blocks'] = True
        self.save_cache()
        close()

    def __getattribute__(self, item):
        if item in ['load_cache', '_open', 'save_cache', 'close_and_update',
                    '__init__', '__getattribute__', '__reduce_ex__', 'open',
                    'cat', 'get', 'read_block', 'tail', 'head',
                    '_check_file', '_check_cache']:
            # all the methods defined in this class. Note `open` here, since
            # it calls `_open`, but is actually in superclass
            return lambda *args, **kw: getattr(type(self), item)(
                self, *args, **kw
            )
        if item == '__class__':
            return type(self)
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
    """Caches whole remote files on first access

    This class is intended as a layer over any other file system, and
    will make a local copy of each file accessed, so that all subsequent
    reads are local. This is similar to ``CachingFileSystem``, but without
    the block-wise functionality and so can work even when sparse files
    are not allowed. See its docstring for definition of the init
    arguments.

    The class still needs access to the remote store for listing files,
    and may refresh cached files.
    """
    protocol = 'filecache'

    def _open(self, path, mode='rb', **kwargs):
        path = self._strip_protocol(path)
        if not path.startswith(self.protocol):
            path = self.protocol + "://" + path
        if mode != 'rb':
            return self.fs._open(path, mode=mode, **kwargs)
        detail, fn = self._check_file(path)
        if detail:
            hash, blocks = detail['fn'], detail['blocks']
            if blocks is True:
                logger.debug("Opening local copy of %s" % path)
                return open(fn, 'rb')
            else:
                raise ValueError("Attempt to open partially cached file %s"
                                 "as a wholly cached file" % path)
        else:
            hash = hashlib.sha256(path.encode()).hexdigest()
            fn = os.path.join(self.storage[-1], hash)
            blocks = True
            detail = {'fn': hash, 'blocks': blocks,
                      'time': time.time(),
                      'uid': self.fs.ukey(path)}
            self.cached_files[-1][path] = detail
            logger.debug("Copying %s to local cache" % path)
        kwargs['cache_type'] = 'none'
        kwargs['mode'] = mode

        # call target filesystems open
        f = self.fs._open(path, **kwargs)
        with open(fn, 'wb') as f2:
            if f.blocksize and f.size:
                # opportunity to parallelise here
                data = True
                while data:
                    data = f.read(f.blocksize)
                    f2.write(data)
            else:
                # this only applies to HTTP, should instead use streaming
                f2.write(f.read())
        self.save_cache()
        return self._open(path, mode)
