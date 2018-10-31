from hashlib import md5
from .utils import read_block, tokenize

aliases = [
    ('makedir', 'mkdir'),
    ('listdir', 'ls'),
    ('cp', 'copy'),
    ('move', 'mv'),
    ('stat', 'info'),
    ('disk_usage', 'du'),
    ('rename', 'mv'),
    ('delete', 'rm'),
]


class AbstractFileSystem(object):
    """
    An abstract super-class for pythonic file-systems
    """
    _singleton = None
    _cache = None
    cachable = True  # this class can be cached, instances reused
    _cached = False
    blocksize = 2**22
    protocol = 'abstract'

    def __new__(cls, *args, **storage_options):
        """
        Will reuse existing instance if:
        - cls.cachable is True and
        - the token (a hash of args and kwargs by default) exists in the cache

        The instance will skip init if instance.cached = True.
        """
        if cls._singleton is None:
            # set up space for singleton
            cls._singleton = [None]
        if cls._cache is None and cls.cachable:
            # set up instance cache, if using
            cls._cache = {}

        # TODO: defer to a class-specific tokeniser?
        token = tokenize(cls, args, storage_options)
        if cls.cachable and token in cls._cache:
            # check for cached instance
            return cls._cache[token]
        self = object.__new__(cls)
        self.token = token
        if self.cachable:
            # store for caching - can hold memory
            cls._cache[token] = self
        return self

    def __init__(self, *args, **storage_options):
        """Create and configure file-system instance

        Instances may be cachable, so if similar enough arguments are seen
        a new instance is not required. The token attribute exists to allow
        implementations to cache instances if they wish.

        A reasonable default should be provided if there are no arguments.

        Subclasses should call this method.

        Magic kwargs that affect functionality here:
        add_docs: if True, will append docstrings from this spec to the
            specific implementation
        add_aliases: if True, will add method aliases
        """
        if self._cached:
            # reusing instance, don't change
            return
        self._cached = True
        self._intrans = False
        self._transaction = Transaction(self)
        self._singleton[0] = self
        if storage_options.get('add_docs', True):
            self._mangle_docstrings()
        if storage_options.get('add_aliases', True):
            for new, old in aliases:
                setattr(self, new, getattr(self, old))

    def _mangle_docstrings(self):
        """Add AbstractFileSystem docstrings to subclass methods

        Disable by including ``add_docs=False`` to init kwargs.
        """
        for method in dir(self.__class__):
            if method.startswith('_'):
                continue
            if self.__class__ is not AbstractFileSystem:
                m = getattr(self.__class__, method)
                n = getattr(AbstractFileSystem, method, None).__doc__
                if (not callable(m) or not n or n in
                        (m.__doc__ or "")):
                    # ignore if a) not a method, b) no superclass doc
                    # c) already includes docstring
                    continue
                try:
                    if m.__doc__:
                        m.__doc__ += ("\n Upstream docstring: \n" + getattr(
                                      AbstractFileSystem, method).__doc__)
                    else:
                        m.__doc__ = getattr(
                            AbstractFileSystem, method).__doc__
                except AttributeError:
                    pass

    def _strip_protocol(self, path):
        """ Turn path from fully-qualified to file-system-specific

        May require FS-specific handling, e.g., for relative paths or links.
        """
        if path.startswith(self.protocol + '://'):
            return path[len(self.protocol) + 3:]
        elif path.startswith(self.protocol + ':'):
            return path[len(self.protocol) + 1:]
        elif path.startswith(self.protocol):
            return path[len(self.protocol):]
        else:
            return path

    @staticmethod
    def _get_kwargs_from_urls(paths):
        """If kwargs can be encoded in the paths, extract them here

        This should happen before instantiation of the class; incoming paths
        then should be amended to strip the options in methods.

        Examples may look like an sftp path "sftp://user@host:/my/path", where
        the user and host should become kwargs and later get stripped.
        """
        # by default, nothing happens
        return {}

    @classmethod
    def current(cls):
        """ Return the most recently created FileSystem

        If no instance has been created, then create one with defaults
        """
        if not cls._singleton[0]:
            return cls()
        else:
            return cls._singleton[0]

    @property
    def transaction(self):
        """A context within which files are committed together upon exit

        Requires the file class to implement `.commit()` and `.discard()`
        for the normal and exception cases.
        """
        return self._transaction

    def start_transaction(self):
        """Begin write transaction for deferring files, non-context version"""
        self._intrans = True
        return self.transaction

    def end_transaction(self):
        """Finish write transaction, non-context version"""
        self.transaction.complete()

    def invalidate_cache(self, path=None):
        """
        Discard any cached directory information

        Parameters
        ----------
        path: string or None
            If None, clear all listings cached else listings at or under given
            path.
        """
        pass  # not necessary to implement, may have no cache

    def mkdir(self, path, **kwargs):
        """
        Create directory entry at path

        For systems that don't have true directories, may create an for
        this instance only and not touch the real filesystem

        Parameters
        ----------
        path: str
            location
        kwargs:
            may be permissions, etc.
        """
        pass  # not necessary to implement, may not have directories

    def makedirs(self, path, exist_ok=False):
        """Recursively make directories

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If True, will error if the target already exists
        """
        pass  # not necessary to implement, may not have directories

    def rmdir(self, path):
        """Remove a directory, if empty"""
        pass  # not necessary to implement, may not have directories

    def ls(self, path, detail=False):
        """List objects at path.

        This should include subdirectories and files at that location. The
        difference between a file and a directory must be clear when details
        are requested.

        The specific keys, or perhaps a FileInfo class, or similar, is TBD,
        but must be consistent across implementations.
        Must include:
        - full path to the entry
        - size of the entry, in bytes

        Additional information
        may be present, approriate to the file-system, e.g., generation,
        checksum, etc.

        Parameters
        ----------
        path: str
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        """
        raise NotImplementedError

    def walk(self, path, maxdepth=3):
        """ Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.
        """
        path = self._strip_protocol(path)
        full_dirs = []
        dirs = []
        files = []

        for info in self.ls(path, True):
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            name = info['name']
            if info['type'] == 'directory':
                full_dirs.append(name.rstrip('/'))
                dirs.append(name.rstrip('/').rsplit('/', 1)[-1])
            else:
                files.append(name.rsplit('/', 1)[-1])
        yield path, dirs, files

        for d in full_dirs:
            if maxdepth is None or maxdepth > 1:
                for res in self.walk(d, maxdepth=(maxdepth - 1)
                                     if maxdepth is not None else None):
                    yield res

    def find(self, path, maxdepth=3):
        """List all files below path.

        Like posix ``find`` command without conditions
        """
        out = []
        for path, _, files in self.walk(path, maxdepth):
            for name in files:
                out.append('/'.join([path.rstrip('/'), name]) if path else name)
        return out

    def du(self, path, total=True, maxdepth=4):
        """Space used by files within a path

        Parameters
        ----------
        total: bool
            whether to sum all the file sizes
        maxdepth: int or None
            maximum number of directory levels to descend, None for unlimited.

        Returns
        -------
        Dict of {fn: size} if total=False, or int otherwise, where numbers
        refer to bytes used.
        """
        sizes = {}
        for f in self.find(path, maxdepth=maxdepth):
            info = self.info(f)
            sizes[info['name']] = info['size']
        if total:
            return sum(sizes.values())
        else:
            return sizes

    def glob(self, path):
        """
        Find files by glob-matching.

        If the path ends with '/' and does not contain "*", it is essentially
        the same as ``ls(path)``, returning only files.

        We do not attempt to match for ``"**"`` notation, but we do support
        ``"?"`` and ``"[..]"``.

        Example reimplements code in ``glob.glob()``, taken from hdfs3.
        """
        import re
        import posixpath
        if "*" not in path:
            root = path
            depth = 1
            if path.endswith('/'):
                path += '*'
            elif self.exists(path):
                return [path]
            else:
                raise FileNotFoundError(path)
        elif '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind + 1]
            depth = path[ind + 1:].count('/') + 1
        else:
            root = ''
            depth = 1
        allpaths = []
        for dirname, dirs, fils in self.walk(root, maxdepth=depth):
            allpaths.extend(posixpath.join(dirname, f) for f in fils)
        pattern = re.compile("^" + path.replace('//', '/')
                             .rstrip('/')
                             .replace('*', '[^/]*')
                             .replace('?', '.') + "$")
        return [p for p in allpaths
                if pattern.match(p.replace('//', '/').rstrip('/'))]

    def exists(self, path):
        """Is there a file at the given path"""
        try:
            self.info(path)
            return True
        except:   # any exception allowed bar FileNotFoundError?
            return False

    def info(self, path):
        """Give details of entry at path

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should probably be overridden by a shortcut.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        out = self.ls(path, detail=True)
        out = [o for o in out if o['name'].rstrip('/') == path.rstrip('/')]
        if out:
            return out[0]
        if '/' in path:
            parent = path.rsplit('/', 1)[0]
        out = self.ls(parent, detail=True)
        out = [o for o in out if o['name'].rstrip('/') == path.rstrip('/')]
        if out:
            return out[0]
        raise FileNotFoundError(path)

    def size(self, path):
        """Size in bytes of file"""
        return self.info(path)['size']

    def isdir(self, path):
        """Is this entry directory-like?"""
        return self.info(path)['type'] == 'directory'

    def isfile(self, path):
        """Is this entry file-like?"""
        return self.info(path)['type'] == 'file'

    def cat(self, path):
        """ Get the content of a file """
        return self.open(path, 'rb').read()

    def get(self, rpath, lpath, **kwargs):
        """ Copy file to local

        Possible extension: maybe should be able to copy to any file-system
        (streaming through local).
        """
        with self.open(rpath, 'rb') as f1:
            with open(lpath, 'wb') as f2:
                data = True
                while data:
                    data = f1.read(self.blocksize)
                    f2.write(data)

    def put(self, lpath, rpath, **kwargs):
        """ Upload file from local """
        with open(lpath, 'rb') as f1:
            with self.open(rpath, 'wb') as f2:
                data = True
                while data:
                    data = f1.read(self.blocksize)
                    f2.write(data)

    def head(self, path, size=1024):
        """ Get the first ``size`` bytes from file """
        with self.open(path, 'rb') as f:
            return f.read(size)

    def tail(self, path, size=1024):
        """ Get the last ``size`` bytes from file """
        with self.open(path, 'rb') as f:
            f.seek(-size, 2)
            return f.read()

    def copy(self, path1, path2, **kwargs):
        """ Copy within two locations in the filesystem"""
        raise NotImplementedError

    def mv(self, path1, path2, **kwargs):
        """ Move file from one location to another """
        self.copy(path1, path2, **kwargs)
        self.rm(path1, recursive=True)

    def _rm(self, path):
        """Delete a file"""
        raise NotImplementedError

    def rm(self, path, recursive=False, maxdepth=None):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """
        # prefer some bulk method, if possible
        if not isinstance(path, list):
            path = [path]
        for p in path:
            if recursive:
                out = self.walk(p, maxdepth=maxdepth)
                for pa, _, files in out:
                    for name in files:
                        self.rm('/'.join([pa, name]))
                for pa, dirs, _ in out:
                    # may fail to remove directories if maxdepth is small
                    for d in dirs:
                        self.rmdir('/'.join([pa, d]))
                self.rmdir(p)
            else:
                self._rm(p)

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        """Return raw bytes-mode file-like from the file-system"""
        raise NotImplementedError

    def open(self, path, mode='rb', block_size=None, **kwargs):
        """
        Return a file-like object from the filesystem

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        path: str
            Target file
        mode: str like 'rb', 'w'
            See builtin ``open()``
        block_size: int
            Some indication of buffering - this is a value in bytes
        """
        import io
        if 'b' not in mode:
            mode = mode.replace('t', '') + 'b'
            return io.TextIOWrapper(
                self.open(path, mode, block_size, **kwargs))
        else:
            ac = kwargs.pop('autocommit', not self._intrans)
            if not self._intrans and not ac:
                raise ValueError('Must use autocommit outside a transaction.')
            f = self._open(path, mode=mode, block_size=block_size,
                           autocommit=ac, **kwargs)
            if not ac:
                self.transaction.files.append(f)
            return f

    def touch(self, path, **kwargs):
        """ Create empty file, or update timestamp """
        if not self.exists(path):
            with self.open(path, 'wb', **kwargs):
                pass
        else:
            raise NotImplementedError  # update timestamp, if possible

    def ukey(self, path):
        """Hash of file properties, to tell if it has changed"""
        return md5(str(self.info(path)).encode()).hexdigest()

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on GCS
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> fs.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> fs.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> fs.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.size
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes

    def __getstate__(self):
        """ Instance should be pickleable """
        d = self.__dict__.copy()
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)

    def _get_pyarrow_filesystem(self):
        """
        Make a version of the FS instance which will be acceptable to pyarrow
        """
        return self

    def get_mapper(self, root, check=False, create=False):
        """Create key/value store based on this file-system

        Makes a MutibleMapping interface to the FS at the given root path.
        See ``fsspec.mapping.FSMap`` for further details.
        """
        from .mapping import FSMap
        return FSMap(root, self, check, create)

    @classmethod
    def clear_instance_cache(cls):
        """Remove cached instances from the class cache"""
        cls._cache.clear()


class Transaction(object):
    """Filesystem transaction write context

    Gathers files for deferred commit or discard, so that several write
    operations can be finalized semi-atomically.
    """

    def __init__(self, fs):
        """
        Parameters
        ----------
        fs: FileSystem instance
        """
        self.fs = fs
        self.files = []

    def __enter__(self):
        """Start a transaction on this FileSystem"""
        self.fs._intrans = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction and commit, if exit is not due to exception"""
        # only commit if there was no exception
        self.complete(commit=exc_type is None)
        self.fs._intrans = False

    def complete(self, commit=True):
        """Finish transation: commit or discard all deferred files"""
        for f in self.files:
            if commit:
                f.commit()
            else:
                f.discard()
        self.files = []
        self.fs._intrans = False
