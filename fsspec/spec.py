from hashlib import md5
import io
from .utils import read_block, tokenize

# alternative names for some methods, which get patched to new instances
# (alias, original)
aliases = [
    ('makedir', 'mkdir'),
    ('mkdirs', 'makedirs'),
    ('listdir', 'ls'),
    ('cp', 'copy'),
    ('move', 'mv'),
    ('stat', 'info'),
    ('disk_usage', 'du'),
    ('rename', 'mv'),
    ('delete', 'rm'),
]

try:   # optionally derive from pyarrow's FileSystem, if available
    import pyarrow as pa
    up = pa.filesystem.DaskFileSystem
except ImportError:
    up = object


class AbstractFileSystem(up):
    """
    An abstract super-class for pythonic file-systems

    Implementations are expected to be compatible with or, better, subclass
    from here.
    """
    _singleton = [None]  # will contain the newest instance
    _cache = None
    cachable = True  # this class can be cached, instances reused
    _cached = False
    blocksize = 2**22
    protocol = 'abstract'
    root_marker = ""  # For some FSs, may require leading '/' or other character

    def __new__(cls, *args, **storage_options):
        """
        Will reuse existing instance if:
        - cls.cachable is True and
        - the token (a hash of args and kwargs by default) exists in the cache

        The instance will skip init if instance.cached = True.
        """
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
        self.dircache = {}
        if storage_options.pop('add_docs', True):
            self._mangle_docstrings()
        if storage_options.pop('add_aliases', True):
            for new, old in aliases:
                if not hasattr(self, new):
                    # don't apply alias if attribute exists already
                    setattr(self, new, getattr(self, old))

    def __dask_tokenize__(self):
        return self.token

    def __hash__(self):
        return int(self._tok, 16)

    def __eq__(self, other):
        return self.token == other.token

    @classmethod
    def clear_instance_cache(cls, remove_singleton=True):
        """Remove any instances stored in class attributes"""
        cls._cache.clear()
        if remove_singleton:
            cls._singleton = [None]

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

    @classmethod
    def _strip_protocol(cls, path):
        """ Turn path from fully-qualified to file-system-specific

        May require FS-specific handling, e.g., for relative paths or links.
        """
        path = path.rstrip('/')
        if path.startswith(cls.protocol + '://'):
            path = path[len(cls.protocol) + 3:]
        elif path.startswith(cls.protocol + ':'):
            path = path[len(cls.protocol) + 1:]
        elif path.startswith(cls.protocol):
            path = path[len(cls.protocol):]
        # use of root_marker to make minimum required path, e.g., "/"
        return path or cls.root_marker

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

    def ls(self, path, **kwargs):
        """List objects at path.

        This should include subdirectories and files at that location. The
        difference between a file and a directory must be clear when details
        are requested.

        The specific keys, or perhaps a FileInfo class, or similar, is TBD,
        but must be consistent across implementations.
        Must include:
        - full path to the entry (without protocol)
        - size of the entry, in bytes
        - type of entry, "file", "directory" or other

        Additional information
        may be present, aproriate to the file-system, e.g., generation,
        checksum, etc.

        May use refresh=True|False to allow use of self._ls_from_cache to
        check for a saved listing and avoid calling the backend. This would be
        common where listing may be expensive.

        Parameters
        ----------
        path: str
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        kwargs: may have additional backend-specific options, such as version
            information

        Returns
        -------
        List of strings if detail is False, or list of directory information
        dicts if detail is True.
        """
        raise NotImplementedError

    def _ls_from_cache(self, path):
        """Check cache for listing

        Returns listing, if found (may me empty list for a directly that exists
        but contains nothing), None if not in cache.
        """
        parent = self._parent(path)
        if path in self.dircache:
            return self.dircache[path]
        elif parent in self.dircache:
            files = [f for f in self.dircache[parent] if f['name'] == path]
            if len(files) == 0:
                # parent dir was listed but did not contain this file
                raise FileNotFoundError(path)
            return files

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

        try:
            listing = self.ls(path, True)
        except FileNotFoundError:
            return [], [], []

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            name = info['name'].rstrip('/')
            if info['type'] == 'directory' and name != path:
                # do not include "self" path
                full_dirs.append(name)
                dirs.append(name.rsplit('/', 1)[-1])
            elif name == path:
                # file-like with same name as give path
                files.append('')
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
        path: str
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
        pattern = re.compile("^" + path.replace('.', r'\.')
                             .replace('//', '/')
                             .rstrip('/')
                             .replace('*', '[^/]*')
                             .replace('?', '.') + "$")
        out = {p for p in allpaths
               if pattern.match(p.replace('//', '/').rstrip('/'))}
        return list(sorted(out))

    def exists(self, path):
        """Is there a file at the given path"""
        try:
            self.info(path)
            return True
        except:   # any exception allowed bar FileNotFoundError?
            return False

    def info(self, path, **kwargs):
        """Give details of entry at path

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        out = self.ls(self._parent(path), detail=True, **kwargs)
        out = [o for o in out if o['name'].rstrip('/') == path]
        if out:
            return out[0]
        out = self.ls(path, detail=True, **kwargs)
        path = path.rstrip('/')
        out1 = [o for o in out if o['name'].rstrip('/') == path]
        if len(out1) == 1:
            return out1[0]
        elif len(out1) > 1:
            return {'name': path, 'size': 0, 'type': 'directory'}
        else:
            raise FileNotFoundError

    def checksum(self, path):
        """Unique value for current version of file

        If the checksum is the same from one moment to another, the contents
        are guaranteed to be the same. If the checksum changes, the contents
        *might* have changed.

        This should normally be overridden; default will probably capture
        creation/modification timestamp (which would be good) or maybe
        access timestamp (which would be bad)
        """
        return int(tokenize(self.info(path)), 16)

    def size(self, path):
        """Size in bytes of file"""
        return self.info(path)['size']

    def isdir(self, path):
        """Is this entry directory-like?"""
        try:
            return self.info(path)['type'] == 'directory'
        except FileNotFoundError:
            return False

    def isfile(self, path):
        """Is this entry file-like?"""
        try:
            return self.info(path)['type'] == 'file'
        except FileNotFoundError:
            return False

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
            f.seek(max(-size, -f.size), 2)
            return f.read()

    def copy(self, path1, path2, **kwargs):
        """ Copy within two locations in the filesystem"""
        raise NotImplementedError

    def mv(self, path1, path2, **kwargs):
        """ Move file from one location to another """
        self.copy(path1, path2, **kwargs)
        self.rm(path1, recursive=False)

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
                for pa, _, files in reversed(list(out)):
                    for name in files:
                        self.rm('/'.join([pa, name]))
                    self.rmdir(pa)
            else:
                self._rm(p)

    @classmethod
    def _parent(cls, path):
        path = path.rstrip('/').lstrip('/')
        if '/' in path:
            return path.rsplit('/', 1)[0]
        else:
            return cls.root_marker

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        """Return raw bytes-mode file-like from the file-system"""
        return AbstractBufferedFile(self, path, mode, block_size, autocommit)

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
        path = self._strip_protocol(path)
        if 'b' not in mode:
            mode = mode.replace('t', '') + 'b'
            return io.TextIOWrapper(self.open(path, mode, block_size, **kwargs))
        else:
            ac = kwargs.pop('autocommit', not self._intrans)
            f = self._open(path, mode=mode, block_size=block_size,
                           autocommit=ac, **kwargs)
            if not ac:
                self.transaction.files.append(f)
            return f

    def touch(self, path, truncate=True, **kwargs):
        """ Create empty file, or update timestamp

        Parameters
        ----------
        path : str
            file location
        truncate : bool
            If True, always set file size to 0; if False, update timestamp and
            leave file unchanged, if backend allows this
        """
        if truncate or not self.exists(path):
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
            Path to filename
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
            if size is not None and offset + length > size:
                length = size - offset
            return read_block(f, offset, length, delimiter)

    def __getstate__(self):
        """ Instance should be pickleable """
        d = self.__dict__.copy()
        d.pop('dircache')
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.dircache = {}

    def _get_pyarrow_filesystem(self):
        """
        Make a version of the FS instance which will be acceptable to pyarrow
        """
        # all instances already also derive from pyarrow
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
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction and commit, if exit is not due to exception"""
        # only commit if there was no exception
        self.complete(commit=exc_type is None)
        self.fs._intrans = False

    def start(self):
        """Start a transaction on this FileSystem"""
        self.fs._intrans = True

    def complete(self, commit=True):
        """Finish transaction: commit or discard all deferred files"""
        for f in self.files:
            if commit:
                f.commit()
            else:
                f.discard()
        self.files = []
        self.fs._intrans = False


class AbstractBufferedFile(object):
    """Convenient class to derive from to provide buffering

    In the case that the backend does not provide a pythonic file-like object
    already, this class contains much of the logic to build one. The only
    methods that need to be overridden are ``_upload_chunk``,
    ``_initate_upload`` and ``_fetch_range``.
    """
    DEFAULT_BLOCK_SIZE = 5 * 2**20

    def __init__(self, fs, path, mode='rb', block_size='default',
                 autocommit=True, cache_type='bytes', **kwargs):
        """
        Template for files with buffered reading and writing

        Parameters
        ----------
        fs: instance of FileSystem
        path: str
            location in file-system
        mode: str
            Normal file modes. Currently only 'wb', 'ab' or 'rb'. Some file
            systems may be read-only, and some may not support append.
        block_size: int
            Buffer size for reading or writing, 'default' for class default
        autocommit: bool
            Whether to write to final destination; may only impact what
            happens when file is being closed.
        cache_type : str
            Caching policy in read mode, one of 'none', 'bytes', 'mmap', see
            the definitions in ``core``.
        kwargs:
            Gets stored as self.kwargs
        """
        from .core import caches
        self.path = path
        self.fs = fs
        self.mode = mode
        self.blocksize = (self.DEFAULT_BLOCK_SIZE
                          if block_size == 'default' else block_size)
        self.loc = 0
        self.autocommit = autocommit
        self.end = None
        self.start = None
        self.closed = False
        self.trim = kwargs.pop('trim', True)
        self.kwargs = kwargs
        if mode not in {'ab', 'rb', 'wb'}:
            raise NotImplementedError('File mode not supported')
        if mode == 'rb':
            if not hasattr(self, 'details'):
                self.details = fs.info(path)
            self.size = self.details['size']
            self.cache = caches[cache_type](self.blocksize, self._fetch_range,
                                            self.size, trim=self.trim)
        else:
            self.buffer = io.BytesIO()
            self.offset = 0
            self.forced = False
            self.location = None

    def __hash__(self):
        return self.fs.checksum(self.path)

    def __eq__(self, other):
        """Files are equal if they have the same checksum, only in read mode"""
        assert (self.mode == 'rb' and other.mode == 'rb'
                and hash(self) == hash(other))

    def commit(self):
        """Move from temp to final destination"""

    def discard(self):
        """Throw away temporary file"""

    def info(self):
        """ File information about this path """
        return self.details  # error in write mode

    def tell(self):
        """ Current file location """
        return self.loc

    def seek(self, loc, whence=0):
        """ Set current file location

        Parameters
        ----------
        loc : int
            byte location
        whence : {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        loc = int(loc)
        if not self.mode == 'rb':
            raise ValueError('Seek only available in read mode')
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError(
                "invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError('Seek before start of file')
        self.loc = nloc
        return self.loc

    def write(self, data):
        """
        Write data to buffer.

        Buffer only sent on flush() or if buffer is greater than
        or equal to blocksize.

        Parameters
        ----------
        data : bytes
            Set of bytes to be written.
        """
        if self.mode not in {'wb', 'ab'}:
            raise ValueError('File not in write mode')
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        if self.forced:
            raise ValueError('This file has been force-flushed, can only close')
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() >= self.blocksize:
            self.flush()
        return out

    def flush(self, force=False):
        """
        Write buffered data to backend store.

        Writes the current buffer, if it is larger than the block-size, or if
        the file is being closed.

        Parameters
        ----------
        force : bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be. Disallows further writing to this file.
        """

        if self.closed:
            raise ValueError('Flush on closed file')
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")

        if self.mode not in {'wb', 'ab'}:
            assert not hasattr(self, "buffer"), "flush on read-mode file " \
                                                "with non-empty buffer"
            return
        if self.buffer.tell() == 0 and not force:
            # no data in the buffer to write
            return

        if not self.offset:
            if not force and self.buffer.tell() < self.blocksize:
                # Defer write on small block
                return
            else:
                # Initialize a multipart upload
                self._initiate_upload()

        self._upload_chunk(final=force)
        self.offset += self.buffer.seek(0, 2)
        self.buffer = io.BytesIO()

        if force:
            self.forced = True

    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        # may not yet have been initialized, may neet to call _initialize_upload

    def _initiate_upload(self):
        """ Create remote file/upload """
        pass

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        raise NotImplementedError

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary

        Parameters
        ----------
        length : int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        length = int(length)
        if self.mode != 'rb':
            raise ValueError('File not in read mode')
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        out = self.cache._fetch(self.loc, self.loc + length)
        self.loc += len(out)
        return out

    def readinto(self, b):
        """mirrors builtin file's readinto method

        https://docs.python.org/3/library/io.html#io.RawIOBase.readinto
        """
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)

    def readinto1(self, b):
        return self.readinto(b)

    def close(self):
        """ Close file

        Finalizes writes, discards cache
        """
        if self.closed:
            return
        if self.mode == 'rb':
            self.cache = None
        else:
            if not self.forced:
                self.flush(force=True)
            else:
                assert self.buffer.tell() == 0

            self.fs.invalidate_cache(self.path)
            self.fs.invalidate_cache(self.fs._parent(self.path))

        self.closed = True

    def readable(self):
        """Whether opened for reading"""
        return self.mode == 'rb' and not self.closed

    def seekable(self):
        """Whether is seekable (only in read mode)"""
        return self.readable()

    def writable(self):
        """Whether opened for writing"""
        return self.mode in {'wb', 'ab'} and not self.closed

    def __del__(self):
        self.close()

    def __str__(self):
        return "<File-like object %s, %s>" % (type(self.fs).__name__, self.path)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
