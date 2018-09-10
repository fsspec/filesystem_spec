from hashlib import md5
from .utils import read_block

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
    _singleton = [None]
    blocksize = 2**22
    protocol = 'abstract'

    def __init__(self, *args, **storage_options):
        """Create and configure file-system instance

        Instances may be cachable, so if similar enough arguments are seen
        a new instance is not required.

        A reasonable default should be provided if there are no arguments.

        Subclasses should call this method.

        Magic kwargs that affect functionality here:
        add_docs: if True, will append docstrings from this spec to the
            specific implementation
        add_aliases: if True, will add method aliases
        """
        self.autocommit = True
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
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        """
        raise NotImplementedError

    def walk(self, path, simple=False, maxdepth=3):
        """ Return all files belows path

        Similar to ``ls``, but recursing into subdirectories.

        Parameters
        ----------
        path: str
            Root to recurse into
        simple: bool (False)
            If True, returns an iterator of filenames. If False, returns an
            iterator over tuples like
            (dirpath, dirnames, filenames), see ``os.walk``.
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recomended
            on link-based file-systems.
        """
        full_dirs = []
        dirs = []
        files = []

        for info in self.ls(path, True):
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            name = info['name']
            if name.endswith('/'):
                tail = '/'.join(name.rsplit('/', 2)[-2:])
            else:
                tail = name.rsplit('/', 1)[1]
            if info['type'] == 'directory':
                full_dirs.append(name)
                dirs.append(tail)
            else:
                files.append(tail)
        if simple:
            for name in files:
                yield '/'.join([path.rstrip('/'), name])
        else:
            yield path, dirs, files

        for d in full_dirs:
            if maxdepth is None or maxdepth > 1:
                for res in self.walk(d, maxdepth=(maxdepth - 1)
                                     if maxdepth is not None else None):
                    if simple:
                        path, dirs, files = res
                        for name in files:
                            yield '/'.join([path, name])
                    else:
                        yield res

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
        for f in self.walk(path, True, maxdepth=maxdepth):
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

        We do not attempt to match for ``"**"`` notation.

        Example reimplements code in ``glob.glob()``, taken from hdfs3.
        """
        import re
        import posixpath
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind + 1]
        else:
            root = '/'
        allpaths = []
        for dirname, dirs, fils in self.walk(root):
            allpaths.extend(posixpath.join(dirname, d) for d in dirs)
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
        except:
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
        return self.ls(path, detail=True)[0]

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
        self.rm(path1)

    def _rm(self, path):
        """Delete a file"""
        raise NotImplementedError

    def rm(self, path, recursive=False):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        """
        if recursive:
            # prefer some bulk method, if possible
            [self._rm(f) for f in self.walk(path, simple=True)]
        else:
            self._rm(path)

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


class Transaction(object):
    """Filesystem transaction context

    Gathers files for deferred commit or discard, so that several write
    operations can be finalized semi-atomically.
    """

    def __init__(self, fs):
        self.fs = fs

    def __enter__(self):
        self.files = []
        self.fs._intrans = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        # only commit if there was no exception
        self.complete(commit=exc_type is None)

    def complete(self, commit=True):
        for f in self.files:
            if commit:
                f.commit()
            else:
                f.discard()
        self.files = []
        self.fs._intrans = False
