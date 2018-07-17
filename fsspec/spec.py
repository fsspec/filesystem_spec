from contextlib import contextmanager
from .utils import read_block


aliases = [
    ('makedir', 'mkdir'),
    ('listdir', 'ls'),
    ('cp', 'copy'),
    ('move', 'mv'),
    ('delete', 'rm'),
]


class AbstractFileSystem(object):
    """
    A specification for python file-systems
    """
    _singleton = [None]

    def __init__(self, *args, **kwargs):
        """Configure

        Instances may be cachable, so if similar enough arguments are seen
        a new instance is not required.

        A reasonable default should be provided if there are no arguments
        """
        self.autocommit = True
        self.files = None
        self._intrans = False
        self._singleton[0] = self
        for new, old in aliases:
            setattr(self, new, getattr(self, old))

    @classmethod
    def current(cls):
        """ Return the most recently created FileSystem

        If no instance has been created, then create one with defaults
        """
        if not cls._singleton[0]:
            return AbstractFileSystem()
        else:
            return cls._singleton[0]

    def invalidate_cache(self, path=None):
        """
        Discard any cached directory information

        Parameters
        ----------
        path: string or None
            If None, clear all listings cached else listings at or under given
            path.
        """
        pass

    @contextmanager
    def transaction(self):
        self.files = []
        self._intrans = True
        yield
        for f in self.files:
            f.commit()
        self.files = None
        self._intrans = False

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
        pass

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

    def rmdir(self, path):
        """Remove a directory, if empty"""
        pass

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
        may be present, aproriate to the file-system, e.g., generation,
        checksum, etc.

        Parameters
        ----------
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        """
        pass

    def walk(self, path, simple=False):
        """ Return all files belows path

        Similar to ``ls``, but recursing into subdirectories.

        Parameters
        ----------
        path: str
            Root to recurse into
        simple: bool (False)
            If True, returns a list of filenames. If False, returns an
            iterator over tuples like
            (dirpath, dirnames, filenames), see ``os.walk``.
        """

    def du(self, path, total=False):
        """Space used by files within a path

        If total is True, returns a number (bytes), if False, returns a
        dict mapping file to size.

        Parameters
        ----------
        total: bool
            whether to sum all the file sizes
        """
        sizes = {}
        for f in self.walk(path, True):
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
        would with ``detail=True``

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """

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

    def put(self, lpath, rpath, **kwargs):
        """ Upload file from local """

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


    def mv(self, path1, path2, **kwargs):
        """ Move file from one location to another """
        self.copy(path1, path2, **kwargs)
        self.rm(path1)

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

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        pass

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
            f = self._open(path, mode=mode, block_size=block_size,
                           autocommit=ac, **kwargs)
            if not ac:
                self.files.append(f)
            return f

    def touch(self, path, **kwargs):
        """ Create empty file, or update timestamp """
        if not self.exists(path):
            with self.open(path, 'wb', **kwargs):
                pass
        else:
            raise NotImplementedError  # update timestamp, if possible

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
