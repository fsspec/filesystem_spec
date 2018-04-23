
from .utils import read_block


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
        self._singleton[0] = self

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

    def walk(self, path, detail=False):
        """ Return all files belows path

        Like ``ls``, but recursing into subdirectories. If detail is False,
        returns a list of full paths.
        """

    def du(self, path, total=False, deep=False):
        """Space used by files within a path

        If total is True, returns a number (bytes), if False, returns a
        dict mapping file to size.

        Parameters
        ----------
        total: bool
            whether to sum all the file sized
        deep: bool
            whether to descend into subdirectories.
        """
        if deep:
            sizes = {f['name']: f['size'] for f in self.walk(path, True)}
        else:
            sizes = {f['name']: f['size'] for f in self.ls(path, True)}
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
        pass

    def info(self, path):
        """Give details of entry at path

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``
        """

    def isdir(self, path):
        """Is this entry directory-like?"""

    def cat(self, path):
        """ Get the content of a file """

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

    def open(self, path, mode='rb', block_size=None, **kwargs):
        """
        Return a file-like object from the filesystem

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        mode: str like 'rb', 'w'
            See builtin ``open()``
        block_size: int
            Some indication of buffering - this is a value in bytes
        """
        import io
        if 'b' not in mode:
            mode = mode.replace('t', '') + 'b'
            return io.TextIOWrapper(
                self.open(self, path, mode, block_size, **kwargs))

    def touch(self, path, **kwargs):
        """ Create empty file """
        with self.open(path, 'wb', **kwargs):
            pass

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
