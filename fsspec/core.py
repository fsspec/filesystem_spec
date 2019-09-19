from __future__ import print_function, division, absolute_import

import io
import os
import logging
from .compression import compr
from .utils import (
    infer_compression,
    build_name_function,
    update_storage_options,
    stringify_path,
)
from .registry import get_filesystem_class

logger = logging.getLogger("fsspec")


class OpenFile(object):
    """
    File-like object to be used in a context

    Can layer (buffered) text-mode and compression over any file-system, which
    are typically binary-only.

    These instances are safe to serialize, as the low-level file object
    is not created until invoked using `with`.

    Parameters
    ----------
    fs: FileSystem
        The file system to use for opening the file. Should match the interface
        of ``dask.bytes.local.LocalFileSystem``.
    path: str
        Location to open
    mode: str like 'rb', optional
        Mode of the opened file
    compression: str or None, optional
        Compression to apply
    encoding: str or None, optional
        The encoding to use if opened in text mode.
    errors: str or None, optional
        How to handle encoding errors if opened in text mode.
    newline: None or str
        Passed to TextIOWrapper in text mode, how to handle line endings.
    """

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        compression=None,
        encoding=None,
        errors=None,
        newline=None,
    ):
        self.fs = fs
        self.path = path
        self.mode = mode
        self.compression = get_compression(path, compression)
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.fobjects = []

    def __reduce__(self):
        return (
            OpenFile,
            (
                self.fs,
                self.path,
                self.mode,
                self.compression,
                self.encoding,
                self.errors,
            ),
        )

    def __repr__(self):
        return "<OpenFile '{}'>".format(self.path)

    def __fspath__(self):
        return self.path

    def __enter__(self):
        mode = self.mode.replace("t", "").replace("b", "") + "b"

        f = self.fs.open(self.path, mode=mode)

        self.fobjects = [f]

        if self.compression is not None:
            compress = compr[self.compression]
            f = compress(f, mode=mode[0])
            self.fobjects.append(f)

        if "b" not in self.mode:
            # assume, for example, that 'r' is equivalent to 'rt' as in builtin
            f = io.TextIOWrapper(
                f, encoding=self.encoding, errors=self.errors, newline=self.newline
            )
            self.fobjects.append(f)

        return self.fobjects[-1]

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def open(self):
        """Materialise this as a real open file without context

        The file should be explicitly closed to avoid enclosed open file
        instances persisting
        """
        return self.__enter__()

    def close(self):
        """Close all encapsulated file objects"""
        for f in reversed(self.fobjects):
            if "r" not in self.mode and not f.closed:
                f.flush()
            f.close()
        self.fobjects = []


def open_files(
    urlpath,
    mode="rb",
    compression=None,
    encoding="utf8",
    errors=None,
    name_function=None,
    num=1,
    protocol=None,
    newline=None,
    **kwargs
):
    """ Given a path or paths, return a list of ``OpenFile`` objects.

    For writing, a str path must contain the "*" character, which will be filled
    in by increasing numbers, e.g., "part*" ->  "part1", "part2" if num=2.

    For either reading or writing, can instead provide explicit list of paths.

    Parameters
    ----------
    urlpath: string or list
        Absolute or relative filepath(s). Prefix with a protocol like ``s3://``
        to read from alternative filesystems. To read from multiple files you
        can pass a globstring or a list of paths, with the caveat that they
        must all have the same protocol.
    mode: 'rb', 'wt', etc.
    compression: string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    encoding: str
        For text mode only
    errors: None or str
        Passed to TextIOWrapper in text mode
    name_function: function or None
        if opening a set of files for writing, those files do not yet exist,
        so we need to generate their names by formatting the urlpath for
        each sequence number
    num: int [1]
        if writing mode, number of files we expect to create (passed to
        name+function)
    protocol: str or None
        If given, overrides the protocol found in the URL.
    newline: bytes or None
        Used for line terminator in text mode. If None, uses system default;
        if blank, uses no translation.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> files = open_files('2015-*-*.csv')  # doctest: +SKIP
    >>> files = open_files('s3://bucket/2015-*-*.csv.gz', compression='gzip')  # doctest: +SKIP

    Returns
    -------
    List of ``OpenFile`` objects.
    """
    fs, fs_token, paths = get_fs_token_paths(
        urlpath,
        mode,
        num=num,
        name_function=name_function,
        storage_options=kwargs,
        protocol=protocol,
    )
    return [
        OpenFile(
            fs,
            path,
            mode=mode,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
        for path in paths
    ]


def open(
    urlpath,
    mode="rb",
    compression=None,
    encoding="utf8",
    errors=None,
    protocol=None,
    newline=None,
    **kwargs
):
    """ Given a path or paths, return one ``OpenFile`` object.

    Parameters
    ----------
    urlpath: string or list
        Absolute or relative filepath. Prefix with a protocol like ``s3://``
        to read from alternative filesystems. Should not include glob
        character(s).
    mode: 'rb', 'wt', etc.
    compression: string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    encoding: str
        For text mode only
    errors: None or str
        Passed to TextIOWrapper in text mode
    protocol: str or None
        If given, overrides the protocol found in the URL.
    newline: bytes or None
        Used for line terminator in text mode. If None, uses system default;
        if blank, uses no translation.
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> openfile = open('2015-01-01.csv')  # doctest: +SKIP
    >>> openfile = open('s3://bucket/2015-01-01.csv.gz', compression='gzip')  # doctest: +SKIP
    ... with openfile as f:
    ...     df = pd.read_csv(f)

    Returns
    -------
    ``OpenFile`` object.
    """
    return open_files(
        [urlpath],
        mode,
        compression,
        encoding,
        errors,
        protocol,
        newline=newline,
        **kwargs
    )[0]


def get_compression(urlpath, compression):
    if compression == "infer":
        compression = infer_compression(urlpath)
    if compression is not None and compression not in compr:
        raise ValueError("Compression type %s not supported" % compression)
    return compression


def split_protocol(urlpath):
    """Return protocol, path pair"""
    urlpath = stringify_path(urlpath)
    if "://" in urlpath:
        protocol, path = urlpath.split("://", 1)
        if len(protocol) > 1:
            # excludes Windows paths
            return protocol, path
    return None, urlpath


def strip_protocol(urlpath):
    """Return only path part of full URL, according to appropriate backend"""
    protocol, _ = split_protocol(urlpath)
    cls = get_filesystem_class(protocol)
    return cls._strip_protocol(urlpath)


def expand_paths_if_needed(paths, mode, num, fs, name_function):
    """Expand paths if they have a ``*`` in them.

    :param paths: list of paths
    mode: str
        Mode in which to open files.
    num: int
        If opening in writing mode, number of files we expect to create.
    fs: filesystem object
    name_function: callable
        If opening in writing mode, this callable is used to generate path
        names. Names are generated for each partition by
        ``urlpath.replace('*', name_function(partition_index))``.
    :return: list of paths
    """
    expanded_paths = []
    paths = list(paths)
    if "w" in mode and sum([1 for p in paths if "*" in p]) > 1:
        raise ValueError(
            "When writing data, only one filename mask can " "be specified."
        )
    elif "w" in mode:
        num = max(num, len(paths))
    for curr_path in paths:
        if "*" in curr_path:
            if "w" in mode:
                # expand using name_function
                expanded_paths.extend(_expand_paths(curr_path, name_function, num))
            else:
                # expand using glob
                expanded_paths.extend(fs.glob(curr_path))
        else:
            expanded_paths.append(curr_path)
    # if we generated more paths that asked for, trim the list
    if "w" in mode and len(expanded_paths) > num:
        expanded_paths = expanded_paths[:num]
    return expanded_paths


def get_fs_token_paths(
    urlpath, mode="rb", num=1, name_function=None, storage_options=None, protocol=None
):
    """Filesystem, deterministic token, and paths from a urlpath and options.

    Parameters
    ----------
    urlpath: string or iterable
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    mode: str, optional
        Mode in which to open files.
    num: int, optional
        If opening in writing mode, number of files we expect to create.
    name_function: callable, optional
        If opening in writing mode, this callable is used to generate path
        names. Names are generated for each partition by
        ``urlpath.replace('*', name_function(partition_index))``.
    storage_options: dict, optional
        Additional keywords to pass to the filesystem class.
    protocol: str or None
        To override the protocol specifier in the URL
    """
    if isinstance(urlpath, (list, tuple)):
        if not urlpath:
            raise ValueError("empty urlpath sequence")
        protocols, paths = zip(*map(split_protocol, urlpath))
        protocol = protocol or protocols[0]
        if not all(p == protocol for p in protocols):
            raise ValueError(
                "When specifying a list of paths, all paths must "
                "share the same protocol"
            )
        cls = get_filesystem_class(protocol)
        optionss = list(map(cls._get_kwargs_from_urls, urlpath))
        paths = [cls._strip_protocol(u) for u in urlpath]
        options = optionss[0]
        if not all(o == options for o in optionss):
            raise ValueError(
                "When specifying a list of paths, all paths must "
                "share the same file-system options"
            )
        update_storage_options(options, storage_options)
        fs = cls(**options)
        paths = expand_paths_if_needed(paths, mode, num, fs, name_function)

    elif isinstance(urlpath, str) or hasattr(urlpath, "name"):
        protocols, path = split_protocol(urlpath)
        protocol = protocol or protocols
        cls = get_filesystem_class(protocol)

        options = cls._get_kwargs_from_urls(urlpath)
        path = cls._strip_protocol(urlpath)
        update_storage_options(options, storage_options)
        fs = cls(**options)

        if "w" in mode:
            paths = _expand_paths(path, name_function, num)
        elif "*" in path:
            paths = sorted(fs.glob(path))
        else:
            paths = [path]

    else:
        raise TypeError("url type not understood: %s" % urlpath)

    return fs, fs._fs_token, paths


def _expand_paths(path, name_function, num):
    if isinstance(path, str):
        if path.count("*") > 1:
            raise ValueError("Output path spec must contain exactly one '*'.")
        elif "*" not in path:
            path = os.path.join(path, "*.part")

        if name_function is None:
            name_function = build_name_function(num - 1)

        paths = [path.replace("*", name_function(i)) for i in range(num)]
        if paths != sorted(paths):
            logger.warning(
                "In order to preserve order between partitions"
                " paths created with ``name_function`` should "
                "sort to partition order"
            )
    elif isinstance(path, (tuple, list)):
        assert len(path) == num
        paths = list(path)
    else:
        raise ValueError(
            "Path should be either\n"
            "1. A list of paths: ['foo.json', 'bar.json', ...]\n"
            "2. A directory: 'foo/\n"
            "3. A path with a '*' in it: 'foo.*.json'"
        )
    return paths


class BaseCache(object):
    """Pass-though cache: doesn't keep anything, calls every time

    Acts as base class for other cachers

    Parameters
    ----------
    blocksize: int
        How far to read ahead in numbers of bytes
    fetcher: func
        Function of the form f(start, end) which gets bytes from remote as
        specified
    size: int
        How big this file is
    """

    def __init__(self, blocksize, fetcher, size, **kwargs):
        self.blocksize = blocksize
        self.fetcher = fetcher
        self.size = size

    def _fetch(self, start, end):
        return self.fetcher(start, end)


class MMapCache(BaseCache):
    """memory-mapped sparse file cache

    Opens temporary file, which is filled blocks-wise when data is requested.
    Ensure there is enough disc space in the temporary location.

    This cache method might only work on posix
    """

    def __init__(self, blocksize, fetcher, size, location=None, blocks=None, **kwargs):
        super().__init__(blocksize, fetcher, size)
        self.blocks = set() if blocks is None else blocks
        self.location = location
        self.cache = self._makefile()

    def _makefile(self):
        import tempfile
        import mmap

        if self.size == 0:
            return bytearray()

        # posix version
        if self.location is None or not os.path.exists(self.location):
            if self.location is None:
                fd = tempfile.TemporaryFile()
                self.blocks = set()
            else:
                fd = io.open(self.location, "wb+")
            fd.seek(self.size - 1)
            fd.write(b"1")
            fd.flush()
        else:
            fd = io.open(self.location, "rb+")

        return mmap.mmap(fd.fileno(), self.size)

    def _fetch(self, start, end):
        start_block = start // self.blocksize
        end_block = end // self.blocksize
        need = [i for i in range(start_block, end_block + 1) if i not in self.blocks]
        while need:
            # TODO: not a for loop so we can consolidate blocks later to
            # make fewer fetch calls; this could be parallel
            i = need.pop(0)
            sstart = i * self.blocksize
            send = min(sstart + self.blocksize, self.size)
            self.cache[sstart:send] = self.fetcher(sstart, send)
            self.blocks.add(i)

        return self.cache[start:end]

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["cache"]
        return state

    def __setstate__(self, state):
        # Restore instance attributes
        self.__dict__.update(state)
        self.cache = self._makefile()


class ReadAheadCache(BaseCache):
    """ Cache which reads only when we get beyond a block of data

    This is a much simpler version of BytesCache, and does not attempt to
    fill holes in the cache or keep fragments alive. It is besst suited to
    many small reads in a sequential order (e.g., readling lines from a file).
    """

    def __init__(self, blocksize, fetcher, size, **kwargs):
        super().__init__(blocksize, fetcher, size)
        self.cache = b""
        self.start = 0
        self.end = 0

    def _fetch(self, start, end):
        end = min(self.size, end)
        l = end - start
        if start >= self.size:
            return b""
        elif start >= self.start and end <= self.end:
            # cache hit
            return self.cache[start - self.start : end - self.start]
        elif self.start <= start < self.end:
            # partial hit
            part = self.cache[start - self.start :]
            l -= len(part)
            start = self.end
        else:
            # miss
            part = b""
        end = min(self.size, end + self.blocksize)
        self.cache = self.fetcher(start, end)  # new block replaces old
        self.start = start
        self.end = self.start + len(self.cache)
        return part + self.cache[:l]


class BytesCache(BaseCache):
    """Cache which holds data in a in-memory bytes object

    Implements read-ahead by the block size, for semi-random reads progressing
    through the file.

    Parameters
    ----------
    trim: bool
        As we read more data, whether to discard the start of the buffer when
        we are more than a blocksize ahead of it.
    """

    def __init__(self, blocksize, fetcher, size, trim=True, **kwargs):
        super().__init__(blocksize, fetcher, size)
        self.cache = b""
        self.start = None
        self.end = None
        self.trim = trim

    def _fetch(self, start, end):
        # TODO: only set start/end after fetch, in case it fails?
        # is this where retry logic might go?
        if (
            self.start is not None
            and start >= self.start
            and self.end is not None
            and end < self.end
        ):
            # cache hit: we have all the required data
            offset = start - self.start
            return self.cache[offset : offset + end - start]
        if self.blocksize:
            bend = min(self.size, end + self.blocksize)
        else:
            bend = end
        if bend == start or start > self.size:
            return b""
        if (self.start is None or start < self.start) and (
            self.end is None or end > self.end
        ):
            # First read, or extending both before and after
            self.cache = self.fetcher(start, bend)
            self.start = start
        elif start < self.start:
            if self.end - end > self.blocksize:
                self.cache = self.fetcher(start, bend)
                self.start = start
            else:
                new = self.fetcher(start, self.start)
                self.start = start
                self.cache = new + self.cache
        elif bend > self.end:
            if self.end > self.size:
                pass
            elif end - self.end > self.blocksize:
                self.cache = self.fetcher(start, bend)
                self.start = start
            else:
                new = self.fetcher(self.end, bend)
                self.cache = self.cache + new

        self.end = self.start + len(self.cache)
        offset = start - self.start
        out = self.cache[offset : offset + end - start]
        if self.trim:
            num = (self.end - self.start) // (self.blocksize + 1)
            if num > 1:
                self.start += self.blocksize * num
                self.cache = self.cache[self.blocksize * num :]
        return out

    def __len__(self):
        return len(self.cache)


caches = {
    "none": BaseCache,
    "mmap": MMapCache,
    "bytes": BytesCache,
    "readahead": ReadAheadCache,
}
