import math
import os
import pathlib
import re
import sys
from hashlib import sha256
from urllib.parse import urlsplit

DEFAULT_BLOCK_SIZE = 5 * 2 ** 20


def infer_storage_options(urlpath, inherit_storage_options=None):
    """Infer storage options from URL path and merge it with existing storage
    options.

    Parameters
    ----------
    urlpath: str or unicode
        Either local absolute file path or URL (hdfs://namenode:8020/file.csv)
    inherit_storage_options: dict (optional)
        Its contents will get merged with the inferred information from the
        given path

    Returns
    -------
    Storage options dict.

    Examples
    --------
    >>> infer_storage_options('/mnt/datasets/test.csv')  # doctest: +SKIP
    {"protocol": "file", "path", "/mnt/datasets/test.csv"}
    >>> infer_storage_options(
    ...     'hdfs://username:pwd@node:123/mnt/datasets/test.csv?q=1',
    ...     inherit_storage_options={'extra': 'value'},
    ... )  # doctest: +SKIP
    {"protocol": "hdfs", "username": "username", "password": "pwd",
    "host": "node", "port": 123, "path": "/mnt/datasets/test.csv",
    "url_query": "q=1", "extra": "value"}
    """
    # Handle Windows paths including disk name in this special case
    if (
        re.match(r'^[a-zA-Z]:[\\/]', urlpath)
        or re.match(r'^[a-zA-Z0-9]+://', urlpath) is None
    ):
        return {'protocol': 'file', 'path': urlpath}

    parsed_path = urlsplit(urlpath)
    protocol = parsed_path.scheme or 'file'
    if parsed_path.fragment:
        path = '#'.join([parsed_path.path, parsed_path.fragment])
    else:
        path = parsed_path.path
    if protocol == 'file':
        # Special case parsing file protocol URL on Windows according to:
        # https://msdn.microsoft.com/en-us/library/jj710207.aspx
        windows_path = re.match(r'^/([a-zA-Z])[:|]([\\/].*)$', path)
        if windows_path:
            path = '%s:%s' % windows_path.groups()

    if protocol in ['http', 'https']:
        # for HTTP, we don't want to parse, as requests will anyway
        return {'protocol': protocol, 'path': urlpath}

    options = {'protocol': protocol, 'path': path}

    if parsed_path.netloc:
        # Parse `hostname` from netloc manually because `parsed_path.hostname`
        # lowercases the hostname which is not always desirable (e.g. in S3):
        # https://github.com/dask/dask/issues/1417
        options['host'] = parsed_path.netloc.rsplit('@', 1)[-1].rsplit(':', 1)[0]

        if protocol in ('s3', 'gcs', 'gs'):
            options['path'] = options['host'] + options['path']
        else:
            options['host'] = options['host']
        if parsed_path.port:
            options['port'] = parsed_path.port
        if parsed_path.username:
            options['username'] = parsed_path.username
        if parsed_path.password:
            options['password'] = parsed_path.password

    if parsed_path.query:
        options['url_query'] = parsed_path.query
    if parsed_path.fragment:
        options['url_fragment'] = parsed_path.fragment

    if inherit_storage_options:
        update_storage_options(options, inherit_storage_options)

    return options


def update_storage_options(options, inherited=None):
    if not inherited:
        inherited = {}
    collisions = set(options) & set(inherited)
    if collisions:
        collisions = '\n'.join('- %r' % k for k in collisions)
        raise KeyError(
            'Collision between inferred and specified storage '
            'options:\n%s' % collisions
        )
    options.update(inherited)


# Compression extensions registered via fsspec.compression.register_compression
compressions = {}


def infer_compression(filename):
    """Infer compression, if available, from filename.

    Infer a named compression type, if registered and available, from filename
    extension. This includes builtin (gz, bz2, zip) compressions, as well as
    optional compressions. See fsspec.compression.register_compression.
    """
    extension = os.path.splitext(filename)[-1].strip('.')
    if extension in compressions:
        return compressions[extension]


def build_name_function(max_int):
    """Returns a function that receives a single integer
    and returns it as a string padded by enough zero characters
    to align with maximum possible integer

    >>> name_f = build_name_function(57)

    >>> name_f(7)
    '07'
    >>> name_f(31)
    '31'
    >>> build_name_function(1000)(42)
    '0042'
    >>> build_name_function(999)(42)
    '042'
    >>> build_name_function(0)(0)
    '0'
    """
    # handle corner cases max_int is 0 or exact power of 10
    max_int += 1e-8

    pad_length = int(math.ceil(math.log10(max_int)))

    def name_function(i):
        return str(i).zfill(pad_length)

    return name_function


def seek_delimiter(file, delimiter, blocksize):
    r"""Seek current file to file start, file end, or byte after delimiter seq.

    Seeks file to next chunk delimiter, where chunks are defined on file start,
    a delimiting sequence, and file end. Use file.tell() to see location afterwards.
    Note that file start is a valid split, so must be at offset > 0 to seek for
    delimiter.

    Parameters
    ----------
    file: a file
    delimiter: bytes
        a delimiter like ``b'\n'`` or message sentinel, matching file .read() type
    blocksize: int
        Number of bytes to read from the file at once.


    Returns
    -------
    Returns True if a delimiter was found, False if at file start or end.

    """

    if file.tell() == 0:
        # beginning-of-file, return without seek
        return False

    # Interface is for binary IO, with delimiter as bytes, but initialize last
    # with result of file.read to preserve compatibility with text IO.
    last = None
    while True:
        current = file.read(blocksize)
        if not current:
            # end-of-file without delimiter
            return False
        full = last + current if last else current
        try:
            if delimiter in full:
                i = full.index(delimiter)
                file.seek(file.tell() - (len(full) - i) + len(delimiter))
                return True
            elif len(current) < blocksize:
                # end-of-file without delimiter
                return False
        except (OSError, ValueError):
            pass
        last = full[-len(delimiter) :]


def read_block(f, offset, length, delimiter=None, split_before=False):
    """Read a block of bytes from a file

    Parameters
    ----------
    f: File
        Open file
    offset: int
        Byte offset to start read
    length: int
        Number of bytes to read, read through end of file if None
    delimiter: bytes (optional)
        Ensure reading starts and stops at delimiter bytestring
    split_before: bool (optional)
        Start/stop read *before* delimiter bytestring.


    If using the ``delimiter=`` keyword argument we ensure that the read
    starts and stops at delimiter boundaries that follow the locations
    ``offset`` and ``offset + length``.  If ``offset`` is zero then we
    start at zero, regardless of delimiter.  The bytestring returned WILL
    include the terminating delimiter string.

    Examples
    --------

    >>> from io import BytesIO  # doctest: +SKIP
    >>> f = BytesIO(b'Alice, 100\\nBob, 200\\nCharlie, 300')  # doctest: +SKIP
    >>> read_block(f, 0, 13)  # doctest: +SKIP
    b'Alice, 100\\nBo'

    >>> read_block(f, 0, 13, delimiter=b'\\n')  # doctest: +SKIP
    b'Alice, 100\\nBob, 200\\n'

    >>> read_block(f, 10, 10, delimiter=b'\\n')  # doctest: +SKIP
    b'Bob, 200\\nCharlie, 300'
    """
    if delimiter:
        f.seek(offset)
        found_start_delim = seek_delimiter(f, delimiter, 2 ** 16)
        if length is None:
            return f.read()
        start = f.tell()
        length -= start - offset

        f.seek(start + length)
        found_end_delim = seek_delimiter(f, delimiter, 2 ** 16)
        end = f.tell()

        # Adjust split location to before delimiter iff seek found the
        # delimiter sequence, not start or end of file.
        if found_start_delim and split_before:
            start -= len(delimiter)

        if found_end_delim and split_before:
            end -= len(delimiter)

        offset = start
        length = end - start

    f.seek(offset)
    b = f.read(length)
    return b


def tokenize(*args, **kwargs):
    """Deterministic token

    (modified from dask.base)

    >>> tokenize([1, 2, '3'])
    '9d71491b50023b06fc76928e6eddb952'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs:
        args += (kwargs,)
    return sha256(str(args).encode()).hexdigest()


def stringify_path(filepath):
    """Attempt to convert a path-like object to a string.

    Parameters
    ----------
    filepath: object to be converted

    Returns
    -------
    filepath_str: maybe a string version of the object

    Notes
    -----
    Objects supporting the fspath protocol (Python 3.6+) are coerced
    according to its __fspath__ method.

    For backwards compatibility with older Python version, pathlib.Path
    objects are specially coerced.

    Any other object is passed through unchanged, which includes bytes,
    strings, buffers, or anything else that's not even path-like.
    """
    if hasattr(filepath, '__fspath__'):
        return filepath.__fspath__()
    elif isinstance(filepath, pathlib.Path):
        return str(filepath)
    return filepath


def make_instance(cls, args, kwargs):
    inst = cls(*args, **kwargs)
    inst._determine_worker()
    return inst


def common_prefix(paths):
    """For a list of paths, find the shortest prefix common to all"""
    parts = [p.split('/') for p in paths]
    lmax = min(len(p) for p in parts)
    end = 0
    for i in range(lmax):
        end = all(p[i] == parts[0][i] for p in parts)
        if not end:
            break
    i += end
    return '/'.join(parts[0][:i])


def other_paths(paths, path2, is_dir=None):
    """In bulk file operations, construct a new file tree from a list of files

    Parameters
    ----------
    paths: list of str
        The input file tree
    path2: str or list of str
        Root to construct the new list in. If this is already a list of str, we just
        assert it has the right number of elements.
    is_dir: bool (optional)
        For the special case where the input in one element, whether to regard the value
        as the target path, or as a directory to put a file path within. If None, a
        directory is inferred if the path ends in '/'

    Returns
    -------
    list of str
    """
    if isinstance(path2, str):
        is_dir = is_dir or path2.endswith('/')
        path2 = path2.rstrip('/')
        if len(paths) > 1:
            cp = common_prefix(paths)
            path2 = [p.replace(cp, path2, 1) for p in paths]
        else:
            if is_dir:
                path2 = [path2.rstrip('/') + '/' + paths[0].rsplit('/')[-1]]
            else:
                path2 = [path2]
    else:
        assert len(paths) == len(path2)
    return path2


def is_exception(obj):
    return isinstance(obj, BaseException)


def get_protocol(url):
    parts = re.split(r'(\:\:|\://)', url, 1)
    if len(parts) > 1:
        return parts[0]
    return 'file'


def can_be_local(path):
    """Can the given URL be used wih open_local?"""
    from fsspec import get_filesystem_class

    try:
        return getattr(get_filesystem_class(get_protocol(path)), 'local_file', False)
    except (ValueError, ImportError):
        # not in registry or import failed
        return False


def setup_logger(logname, level='DEBUG', clear=True):
    """Add standard logging handler to logger of given name"""
    import logging

    logger = logging.getLogger(logname)
    if clear:
        logger.handlers.clear()
    handle = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s ' '- %(message)s'
    )
    handle.setFormatter(formatter)
    logger.addHandler(handle)
    logger.setLevel(level)
    return logger


def get_package_version_without_import(name):
    """For given package name, try to find the version without importing it

    Import and package.__version__ is still the backup here, so an import
    *might* happen.

    Returns either the version string, or None if the package
    or the version was not readily  found.
    """
    if name in sys.modules:
        mod = sys.modules[name]
        if hasattr(mod, '__version__'):
            return mod.__version__
    if sys.version_info >= (3, 8):
        try:
            import importlib.metadata

            return importlib.metadata.distribution(name).version
        except ImportError:
            pass
    else:
        try:
            import importlib_metadata

            return importlib_metadata.distribution(name).version
        except ImportError:
            pass
    try:
        import importlib

        mod = importlib.import_module(name)
        return mod.__version__
    except (ImportError, AttributeError):
        return None
