import io
import logging
import math
import os
import pathlib
import re
import sys
from contextlib import contextmanager
from functools import partial
from hashlib import md5
from urllib.parse import urlsplit

DEFAULT_BLOCK_SIZE = 5 * 2 ** 20
PY36 = sys.version_info < (3, 7)


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
        re.match(r"^[a-zA-Z]:[\\/]", urlpath)
        or re.match(r"^[a-zA-Z0-9]+://", urlpath) is None
    ):
        return {"protocol": "file", "path": urlpath}

    parsed_path = urlsplit(urlpath)
    protocol = parsed_path.scheme or "file"
    if parsed_path.fragment:
        path = "#".join([parsed_path.path, parsed_path.fragment])
    else:
        path = parsed_path.path
    if protocol == "file":
        # Special case parsing file protocol URL on Windows according to:
        # https://msdn.microsoft.com/en-us/library/jj710207.aspx
        windows_path = re.match(r"^/([a-zA-Z])[:|]([\\/].*)$", path)
        if windows_path:
            path = "%s:%s" % windows_path.groups()

    if protocol in ["http", "https"]:
        # for HTTP, we don't want to parse, as requests will anyway
        return {"protocol": protocol, "path": urlpath}

    options = {"protocol": protocol, "path": path}

    if parsed_path.netloc:
        # Parse `hostname` from netloc manually because `parsed_path.hostname`
        # lowercases the hostname which is not always desirable (e.g. in S3):
        # https://github.com/dask/dask/issues/1417
        options["host"] = parsed_path.netloc.rsplit("@", 1)[-1].rsplit(":", 1)[0]

        if protocol in ("s3", "s3a", "gcs", "gs"):
            options["path"] = options["host"] + options["path"]
        else:
            options["host"] = options["host"]
        if parsed_path.port:
            options["port"] = parsed_path.port
        if parsed_path.username:
            options["username"] = parsed_path.username
        if parsed_path.password:
            options["password"] = parsed_path.password

    if parsed_path.query:
        options["url_query"] = parsed_path.query
    if parsed_path.fragment:
        options["url_fragment"] = parsed_path.fragment

    if inherit_storage_options:
        update_storage_options(options, inherit_storage_options)

    return options


def update_storage_options(options, inherited=None):
    if not inherited:
        inherited = {}
    collisions = set(options) & set(inherited)
    if collisions:
        for collision in collisions:
            if options.get(collision) != inherited.get(collision):
                raise KeyError(
                    "Collision between inferred and specified storage "
                    "option:\n%s" % collision
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
    extension = os.path.splitext(filename)[-1].strip(".").lower()
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
    return md5(str(args).encode()).hexdigest()


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
    if isinstance(filepath, str):
        return filepath
    elif hasattr(filepath, "__fspath__"):
        return filepath.__fspath__()
    elif isinstance(filepath, pathlib.Path):
        return str(filepath)
    elif hasattr(filepath, "path"):
        return filepath.path
    else:
        return filepath


def make_instance(cls, args, kwargs):
    inst = cls(*args, **kwargs)
    inst._determine_worker()
    return inst


def common_prefix(paths):
    """For a list of paths, find the shortest prefix common to all"""
    parts = [p.split("/") for p in paths]
    lmax = min(len(p) for p in parts)
    end = 0
    for i in range(lmax):
        end = all(p[i] == parts[0][i] for p in parts)
        if not end:
            break
    i += end
    return "/".join(parts[0][:i])


def other_paths(paths, path2, is_dir=None, exists=False):
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
    exists: bool (optional)
        For a str destination, it is already exists (and is a dir), files should
        end up inside.

    Returns
    -------
    list of str
    """
    if isinstance(path2, str):
        is_dir = is_dir or path2.endswith("/")
        path2 = path2.rstrip("/")
        if len(paths) > 1:
            cp = common_prefix(paths)
            if exists:
                cp = cp.rsplit("/", 1)[0]
            path2 = [p.replace(cp, path2, 1) for p in paths]
        else:
            if is_dir:
                path2 = [path2.rstrip("/") + "/" + paths[0].rsplit("/")[-1]]
            else:
                path2 = [path2]
    else:
        assert len(paths) == len(path2)
    return path2


def is_exception(obj):
    return isinstance(obj, BaseException)


def get_protocol(url):
    parts = re.split(r"(\:\:|\://)", url, 1)
    if len(parts) > 1:
        return parts[0]
    return "file"


def can_be_local(path):
    """Can the given URL be used with open_local?"""
    from fsspec import get_filesystem_class

    try:
        return getattr(get_filesystem_class(get_protocol(path)), "local_file", False)
    except (ValueError, ImportError):
        # not in registry or import failed
        return False


def get_package_version_without_import(name):
    """For given package name, try to find the version without importing it

    Import and package.__version__ is still the backup here, so an import
    *might* happen.

    Returns either the version string, or None if the package
    or the version was not readily  found.
    """
    if name in sys.modules:
        mod = sys.modules[name]
        if hasattr(mod, "__version__"):
            return mod.__version__
    if sys.version_info >= (3, 8):
        try:
            import importlib.metadata

            return importlib.metadata.distribution(name).version
        except:  # noqa: E722
            pass
    else:
        try:
            import importlib_metadata

            return importlib_metadata.distribution(name).version
        except:  # noqa: E722
            pass
    try:
        import importlib

        mod = importlib.import_module(name)
        return mod.__version__
    except (ImportError, AttributeError):
        return None


def setup_logging(logger=None, logger_name=None, level="DEBUG", clear=True):
    if logger is None and logger_name is None:
        raise ValueError("Provide either logger object or logger name")
    logger = logger or logging.getLogger(logger_name)
    handle = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s -- %(message)s"
    )
    handle.setFormatter(formatter)
    if clear:
        logger.handlers.clear()
    logger.addHandler(handle)
    logger.setLevel(level)
    return logger


def _unstrip_protocol(name, fs):
    if isinstance(fs.protocol, str):
        if name.startswith(fs.protocol):
            return name
        return fs.protocol + "://" + name
    else:
        if name.startswith(tuple(fs.protocol)):
            return name
        return fs.protocol[0] + "://" + name


def mirror_from(origin_name, methods):
    """Mirror attributes and methods from the given
    origin_name attribute of the instance to the
    decorated class"""

    def origin_getter(method, self):
        origin = getattr(self, origin_name)
        return getattr(origin, method)

    def wrapper(cls):
        for method in methods:
            wrapped_method = partial(origin_getter, method)
            setattr(cls, method, property(wrapped_method))
        return cls

    return wrapper


@contextmanager
def nullcontext(obj):
    yield obj


def merge_offset_ranges(paths, starts, ends, max_gap=0, max_block=None, sort=True):
    """Merge adjacent byte-offset ranges when the inter-range
    gap is <= `max_gap`, and when the merged byte range does not
    exceed `max_block` (if specified). By default, this function
    will re-order the input paths and byte ranges to ensure sorted
    order. If the user can guarantee that the inputs are already
    sorted, passing `sort=False` will skip the re-ordering.
    """

    # Check input
    if not isinstance(paths, list):
        raise TypeError
    if not isinstance(starts, list):
        starts = [starts] * len(paths)
    if not isinstance(ends, list):
        ends = [starts] * len(paths)
    if len(starts) != len(paths) or len(ends) != len(paths):
        raise ValueError

    # Sort by paths and then ranges if `sort=True`
    if sort:
        paths, starts, ends = [list(v) for v in zip(*sorted(zip(paths, starts, ends)))]

    if paths:
        # Loop through the coupled `paths`, `starts`, and
        # `ends`, and merge adjacent blocks when appropriate
        new_paths = paths[:1]
        new_starts = starts[:1]
        new_ends = ends[:1]
        for i in range(1, len(paths)):
            if (
                paths[i] != paths[i - 1]
                or ((starts[i] - new_ends[-1]) > max_gap)
                or ((max_block is not None and (ends[i] - new_starts[-1]) > max_block))
            ):
                # Cannot merge with previous block.
                # Add new `paths`, `starts`, and `ends` elements
                new_paths.append(paths[i])
                new_starts.append(starts[i])
                new_ends.append(ends[i])
            else:
                # Merge with previous block by updating the
                # last element of `ends`
                new_ends[-1] = ends[i]
        return new_paths, new_starts, new_ends

    # `paths` is empty. Just return input lists
    return paths, starts, ends


def get_parquet_byte_ranges(
    paths,
    fs,
    columns=None,
    row_groups=None,
    max_gap=0,
    max_block=256_000_000,
    footer_sample_size=32_000_000,
    add_header_magic=True,
    engine="fastparquet",
):
    """Get a dictionary of the known byte ranges needed
    to read a specific column/row-group selection from a
    Parquet dataset. Each value in the output dictionary
    is intended for use as the `data` argument for the
    `KnownPartsOfAFile` caching strategy of a single path.
    """

    # Gather file footers.
    # We just take the last `footer_sample_size` bytes of each
    # file (or the entire file if it is smaller than that). While
    footer_starts = []
    footer_ends = []
    file_sizes = []
    for path in paths:
        file_sizes.append(fs.size(path))
        footer_ends.append(file_sizes[-1])
        sample_size = max(0, file_sizes[-1] - footer_sample_size)
        footer_starts.append(sample_size)
    footer_samples = fs.cat_ranges(paths, footer_starts, footer_ends)

    # Calculate required byte ranges for each path
    data_paths = []
    data_starts = []
    data_ends = []
    for i in range(len(paths)):

        # Deal with small-file case.
        # Just include all remaining bytes of the file
        # in a single range.
        if file_sizes[i] < max_block:
            if footer_starts[i] > 0:
                # Only need to transfer the data if the
                # footer sample isn't already the whole file
                data_paths.append(paths[i])
                data_starts.append(0)
                data_ends.append(footer_starts[i])
            continue

        # Read the footer size and re-sample if necessary.
        # It may make sense to warn the user that
        # `footer_sample_size` is too small if we end up in
        # this block (since it will be slow).
        footer_size = int.from_bytes(footer_samples[i][-8:-4], "little")
        if footer_sample_size < (footer_size + 8):
            footer_samples[i] = fs.tail(path, footer_size + 8)
            footer_starts[i] = footer_ends[i] - (footer_size + 8)

        # Use "engine" to collect data byte ranges
        if engine == "fastparquet":
            path_data_starts, path_data_ends = _fastparquet_parquet_byte_ranges(
                footer_samples[i], columns, row_groups, footer_starts[i]
            )
        elif engine == "pyarrow":
            path_data_starts, path_data_ends = _pyarrow_parquet_byte_ranges(
                footer_samples[i], columns, row_groups, footer_starts[i]
            )
        else:
            raise ValueError(f"{engine} engine not supported by parquet_byte_ranges")
        data_paths += [paths[i]] * len(path_data_starts)
        data_starts += path_data_starts
        data_ends += path_data_ends

    # Merge adjacent offset ranges
    data_paths, data_starts, data_ends = merge_offset_ranges(
        data_paths,
        data_starts,
        data_ends,
        max_gap=max_gap,
        max_block=max_block,
        sort=False,  # Should already be sorted
    )

    # Start by populating `result` with footer samples
    result = {}
    for i, path in enumerate(paths):
        result[path] = {(footer_starts[i], footer_ends[i]): footer_samples[i]}

    # Use cat_ranges to gather the data byte_ranges
    for i, data in enumerate(fs.cat_ranges(data_paths, data_starts, data_ends)):
        if data_ends[i] > data_starts[i]:
            result[data_paths[i]][(data_starts[i], data_ends[i])] = data

    # Add b"PAR1" to header if necessary
    if add_header_magic:
        for i, path in enumerate(paths):
            add_magic = True
            for k in result[path].keys():
                if k[0] == 0 and k[1] >= 4:
                    add_magic = False
                    break
            if add_magic:
                result[path][(0, 4)] = b"PAR1"

    return result


def _fastparquet_parquet_byte_ranges(footer, columns, row_groups, footer_start):
    import fastparquet as fp

    data_starts, data_ends = [], []
    pf = fp.ParquetFile(io.BytesIO(footer))
    for r, row_group in enumerate(pf.row_groups):
        # Skip this row-group if we are targetting
        # specific row-groups
        if row_groups is None or r in row_groups:
            for column in row_group.columns:
                name = column.meta_data.path_in_schema[0]
                # Skip this column if we are targetting a
                # specific columns
                if columns is None or name in columns:
                    file_offset0 = column.meta_data.dictionary_page_offset
                    if file_offset0 is None:
                        file_offset0 = column.meta_data.data_page_offset
                    num_bytes = column.meta_data.total_uncompressed_size
                    if file_offset0 < footer_start:
                        data_starts.append(file_offset0)
                        data_ends.append(min(file_offset0 + num_bytes, footer_start))
    return data_starts, data_ends


def _pyarrow_parquet_byte_ranges(footer, columns, row_groups, footer_start):
    import pyarrow.parquet as pq

    data_starts, data_ends = [], []
    md = pq.ParquetFile(io.BytesIO(footer)).metadata
    for r in range(md.num_row_groups):
        # Skip this row-group if we are targetting
        # specific row-groups
        if row_groups is None or r in row_groups:
            row_group = md.row_group(r)
            for c in range(row_group.num_columns):
                column = row_group.column(c)
                name = column.path_in_schema
                # Skip this column if we are targetting a
                # specific columns
                if columns is None or name in columns:
                    file_offset0 = column.dictionary_page_offset
                    if file_offset0 is None:
                        file_offset0 = column.data_page_offset
                    num_bytes = column.total_uncompressed_size
                    if file_offset0 < footer_start:
                        data_starts.append(file_offset0)
                        data_ends.append(min(file_offset0 + num_bytes, footer_start))
    return data_starts, data_ends
