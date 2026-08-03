from __future__ import annotations

import contextlib
import logging
import math
import os
import re
import sys
import tempfile
from collections.abc import Callable, Iterable, Iterator, Sequence
from functools import partial
from hashlib import md5
from importlib.metadata import version
from typing import IO, TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlsplit

if TYPE_CHECKING:
    import pathlib
    from typing import TypeGuard

    from fsspec.spec import AbstractFileSystem


DEFAULT_BLOCK_SIZE = 5 * 2**20

T = TypeVar("T")


def infer_storage_options(
    urlpath: str, inherit_storage_options: dict[str, Any] | None = None
) -> dict[str, Any]:
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

    # Discover Windows paths including disk name in this special case.
    is_filesystem = re.match(r"^[a-zA-Z]:[\\/]", urlpath)

    # Discover URI according to RFC 3986: Scheme names consist of a
    # sequence of characters beginning with a letter and followed by
    # any combination of letters, digits, plus ("+"), period ("."),
    # or hyphen ("-").
    # https://datatracker.ietf.org/doc/html/rfc3986#section-3.1
    is_uri = re.match(r"^[a-zA-Z0-9+.-]+://", urlpath)

    if is_filesystem or is_uri is None:
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
            drive, path = windows_path.groups()
            path = f"{drive}:{path}"

    if protocol in ["http", "https"]:
        # for HTTP, we don't want to parse, as requests will anyway
        return {"protocol": protocol, "path": urlpath}

    options: dict[str, Any] = {"protocol": protocol, "path": path}

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


def update_storage_options(
    options: dict[str, Any], inherited: dict[str, Any] | None = None
) -> None:
    if not inherited:
        inherited = {}
    collisions = set(options) & set(inherited)
    if collisions:
        for collision in collisions:
            if options.get(collision) != inherited.get(collision):
                raise KeyError(
                    f"Collision between inferred and specified storage "
                    f"option:\n{collision}"
                )
    options.update(inherited)


# Compression extensions registered via fsspec.compression.register_compression
compressions: dict[str, str] = {}


def infer_compression(filename: str) -> str | None:
    """Infer compression, if available, from filename.

    Infer a named compression type, if registered and available, from filename
    extension. This includes builtin (gz, bz2, zip) compressions, as well as
    optional compressions. See fsspec.compression.register_compression.
    """
    extension = os.path.splitext(filename)[-1].strip(".").lower()
    if extension in compressions:
        return compressions[extension]
    return None


def build_name_function(max_int: float) -> Callable[[int], str]:
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

    def name_function(i: int) -> str:
        return str(i).zfill(pad_length)

    return name_function


def seek_delimiter(file: IO[bytes], delimiter: bytes, blocksize: int) -> bool:
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
    last: bytes | None = None
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


def read_block(
    f: IO[bytes],
    offset: int,
    length: int | None,
    delimiter: bytes | None = None,
    split_before: bool = False,
) -> bytes:
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
        found_start_delim = seek_delimiter(f, delimiter, 2**16)
        if length is None:
            return f.read()
        start = f.tell()
        length -= start - offset

        f.seek(start + length)
        found_end_delim = seek_delimiter(f, delimiter, 2**16)
        end = f.tell()

        # Adjust split location to before delimiter if seek found the
        # delimiter sequence, not start or end of file.
        if found_start_delim and split_before:
            start -= len(delimiter)

        if found_end_delim and split_before:
            end -= len(delimiter)

        offset = start
        length = end - start

    f.seek(offset)

    # TODO: allow length to be None and read to the end of the file?
    assert length is not None
    b = f.read(length)
    return b


def tokenize(*args: Any, **kwargs: Any) -> str:
    """Deterministic token

    (modified from dask.base)

    >>> tokenize([1, 2, '3'])
    '9d71491b50023b06fc76928e6eddb952'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs:
        args += (kwargs,)
    try:
        h = md5(str(args).encode())
    except ValueError:
        # FIPS systems: https://github.com/fsspec/filesystem_spec/issues/380
        h = md5(str(args).encode(), usedforsecurity=False)
    return h.hexdigest()


def stringify_path(filepath: str | os.PathLike[str] | pathlib.Path) -> str:
    """Attempt to convert a path-like object to a string.

    Parameters
    ----------
    filepath: object to be converted

    Returns
    -------
    filepath_str: maybe a string version of the object

    Notes
    -----
    Objects supporting the fspath protocol are coerced according to its
    __fspath__ method.

    For backwards compatibility with older Python version, pathlib.Path
    objects are specially coerced.

    Any other object is passed through unchanged, which includes bytes,
    strings, buffers, or anything else that's not even path-like.
    """
    if isinstance(filepath, str):
        return filepath
    elif hasattr(filepath, "__fspath__"):
        return filepath.__fspath__()
    elif hasattr(filepath, "path"):
        return filepath.path
    else:
        return filepath  # type: ignore[return-value]


def make_instance(
    cls: Callable[..., T], args: Sequence[Any], kwargs: dict[str, Any]
) -> T:
    inst = cls(*args, **kwargs)
    inst._determine_worker()  # type: ignore[attr-defined]
    return inst


def common_prefix(paths: Iterable[str]) -> str:
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


def other_paths(
    paths: list[str],
    path2: str | list[str],
    exists: bool = False,
    flatten: bool = False,
) -> list[str]:
    """In bulk file operations, construct a new file tree from a list of files

    Parameters
    ----------
    paths: list of str
        The input file tree
    path2: str or list of str
        Root to construct the new list in. If this is already a list of str, we just
        assert it has the right number of elements.
    exists: bool (optional)
        For a str destination, it is already exists (and is a dir), files should
        end up inside.
    flatten: bool (optional)
        Whether to flatten the input directory tree structure so that the output files
        are in the same directory.

    Returns
    -------
    list of str
    """

    if isinstance(path2, str):
        path2 = path2.rstrip("/")

        if flatten:
            path2 = ["/".join((path2, p.split("/")[-1])) for p in paths]
        else:
            cp = common_prefix(paths)
            if exists:
                cp = cp.rsplit("/", 1)[0]
            if not cp and all(not s.startswith("/") for s in paths):
                path2 = ["/".join([path2, p]) for p in paths]
            else:
                path2 = [p.replace(cp, path2, 1) for p in paths]
    else:
        assert len(paths) == len(path2)
    return path2


def is_exception(obj: Any) -> bool:
    return isinstance(obj, BaseException)


def isfilelike(f: Any) -> TypeGuard[IO[bytes]]:
    return all(hasattr(f, attr) for attr in ["read", "close", "tell"])


def get_protocol(url: str) -> str:
    url = stringify_path(url)
    parts = re.split(r"(\:\:|\://)", url, maxsplit=1)
    if len(parts) > 1:
        return parts[0]
    return "file"


def get_file_extension(url: str) -> str:
    url = stringify_path(url)
    # Only consider the final path component: a "." in a parent directory name
    # (e.g. "/path/to.dir/file") is not the file's extension.
    ext_parts = url.rsplit("/", 1)[-1].rsplit(".", 1)
    if len(ext_parts) > 1:
        return ext_parts[-1]
    return ""


def can_be_local(path: str) -> bool:
    """Can the given URL be used with open_local?"""
    from fsspec import get_filesystem_class

    try:
        return getattr(get_filesystem_class(get_protocol(path)), "local_file", False)
    except (ValueError, ImportError):
        # not in registry or import failed
        return False


def get_package_version_without_import(name: str) -> str | None:
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
    try:
        return version(name)
    except:  # noqa: E722
        pass
    try:
        import importlib

        mod = importlib.import_module(name)
        return mod.__version__
    except (ImportError, AttributeError):
        return None


def setup_logging(
    logger: logging.Logger | None = None,
    logger_name: str | None = None,
    level: str = "DEBUG",
    clear: bool = True,
) -> logging.Logger:
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


def _unstrip_protocol(name: str, fs: AbstractFileSystem) -> str:
    return fs.unstrip_protocol(name)


def mirror_from(
    origin_name: str, methods: Iterable[str]
) -> Callable[[type[T]], type[T]]:
    """Mirror attributes and methods from the given
    origin_name attribute of the instance to the
    decorated class"""

    def origin_getter(method: str, self: Any) -> Any:
        origin = getattr(self, origin_name)
        return getattr(origin, method)

    def wrapper(cls: type[T]) -> type[T]:
        for method in methods:
            wrapped_method = partial(origin_getter, method)
            setattr(cls, method, property(wrapped_method))
        return cls

    return wrapper


@contextlib.contextmanager
def nullcontext(obj: T) -> Iterator[T]:
    yield obj


def merge_offset_ranges(
    paths: list[str],
    starts: list[int | None] | int | None,
    ends: list[int | None] | int | None,
    max_gap: int = 0,
    max_block: int | None = None,
    sort: bool = True,
) -> tuple[list[str], list[int], list[int | None]]:
    """Merge adjacent byte-offset ranges when the inter-range
    gap is <= `max_gap`, and when the merged byte range does not
    exceed `max_block` (if specified). Every input range is covered by
    at least one returned range. Overlapping input ranges are merged
    where `max_block` allows it, so returned ranges may overlap once a
    chain of overlapping inputs reaches `max_block`; a single input
    range larger than `max_block` is still returned whole.

    An `end` of `None` means to the end of the file. By default, this
    function will re-order the input paths and byte ranges to ensure
    sorted order.

    Passing `sort=False` skips the re-ordering, which is only worthwhile
    when the inputs are already grouped by path and ascending by start
    within each path. Ranges that break that order still appear in the
    output, as their own range rather than merged, so coverage holds for
    any input order.
    """
    # Check input
    if not isinstance(paths, list):
        raise TypeError
    if not isinstance(starts, list):
        starts = [starts] * len(paths)
    if not isinstance(ends, list):
        ends = [ends] * len(paths)
    if len(starts) != len(paths) or len(ends) != len(paths):
        raise ValueError

    starts_i: list[int] = [s or 0 for s in starts]
    ends_i: list[int | None] = ends

    # Early Return
    if len(starts_i) <= 1:
        return paths, starts_i, ends_i

    # Sort by paths and then ranges if `sort=True`
    if sort:
        ranges = sorted(
            zip(paths, starts_i, ends_i),
            # None end sorts last (covers furthest into the file)
            key=lambda pse: (pse[0], pse[1], math.inf if pse[2] is None else pse[2]),
        )
        paths = [r[0] for r in ranges]
        starts_i = [r[1] for r in ranges]
        ends_i = [r[2] for r in ranges]

    # Loop through the coupled `paths`, `starts`, and
    # `ends`, and merge adjacent blocks when appropriate
    new_paths = paths[:1]
    new_starts = starts_i[:1]
    new_ends = ends_i[:1]
    for path, start, end in zip(paths[1:], starts_i[1:], ends_i[1:]):
        prev_end = new_ends[-1]
        if path != new_paths[-1]:
            # Cannot merge with previous block
            new_paths.append(path)
            new_starts.append(start)
            new_ends.append(end)
        elif start < new_starts[-1]:
            # Out of order (only possible when `sort=False`). Walking the
            # current block start backwards would uncover bytes already
            # attributed to it, so give this range its own block
            new_paths.append(path)
            new_starts.append(start)
            new_ends.append(end)
        elif prev_end is None:
            # Previous block already covers the rest of the file
            continue
        elif start < prev_end:
            # Overlap / nested
            if end is not None and end <= prev_end:
                # Already covered by the current block
                continue
            elif (
                end is not None
                and max_block is not None
                and (end - new_starts[-1]) > max_block
            ):
                # Extending would exceed `max_block`. Start a new block,
                # which overlaps the previous one, rather than letting a
                # chain of overlaps grow the block without bound
                new_paths.append(path)
                new_starts.append(start)
                new_ends.append(end)
            else:
                # An `end` of None extends the block to EOF; a separate
                # block would subsume the current one anyway
                new_ends[-1] = end
        elif (start - prev_end) > max_gap or (
            max_block is not None
            and (end is None or (end - new_starts[-1]) > max_block)
        ):
            # Gap too large, or merging would exceed `max_block`
            new_paths.append(path)
            new_starts.append(start)
            new_ends.append(end)
        else:
            # Merge with the previous block
            new_ends[-1] = end

    return new_paths, new_starts, new_ends


def file_size(filelike: IO[bytes]) -> int:
    """Find length of any open read-mode file-like"""
    pos = filelike.tell()
    try:
        return filelike.seek(0, 2)
    finally:
        filelike.seek(pos)


@contextlib.contextmanager
def atomic_write(path: str, mode: str = "wb"):
    """
    A context manager that opens a temporary file next to `path` and, on exit,
    replaces `path` with the temporary file, thereby updating `path`
    atomically.
    """
    fd, fn = tempfile.mkstemp(
        dir=os.path.dirname(path), prefix=os.path.basename(path) + "-"
    )
    try:
        with open(fd, mode) as fp:
            yield fp
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(fn)
        raise
    else:
        os.replace(fn, path)


def _translate(pat, STAR, QUESTION_MARK):
    # Copied from: https://github.com/python/cpython/pull/106703.
    res: list[str] = []
    add = res.append
    i, n = 0, len(pat)
    while i < n:
        c = pat[i]
        i = i + 1
        if c == "*":
            # compress consecutive `*` into one
            if (not res) or res[-1] is not STAR:
                add(STAR)
        elif c == "?":
            add(QUESTION_MARK)
        elif c == "[":
            j = i
            if j < n and pat[j] == "!":
                j = j + 1
            if j < n and pat[j] == "]":
                j = j + 1
            while j < n and pat[j] != "]":
                j = j + 1
            if j >= n:
                add("\\[")
            else:
                stuff = pat[i:j]
                if "-" not in stuff:
                    stuff = stuff.replace("\\", r"\\")
                else:
                    chunks = []
                    k = i + 2 if pat[i] == "!" else i + 1
                    while True:
                        k = pat.find("-", k, j)
                        if k < 0:
                            break
                        chunks.append(pat[i:k])
                        i = k + 1
                        k = k + 3
                    chunk = pat[i:j]
                    if chunk:
                        chunks.append(chunk)
                    else:
                        chunks[-1] += "-"
                    # Remove empty ranges -- invalid in RE.
                    for k in range(len(chunks) - 1, 0, -1):
                        if chunks[k - 1][-1] > chunks[k][0]:
                            chunks[k - 1] = chunks[k - 1][:-1] + chunks[k][1:]
                            del chunks[k]
                    # Escape backslashes and hyphens for set difference (--).
                    # Hyphens that create ranges shouldn't be escaped.
                    stuff = "-".join(
                        s.replace("\\", r"\\").replace("-", r"\-") for s in chunks
                    )
                # Escape set operations (&&, ~~ and ||).
                stuff = re.sub(r"([&~|])", r"\\\1", stuff)
                i = j + 1
                if not stuff:
                    # Empty range: never match.
                    add("(?!)")
                elif stuff == "!":
                    # Negated empty range: match any character.
                    add(".")
                else:
                    if stuff[0] == "!":
                        stuff = "^" + stuff[1:]
                    elif stuff[0] in ("^", "["):
                        stuff = "\\" + stuff
                    add(f"[{stuff}]")
        else:
            add(re.escape(c))
    assert i == n
    return res


def glob_translate(pat):
    # Copied from: https://github.com/python/cpython/pull/106703.
    # The keyword parameters' values are fixed to:
    # recursive=True, include_hidden=True, seps=None
    """Translate a pathname with shell wildcards to a regular expression."""
    if os.path.altsep:
        seps = os.path.sep + os.path.altsep
    else:
        seps = os.path.sep
    escaped_seps = "".join(map(re.escape, seps))
    any_sep = f"[{escaped_seps}]" if len(seps) > 1 else escaped_seps
    not_sep = f"[^{escaped_seps}]"
    one_last_segment = f"{not_sep}+"
    one_segment = f"{one_last_segment}{any_sep}"
    any_segments = f"(?:.+{any_sep})?"
    any_last_segments = ".*"
    results = []
    parts = re.split(any_sep, pat)
    last_part_idx = len(parts) - 1
    for idx, part in enumerate(parts):
        if part == "*":
            results.append(one_segment if idx < last_part_idx else one_last_segment)
            continue
        if part == "**":
            results.append(any_segments if idx < last_part_idx else any_last_segments)
            continue
        elif "**" in part:
            raise ValueError(
                "Invalid pattern: '**' can only be an entire path component"
            )
        if part:
            results.extend(_translate(part, f"{not_sep}*", not_sep))
        if idx < last_part_idx:
            results.append(any_sep)
    res = "".join(results)
    return rf"(?s:{res})\Z"
