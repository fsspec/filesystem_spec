from __future__ import absolute_import, division, print_function

import io
import logging
import os
import re
from glob import has_magic

# for backwards compat, we export cache things from here too
from .caching import (  # noqa: F401
    BaseCache,
    BlockCache,
    BytesCache,
    MMapCache,
    ReadAheadCache,
    caches,
)
from .compression import compr
from .registry import filesystem, get_filesystem_class
from .utils import (
    build_name_function,
    infer_compression,
    stringify_path,
    update_storage_options,
)

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
                self.newline,
            ),
        )

    def __repr__(self):
        return "<OpenFile '{}'>".format(self.path)

    def __fspath__(self):
        # may raise if cannot be resolved to local file
        return self.open().__fspath__()

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
        self.fobjects.clear()  # may cause cleanup of objects and close files

    def open(self):
        """Materialise this as a real open file without context

        The file should be explicitly closed to avoid enclosed file
        instances persisting. This code-path monkey-patches the file-like
        objects, so they can close even if the parent OpenFile object has already
        been deleted; but a with-context is better style.
        """
        out = self.__enter__()
        closer = out.close
        fobjects = self.fobjects.copy()[:-1]
        mode = self.mode

        def close():
            # this func has no reference to
            closer()  # original close bound method of the final file-like
            _close(fobjects, mode)  # call close on other dependent file-likes

        out.close = close
        return out

    def close(self):
        """Close all encapsulated file objects"""
        _close(self.fobjects, self.mode)


class OpenFiles(list):
    """List of OpenFile instances

    Can be used in a single context, which opens and closes all of the
    contained files. Normal list access to get the elements works as
    normal.

    A special case is made for caching filesystems - the files will
    be down/uploaded together at the start or end of the context, and
    this may happen concurrently, if the target filesystem supports it.
    """

    def __init__(self, *args, mode="rb", fs=None):
        self.mode = mode
        self.fs = fs
        self.files = []
        super().__init__(*args)

    def __enter__(self):
        if self.fs is None:
            raise ValueError("Context has already been used")

        fs = self.fs
        while True:
            if hasattr(fs, "open_many"):
                # check for concurrent cache download; or set up for upload
                self.files = fs.open_many(self)
                return self.files
            if hasattr(fs, "fs") and fs.fs is not None:
                fs = fs.fs
            else:
                break
        return [s.__enter__() for s in self]

    def __exit__(self, *args):
        fs = self.fs
        if "r" not in self.mode:
            while True:
                if hasattr(fs, "open_many"):
                    # check for concurrent cache upload
                    fs.commit_many(self.files)
                    self.files.clear()
                    return
                if hasattr(fs, "fs") and fs.fs is not None:
                    fs = fs.fs
                else:
                    break
        [s.__exit__(*args) for s in self]

    def __repr__(self):
        return "<List of %s OpenFile instances>" % len(self)


def _close(fobjects, mode):
    for f in reversed(fobjects):
        if "r" not in mode and not f.closed:
            f.flush()
        f.close()
    fobjects.clear()


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
    auto_mkdir=True,
    expand=True,
    **kwargs,
):
    """Given a path or paths, return a list of ``OpenFile`` objects.

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
    auto_mkdir: bool (True)
        If in write mode, this will ensure the target directory exists before
        writing, by calling ``fs.mkdirs(exist_ok=True)``.
    expand: bool
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Examples
    --------
    >>> files = open_files('2015-*-*.csv')  # doctest: +SKIP
    >>> files = open_files(
    ...     's3://bucket/2015-*-*.csv.gz', compression='gzip'
    ... )  # doctest: +SKIP

    Returns
    -------
    An ``OpenFiles`` instance, which is a ist of ``OpenFile`` objects that can
    be used as a single context
    """
    fs, fs_token, paths = get_fs_token_paths(
        urlpath,
        mode,
        num=num,
        name_function=name_function,
        storage_options=kwargs,
        protocol=protocol,
        expand=expand,
    )
    if "r" not in mode and auto_mkdir:
        parents = {fs._parent(path) for path in paths}
        [fs.makedirs(parent, exist_ok=True) for parent in parents]
    return OpenFiles(
        [
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
        ],
        mode=mode,
        fs=fs,
    )


def _un_chain(path, kwargs):
    if isinstance(path, (tuple, list)):
        bits = [_un_chain(p, kwargs) for p in path]
        out = []
        for pbit in zip(*bits):
            paths, protocols, kwargs = zip(*pbit)
            if len(set(protocols)) > 1:
                raise ValueError("Protocol mismatch in URL chain")
            if len(set(paths)) == 1:
                paths = paths[0]
            else:
                paths = list(paths)
            out.append([paths, protocols[0], kwargs[0]])
        return out
    x = re.compile(".*[^a-z]+.*")  # test for non protocol-like single word
    bits = (
        [p if "://" in p or x.match(p) else p + "://" for p in path.split("::")]
        if "::" in path
        else [path]
    )
    if len(bits) < 2:
        return []
    # [[url, protocol, kwargs], ...]
    out = []
    previous_bit = None
    for bit in reversed(bits):
        protocol = split_protocol(bit)[0] or "file"
        cls = get_filesystem_class(protocol)
        extra_kwargs = cls._get_kwargs_from_urls(bit)
        kws = kwargs.get(protocol, {})
        kw = dict(**extra_kwargs, **kws)
        bit = cls._strip_protocol(bit)
        if (
            protocol in {"blockcache", "filecache", "simplecache"}
            and "target_protocol" not in kw
        ):
            bit = previous_bit
        out.append((bit, protocol, kw))
        previous_bit = bit
    out = list(reversed(out))
    return out


def url_to_fs(url, **kwargs):
    """Turn fully-qualified and potentially chained URL into filesystem instance"""
    chain = _un_chain(url, kwargs)
    if len(chain) > 1:
        inkwargs = {}
        # Reverse iterate the chain, creating a nested target_* structure
        for i, ch in enumerate(reversed(chain)):
            urls, protocol, kw = ch
            if i == len(chain) - 1:
                inkwargs = dict(**kw, **inkwargs)
                continue
            inkwargs["target_options"] = dict(**kw, **inkwargs)
            inkwargs["target_protocol"] = protocol
            inkwargs["fo"] = urls
        urlpath, protocol, _ = chain[0]
        fs = filesystem(protocol, **inkwargs)
    else:
        protocol = split_protocol(url)[0]
        cls = get_filesystem_class(protocol)

        options = cls._get_kwargs_from_urls(url)
        urlpath = cls._strip_protocol(url)
        update_storage_options(options, kwargs)
        fs = cls(**options)
        urlpath = fs._strip_protocol(url)
    return fs, urlpath


def open(
    urlpath,
    mode="rb",
    compression=None,
    encoding="utf8",
    errors=None,
    protocol=None,
    newline=None,
    **kwargs,
):
    """Given a path or paths, return one ``OpenFile`` object.

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
    >>> openfile = open(
    ...     's3://bucket/2015-01-01.csv.gz', compression='gzip'
    ... )  # doctest: +SKIP
    >>> with openfile as f:
    ...     df = pd.read_csv(f)  # doctest: +SKIP
    ...

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
        expand=False,
        **kwargs,
    )[0]


def open_local(url, mode="rb", **storage_options):
    """Open file(s) which can be resolved to local

    For files which either are local, or get downloaded upon open
    (e.g., by file caching)

    Parameters
    ----------
    url: str or list(str)
    mode: str
        Must be read mode
    storage_options:
        passed on to FS for or used by open_files (e.g., compression)
    """
    if "r" not in mode:
        raise ValueError("Can only ensure local files when reading")
    of = open_files(url, mode=mode, **storage_options)
    if not getattr(of[0].fs, "local_file", False):
        raise ValueError(
            "open_local can only be used on a filesystem which"
            " has attribute local_file=True"
        )
    with of as files:
        paths = [f.name for f in files]
    if isinstance(url, str) and not has_magic(url):
        return paths[0]
    return paths


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
        raise ValueError("When writing data, only one filename mask can be specified.")
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
    urlpath,
    mode="rb",
    num=1,
    name_function=None,
    storage_options=None,
    protocol=None,
    expand=True,
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
    expand: bool
        Expand string paths for writing, assuming the path is a directory
    """
    if isinstance(urlpath, (list, tuple, set)):
        if not urlpath:
            raise ValueError("empty urlpath sequence")
        urlpath = [stringify_path(u) for u in urlpath]
    else:
        urlpath = stringify_path(urlpath)
    chain = _un_chain(urlpath, storage_options or {})
    if len(chain) > 1:
        inkwargs = {}
        # Reverse iterate the chain, creating a nested target_* structure
        for i, ch in enumerate(reversed(chain)):
            urls, nested_protocol, kw = ch
            if i == len(chain) - 1:
                inkwargs = dict(**kw, **inkwargs)
                continue
            inkwargs["target_options"] = dict(**kw, **inkwargs)
            inkwargs["target_protocol"] = nested_protocol
            inkwargs["fo"] = urls
        paths, protocol, _ = chain[0]
        fs = filesystem(protocol, **inkwargs)
        if isinstance(paths, (list, tuple, set)):
            paths = [fs._strip_protocol(u) for u in paths]
        else:
            paths = fs._strip_protocol(paths)
    else:
        if isinstance(urlpath, (list, tuple, set)):
            protocols, paths = zip(*map(split_protocol, urlpath))
            if protocol is None:
                protocol = protocols[0]
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
        else:
            protocols = split_protocol(urlpath)[0]
            protocol = protocol or protocols
            cls = get_filesystem_class(protocol)
            options = cls._get_kwargs_from_urls(urlpath)
            paths = cls._strip_protocol(urlpath)
            update_storage_options(options, storage_options)
            fs = cls(**options)

    if isinstance(paths, (list, tuple, set)):
        paths = expand_paths_if_needed(paths, mode, num, fs, name_function)
    else:
        if "w" in mode and expand:
            paths = _expand_paths(paths, name_function, num)
        elif "*" in paths:
            paths = [f for f in sorted(fs.glob(paths)) if not fs.isdir(f)]
        else:
            paths = [paths]

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
