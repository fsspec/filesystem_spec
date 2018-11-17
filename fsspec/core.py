from __future__ import print_function, division, absolute_import

import io
import os
import logging
from .compression import compr
from .utils import (infer_compression, build_name_function,
                    update_storage_options)
from .registry import get_filesystem_class
logger = logging.getLogger('fsspec')


class OpenFile(object):
    """
    File-like object to be used in a context

    These instances are safe to serialize, as the low-level file object
    is not created until invoked using `with`.

    Parameters
    ----------
    fs : FileSystem
        The file system to use for opening the file. Should match the interface
        of ``dask.bytes.local.LocalFileSystem``.
    path : str
        Location to open
    mode : str like 'rb', optional
        Mode of the opened file
    compression : str or None, optional
        Compression to apply
    encoding : str or None, optional
        The encoding to use if opened in text mode.
    errors : str or None, optional
        How to handle encoding errors if opened in text mode.
    """
    def __init__(self, fs, path, mode='rb', compression=None, encoding=None,
                 errors=None):
        self.fs = fs
        self.path = path
        self.mode = mode
        self.compression = get_compression(path, compression)
        self.encoding = encoding
        self.errors = errors
        self.fobjects = []

    def __reduce__(self):
        return (OpenFile, (self.fs, self.path, self.mode, self.compression,
                           self.encoding, self.errors))

    def __repr__(self):
        return f"<OpenFile '{self.path}'>"

    def __enter__(self):
        mode = self.mode.replace('t', '').replace('b', '') + 'b'

        f = self.fs.open(self.path, mode=mode)

        fobjects = [f]

        if self.compression is not None:
            compress = compr[self.compression]
            f = compress(f, mode=mode[0])
            fobjects.append(f)

        if 't' in self.mode:
            f = io.TextIOWrapper(f, encoding=self.encoding,
                                 errors=self.errors)
            fobjects.append(f)

        self.fobjects = fobjects
        return f

    def __exit__(self, *args):
        self.close()

    def close(self):
        """Close all encapsulated file objects"""
        for f in reversed(self.fobjects):
            f.close()
        self.fobjects = []


def open_files(urlpath, mode='rb', compression=None, encoding='utf8',
               errors=None, name_function=None, num=1, **kwargs):
    """ Given a path or paths, return a list of ``OpenFile`` objects.

    Parameters
    ----------
    urlpath : string or list
        Absolute or relative filepath(s). Prefix with a protocol like ``s3://``
        to read from alternative filesystems. To read from multiple files you
        can pass a globstring or a list of paths, with the caveat that they
        must all have the same protocol.
    mode : 'rb', 'wt', etc.
    compression : string
        Compression to use.  See ``dask.bytes.compression.files`` for options.
    encoding : str
        For text mode only
    errors : None or str
        Passed to TextIOWrapper in text mode
    name_function : function or None
        if opening a set of files for writing, those files do not yet exist,
        so we need to generate their names by formatting the urlpath for
        each sequence number
    num : int [1]
        if writing mode, number of files we expect to create (passed to
        name+function)
    **kwargs : dict
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
    fs, fs_token, paths = get_fs_token_paths(urlpath, mode, num=num,
                                             name_function=name_function,
                                             storage_options=kwargs)
    return [OpenFile(fs, path, mode=mode, compression=compression,
                     encoding=encoding, errors=errors)
            for path in paths]


def get_compression(urlpath, compression):
    if compression == 'infer':
        compression = infer_compression(urlpath)
    if compression is not None and compression not in compr:
        raise ValueError("Compression type %s not supported" % compression)
    return compression


def split_protocol(urlpath):
    if "://" in urlpath:
        return urlpath.split("://", 1)
    return None, urlpath


def expand_paths_if_needed(paths, mode, num, fs, name_function):
    """Expand paths if they have a ``*`` in them.

    :param paths: list of paths
    mode : str
        Mode in which to open files.
    num : int
        If opening in writing mode, number of files we expect to create.
    fs : filesystem object
    name_function : callable
        If opening in writing mode, this callable is used to generate path
        names. Names are generated for each partition by
        ``urlpath.replace('*', name_function(partition_index))``.
    :return: list of paths
    """
    expanded_paths = []
    paths = list(paths)
    if 'w' in mode and sum([1 for p in paths if '*' in p]) > 1:
        raise ValueError("When writing data, only one filename mask can "
                         "be specified.")
    for curr_path in paths:
        if '*' in curr_path:
            if 'w' in mode:
                # expand using name_function
                expanded_paths.extend(
                    _expand_paths(curr_path, name_function, num))
            else:
                # expand using glob
                expanded_paths.extend(fs.glob(curr_path))
        else:
            expanded_paths.append(curr_path)
    # if we generated more paths that asked for, trim the list
    if 'w' in mode and len(expanded_paths) > num:
        expanded_paths = expanded_paths[:num]
    return expanded_paths


def get_fs_token_paths(urlpath, mode='rb', num=1, name_function=None,
                       storage_options=None):
    """Filesystem, deterministic token, and paths from a urlpath and options.

    Parameters
    ----------
    urlpath : string or iterable
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    mode : str, optional
        Mode in which to open files.
    num : int, optional
        If opening in writing mode, number of files we expect to create.
    name_function : callable, optional
        If opening in writing mode, this callable is used to generate path
        names. Names are generated for each partition by
        ``urlpath.replace('*', name_function(partition_index))``.
    storage_options : dict, optional
        Additional keywords to pass to the filesystem class.
    """
    if isinstance(urlpath, (list, tuple)):
        if not urlpath:
            raise ValueError("empty urlpath sequence")
        protocols, paths = zip(*map(split_protocol, urlpath))
        protocol = protocols[0]
        if not all(p == protocol for p in protocols):
            raise ValueError("When specifying a list of paths, all paths must "
                             "share the same protocol")
        cls = get_filesystem_class(protocol)
        options = cls._get_kwargs_from_urls(paths)
        update_storage_options(options, storage_options)
        fs = cls(**options)
        paths = expand_paths_if_needed(paths, mode, num, fs, name_function)

    elif isinstance(urlpath, str) or hasattr(urlpath, 'name'):
        protocol, path = split_protocol(urlpath)
        cls = get_filesystem_class(protocol)

        path = cls._strip_protocol(urlpath)
        options = cls._get_kwargs_from_urls(urlpath)
        update_storage_options(options, storage_options)
        fs = cls(**options)

        if 'w' in mode:
            paths = _expand_paths(path, name_function, num)
        elif "*" in path:
            paths = sorted(fs.glob(path))
        else:
            paths = [path]

    else:
        raise TypeError('url type not understood: %s' % urlpath)

    return fs, fs.token, paths


def _expand_paths(path, name_function, num):
    if isinstance(path, str):
        if path.count('*') > 1:
            raise ValueError("Output path spec must contain at most one '*'.")
        elif '*' not in path:
            path = os.path.join(path, '*.part')

        if name_function is None:
            name_function = build_name_function(num - 1)

        paths = [path.replace('*', name_function(i)) for i in range(num)]
        if paths != sorted(paths):
            logger.warning("In order to preserve order between partitions"
                           " paths created with ``name_function`` should "
                           "sort to partition order")
    elif isinstance(path, (tuple, list)):
        assert len(path) == num
        paths = list(path)
    else:
        raise ValueError("Path should be either\n"
                         "1. A list of paths: ['foo.json', 'bar.json', ...]\n"
                         "2. A directory: 'foo/\n"
                         "3. A path with a '*' in it: 'foo.*.json'")
    return paths
