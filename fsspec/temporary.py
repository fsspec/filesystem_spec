from .core import OpenFile, OpenFiles


__all__ = (
  'TemporaryFile',
  'open_temporary', 'open_temporary_files'
)


class TemporaryOpenFile(OpenFile):
        
    def close(self):
        super().close()
        self.fs.rm(self.path)


def open_temporary_files(
    urlpath,
    mode="wb",
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
    """Given a path or paths, return a list of ``TemporaryOpenFile`` objects removed on .close().
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
    An ``OpenFiles`` instance, which is a list of ``TemporaryOpenFile`` objects that can
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
    if auto_mkdir:
        parents = {fs._parent(path) for path in paths}
        [fs.makedirs(parent, exist_ok=True) for parent in parents]
    return OpenFiles(
        [
            TemporaryFile(
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
  
  
  def open_temporary(
    urlpath,
    mode="wb",
    compression=None,
    encoding="utf8",
    errors=None,
    protocol=None,
    newline=None,
    **kwargs,
):
    """Given a path or paths, return one ``TemporaryOpenFile`` object removed on .close().
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
    ``TemporaryOpenFile`` object.
    """
    return open_temporary_files(
        urlpath=[urlpath],
        mode=mode,
        compression=compression,
        encoding=encoding,
        errors=errors,
        protocol=protocol,
        newline=newline,
        expand=False,
        **kwargs,
    )[0]
