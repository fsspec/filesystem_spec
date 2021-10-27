import io
import json
import warnings

from .core import url_to_fs
from .utils import merge_offset_ranges

# Parquet-Specific Utilities for fsspec
#
# Most of the functions defined in this module are NOT
# intended for public consumption. The only exception
# to this is `open_parquet_file`, which should be used
# place of `fs.open()` to open parquet-formatted files
# on remote file systems.


def open_parquet_file(
    path,
    fs=None,
    columns=None,
    row_groups=None,
    storage_options=None,
    engine="auto",
    max_gap=64_000,
    max_block=256_000_000,
    footer_sample_size=1_000_000,
    **kwargs,
):
    """
    Return a file-like object for a single Parquet file.

    The specified parquet `engine` will be used to parse the
    footer metadata, and determine the required byte ranges
    from the file. The target path will then be opened with
    the "parts" (`KnownPartsOfAFile`) caching strategy.

    Note that this method is intended for usage with remote
    file systems, and is unlikely to improve parquet-read
    performance on local file systems.

    Parameters
    ----------
    path: str
        Target file path.
    fs: AbstractFileSystem, optional
        Filesystem object to use for opening the file. If nothing is
        specified, an `AbstractFileSystem` object will be inferred.
    engine : str, default "auto"
        Parquet engine to use for metadata parsing. Allowed options
        include "fastparquet", "pyarrow", and "auto". The specified
        engine must be installed in the current environment. If
        "auto" is specified, and both engines are installed,
        "fastparquet" will take precedence over "pyarrow".
    columns: list, optional
        List of all column names that may be read from the file.
    row_groups : list, optional
        List of all row-group indices that may be read from the file.
    storage_options : dict, optional
        Used to generate an `AbstractFileSystem` object if `fs` was
        not specified.
    max_gap : int, optional
        Neighboring byte ranges will only be merged when their
        inter-range gap is <= `max_gap`. Default is 64KB.
    max_block : int, optional
        Neighboring byte ranges will only be merged when the size of
        the aggregated range is <= `max_block`. Default is 256MB.
    footer_sample_size : int, optional
        Number of bytes to read from the end of the path to look
        for the footer metadata. If the sampled bytes do not contain
        the footer, a second read request will be required, and
        performance will suffer. Default is 1MB.
    **kwargs :
        Optional key-word arguments to pass to `fs.open`
    """

    # Make sure we have an `AbstractFileSystem` object
    # to work with
    if fs is None:
        fs = url_to_fs(path, storage_options=(storage_options or {}))[0]

    # Fetch the known byte ranges needed to read
    # `columns` and/or `row_groups`
    data = _get_parquet_byte_ranges(
        [path],
        fs,
        columns=columns,
        row_groups=row_groups,
        engine=engine,
        max_gap=max_gap,
        max_block=max_block,
        footer_sample_size=footer_sample_size,
    )

    # Call self.open with "parts" caching
    options = kwargs.pop("cache_options", {}).copy()
    return fs.open(
        path,
        mode="rb",
        cache_type="parts",
        cache_options={**options, **{"data": data[path]}},
        **kwargs,
    )


def _get_parquet_byte_ranges(
    paths,
    fs,
    columns=None,
    row_groups=None,
    max_gap=64_000,
    max_block=256_000_000,
    footer_sample_size=1_000_000,
    engine="auto",
):
    """Get a dictionary of the known byte ranges needed
    to read a specific column/row-group selection from a
    Parquet dataset. Each value in the output dictionary
    is intended for use as the `data` argument for the
    `KnownPartsOfAFile` caching strategy of a single path.
    """

    # Set the engine
    engine = _set_engine(engine)

    # Get file sizes asynchronously
    file_sizes = fs.sizes(paths)

    # Populate global paths, starts, & ends
    result = {}
    data_paths = []
    data_starts = []
    data_ends = []
    add_header_magic = True
    if columns is None and row_groups is None:
        # We are NOT selecting specific columns or row-groups.
        #
        # We can avoid sampling the footers, and just transfer
        # all file data with cat_ranges
        for i, path in enumerate(paths):
            result[path] = {}
            for b in range(0, file_sizes[i], max_block):
                data_paths.append(path)
                data_starts.append(b)
                data_ends.append(min(b + max_block, file_sizes[i]))
        add_header_magic = False  # "Magic" should already be included
    else:
        # We ARE selecting specific columns or row-groups.
        #
        # Gather file footers.
        # We just take the last `footer_sample_size` bytes of each
        # file (or the entire file if it is smaller than that)
        footer_starts = []
        footer_ends = []
        for i, path in enumerate(paths):
            footer_ends.append(file_sizes[i])
            sample_size = max(0, file_sizes[i] - footer_sample_size)
            footer_starts.append(sample_size)
        footer_samples = fs.cat_ranges(paths, footer_starts, footer_ends)

        # Check our footer samples and re-sample if necessary.
        missing_footer_starts = footer_starts.copy()
        missing_footer_ends = footer_starts.copy()
        large_footer = 0
        for i, path in enumerate(paths):
            footer_size = int.from_bytes(footer_samples[i][-8:-4], "little")
            real_footer_start = file_sizes[i] - (footer_size + 8)
            if real_footer_start < footer_starts[i]:
                missing_footer_starts[i] = real_footer_start
                missing_footer_ends[i] = footer_starts[i]
                large_footer = max(large_footer, (footer_size + 8))
        if large_footer:
            warnings.warn(
                f"Not enough data was used to sample the parquet footer. "
                f"Try setting footer_sample_size >= {large_footer}."
            )
            for i, block in enumerate(
                fs.cat_ranges(
                    paths,
                    missing_footer_starts,
                    missing_footer_ends,
                )
            ):
                footer_samples[i] = block + footer_samples[i]

        # Calculate required byte ranges for each path
        for i, path in enumerate(paths):

            # Deal with small-file case.
            # Just include all remaining bytes of the file
            # in a single range.
            if file_sizes[i] < max_block:
                if footer_starts[i] > 0:
                    # Only need to transfer the data if the
                    # footer sample isn't already the whole file
                    data_paths.append(path)
                    data_starts.append(0)
                    data_ends.append(footer_starts[i])
                continue

            # Use "engine" to collect data byte ranges
            path_data_starts, path_data_ends = engine._parquet_byte_ranges(
                footer_samples[i], columns, row_groups, footer_starts[i]
            )

            data_paths += [path] * len(path_data_starts)
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

    # Add b"" for reads beyond end of file
    for i, path in enumerate(paths):
        result[path][(file_sizes[i], 2 * file_sizes[i])] = b""

    return result


def _set_engine(engine_str):

    # Define a list of parquet engines to try
    if engine_str == "auto":
        try_engines = ("fastparquet", "pyarrow")
    elif not isinstance(engine_str, str):
        raise ValueError(
            "Failed to set parquet engine! "
            "Please pass 'fastparquet', 'pyarrow', or 'auto'"
        )
    elif engine_str not in ("fastparquet", "pyarrow"):
        raise ValueError(f"{engine_str} engine not supported by `fsspec.parquet`")
    else:
        try_engines = [engine_str]

    # Try importing the engines in `try_engines`,
    # and choose the first one that succeeds
    for engine in try_engines:
        try:
            if engine == "fastparquet":
                return FastparquetEngine()
            elif engine == "pyarrow":
                return PyarrowEngine()
        except ImportError:
            pass

    # Raise an error if a supported parquet engine
    # was not found
    raise ImportError(
        f"The following parquet engines are not installed "
        f"in your python environment: {try_engines}."
        f"Please install 'fastparquert' or 'pyarrow' to "
        f"utilize the `fsspec.parquet` module."
    )


class FastparquetEngine:

    # The purpose of the FastparquetEngine class is
    # to check if fastparquet can be imported (on initialization)
    # and to define a `_parquet_byte_ranges` method. In the
    # future, this class may also be used to define other
    # methods/logic that are specific to fastparquet.

    def __init__(self):
        import fastparquet as fp

        self.fp = fp

    def _parquet_byte_ranges(self, footer, columns, row_groups, footer_start):
        data_starts, data_ends = [], []
        pf = self.fp.ParquetFile(io.BytesIO(footer))

        # Convert columns to a set and add any index columns
        # speficied in the pandas metadata (just in case)
        column_set = None if columns is None else set(columns)
        if column_set is not None and hasattr(pf, "pandas_metadata"):
            column_set |= set(pf.pandas_metadata.get("index_columns", []))

        # Loop through column chunks to add required byte ranges
        for r, row_group in enumerate(pf.row_groups):
            # Skip this row-group if we are targetting
            # specific row-groups
            if row_groups is None or r in row_groups:
                for column in row_group.columns:
                    name = column.meta_data.path_in_schema[0]
                    # Skip this column if we are targetting a
                    # specific columns
                    if column_set is None or name in column_set:
                        file_offset0 = column.meta_data.dictionary_page_offset
                        if file_offset0 is None:
                            file_offset0 = column.meta_data.data_page_offset
                        num_bytes = column.meta_data.total_compressed_size
                        if file_offset0 < footer_start:
                            data_starts.append(file_offset0)
                            data_ends.append(
                                min(file_offset0 + num_bytes, footer_start)
                            )
        return data_starts, data_ends


class PyarrowEngine:

    # The purpose of the PyarrowEngine class is
    # to check if pyarrow can be imported (on initialization)
    # and to define a `_parquet_byte_ranges` method. In the
    # future, this class may also be used to define other
    # methods/logic that are specific to pyarrow.

    def __init__(self):
        import pyarrow.parquet as pq

        self.pq = pq

    def _parquet_byte_ranges(self, footer, columns, row_groups, footer_start):
        data_starts, data_ends = [], []
        md = self.pq.ParquetFile(io.BytesIO(footer)).metadata

        # Convert columns to a set and add any index columns
        # speficied in the pandas metadata (just in case)
        column_set = None if columns is None else set(columns)
        if column_set is not None:
            schema = md.schema.to_arrow_schema()
            has_pandas_metadata = (
                schema.metadata is not None and b"pandas" in schema.metadata
            )
            if has_pandas_metadata:
                column_set |= set(
                    json.loads(schema.metadata[b"pandas"].decode("utf8")).get(
                        "index_columns", []
                    )
                )

        # Loop through column chunks to add required byte ranges
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
                    split_name = name.split(".")[0]
                    if (
                        column_set is None
                        or name in column_set
                        or split_name in column_set
                    ):
                        file_offset0 = column.dictionary_page_offset
                        if file_offset0 is None:
                            file_offset0 = column.data_page_offset
                        num_bytes = column.total_compressed_size
                        if file_offset0 < footer_start:
                            data_starts.append(file_offset0)
                            data_ends.append(
                                min(file_offset0 + num_bytes, footer_start)
                            )
        return data_starts, data_ends
