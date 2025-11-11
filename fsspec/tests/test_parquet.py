import os

import pytest

try:
    import fastparquet
except ImportError:
    fastparquet = None
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

from fsspec.core import url_to_fs
from fsspec.parquet import _get_parquet_byte_ranges, open_parquet_file

# Define `engine` fixture
FASTPARQUET_MARK = pytest.mark.skipif(not fastparquet, reason="fastparquet not found")
PYARROW_MARK = pytest.mark.skipif(not pq, reason="pyarrow not found")


@pytest.fixture(
    params=[
        pytest.param("fastparquet", marks=FASTPARQUET_MARK),
        pytest.param("pyarrow", marks=PYARROW_MARK),
    ]
)
def engine(request):
    return request.param


@pytest.mark.filterwarnings("ignore:.*Not enough data.*")
@pytest.mark.parametrize("columns", [None, ["x"], ["x", "y"], ["z"]])
@pytest.mark.parametrize("max_gap", [0, 64])
@pytest.mark.parametrize("max_block", [64, 256_000_000])
@pytest.mark.parametrize("footer_sample_size", [8, 1_000])
@pytest.mark.parametrize("range_index", [True, False])
def test_open_parquet_file(
    tmpdir, engine, columns, max_gap, max_block, footer_sample_size, range_index
):
    # Pandas required for this test
    pd = pytest.importorskip("pandas")
    if engine != "fastparquet":
        return
    if columns == ["z"] and engine == "fastparquet":
        columns = ["z.a"]  # fastparquet is more specific

    # Write out a simple DataFrame
    path = os.path.join(str(tmpdir), "test.parquet")
    nrows = 40
    df = pd.DataFrame(
        {
            "x": [i * 7 % 5 for i in range(nrows)],
            "y": [[0, i] for i in range(nrows)],  # list
            "z": [{"a": i, "b": "cat"} for i in range(nrows)],  # struct
        },
        index=pd.Index([10 * i for i in range(nrows)], name="myindex"),
    )
    if range_index:
        df = df.reset_index(drop=True)
        df.index.name = "myindex"
    df.to_parquet(path)

    # "Traditional read" (without `open_parquet_file`)
    expect = pd.read_parquet(path, columns=columns, engine=engine)

    # Use `_get_parquet_byte_ranges` to re-write a
    # place-holder file with all bytes NOT required
    # to read `columns` set to b"0". The purpose of
    # this step is to make sure the read will fail
    # if the correct bytes have not been accurately
    # selected by `_get_parquet_byte_ranges`. If this
    # test were reading from remote storage, we would
    # not need this logic to capture errors.
    fs = url_to_fs(path)[0]
    data = _get_parquet_byte_ranges(
        [path],
        fs,
        columns=columns,
        engine=engine,
        max_gap=max_gap,
        max_block=max_block,
        footer_sample_size=footer_sample_size,
    )[path]
    file_size = fs.size(path)
    with open(path, "wb") as f:
        f.write(b"0" * file_size)

        if footer_sample_size == 8 and columns is not None:
            # We know 8 bytes is too small to include
            # the footer metadata, so there should NOT
            # be a key for the last 8 bytes of the file
            bad_key = (file_size - 8, file_size)
            assert bad_key not in data

        for (start, stop), byte_data in data.items():
            f.seek(start)
            f.write(byte_data)

    # Read back the modified file with `open_parquet_file`
    with open_parquet_file(
        path,
        columns=columns,
        engine=engine,
        max_gap=max_gap,
        max_block=max_block,
        footer_sample_size=footer_sample_size,
    ) as f:
        result = pd.read_parquet(f, columns=columns, engine=engine)

    # Check that `result` matches `expect`
    pd.testing.assert_frame_equal(expect, result)

    # Try passing metadata
    if engine == "fastparquet":
        # Should work fine for "fastparquet"
        pf = fastparquet.ParquetFile(path)
        with open_parquet_file(
            path,
            metadata=pf,
            columns=columns,
            engine=engine,
            max_gap=max_gap,
            max_block=max_block,
            footer_sample_size=footer_sample_size,
        ) as f:
            # TODO: construct directory test
            import struct

            footer = bytes(pf.fmd.to_bytes())
            footer2 = footer + struct.pack(b"<I", len(footer)) + b"PAR1"
            f.cache.data[(f.size, f.size + len(footer2))] = footer2
            f.size = f.cache.size = f.size + len(footer2)

            result = pd.read_parquet(f, columns=columns, engine=engine)
        pd.testing.assert_frame_equal(expect, result)
    elif engine == "pyarrow":
        # Should raise ValueError for "pyarrow"
        import pyarrow

        with pytest.raises((ValueError, pyarrow.ArrowError)):
            open_parquet_file(
                path,
                metadata=["Not-None"],
                columns=columns,
                engine=engine,
                max_gap=max_gap,
                max_block=max_block,
                footer_sample_size=footer_sample_size,
            )
