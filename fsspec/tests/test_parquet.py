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
ANY_ENGINE_MARK = pytest.mark.skipif(
    not (fastparquet or pq),
    reason="No parquet engine (fastparquet or pyarrow) found",
)


@pytest.fixture(
    params=[
        pytest.param("fastparquet", marks=FASTPARQUET_MARK),
        pytest.param("pyarrow", marks=PYARROW_MARK),
        pytest.param("auto", marks=ANY_ENGINE_MARK),
    ]
)
def engine(request):
    return request.param


@pytest.mark.parametrize("columns", [None, ["x"], ["x", "y"], ["z"]])
@pytest.mark.parametrize("max_gap", [0, 64])
@pytest.mark.parametrize("max_block", [64, 256_000_000])
@pytest.mark.parametrize("footer_sample_size", [64, 1_000])
def test_open_parquet_file(
    tmpdir, engine, columns, max_gap, max_block, footer_sample_size
):

    # Pandas required for this test
    pd = pytest.importorskip("pandas")

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
    df.to_parquet(path)

    # "Traditional read" (without `open_parquet_file`)
    expect = pd.read_parquet(path, columns=columns)

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
        result = pd.read_parquet(f, columns=columns)

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
            result = pd.read_parquet(f, columns=columns)
        pd.testing.assert_frame_equal(expect, result)
    elif engine == "pyarrow":
        # Should raise ValueError for "pyarrow"
        with pytest.raises(ValueError):
            open_parquet_file(
                path,
                metadata=["Not-None"],
                columns=columns,
                engine=engine,
                max_gap=max_gap,
                max_block=max_block,
                footer_sample_size=footer_sample_size,
            )
