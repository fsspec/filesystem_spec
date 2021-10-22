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

from fsspec.parquet import open_parquet_file

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


@pytest.mark.parametrize("columns", [None, ["x"], ["x", "y"]])
@pytest.mark.parametrize("max_gap", [64, 0])
@pytest.mark.parametrize("max_block", [64, 256_000_000])
@pytest.mark.parametrize("footer_sample_size", [64, 32_000_000])
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
            "x": [i * 7 % 5 for i in range(nrows)],  # Not sorted
            "y": [i * 2.5 for i in range(nrows)],  # Sorted
        },
        index=pd.Index([10 * i for i in range(nrows)], name="myindex"),
    )
    df.to_parquet(path)

    # Read back with and without `open_parquet_file`
    expect = pd.read_parquet(path, columns=columns)
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
