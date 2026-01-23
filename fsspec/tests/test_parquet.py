import os
import random

import pytest

try:
    import fastparquet
except ImportError:
    fastparquet = None
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

from fsspec.parquet import (
    open_parquet_file,
    open_parquet_files,
)

pd = pytest.importorskip("pandas")

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
    if columns == ["z"] and engine == "fastparquet":
        columns = ["z.a"]  # fastparquet is more specific

    # Write out a simple DataFrame
    path = os.path.join(str(tmpdir), "test.parquet")
    nrows = 40
    df = pd.DataFrame(
        {
            "y": [[0, i] for i in range(nrows)],  # list
            "z": [{"a": i, "b": "cat"} for i in range(nrows)],  # struct
            "x": [i * 7 % 5 for i in range(nrows)],
        },
        index=pd.Index([10 * i for i in range(nrows)], name="myindex"),
    )
    if range_index:
        df = df.reset_index(drop=True)
        df.index.name = "myindex"
    df.to_parquet(path)

    # "Traditional read" (without `open_parquet_file`)
    expect = pd.read_parquet(path, columns=columns, engine=engine)

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
        import pyarrow

        with pytest.raises((ValueError, pyarrow.ArrowException)):
            open_parquet_file(
                path,
                metadata=["Not-None"],
                columns=columns,
                engine=engine,
                max_gap=max_gap,
                max_block=max_block,
                footer_sample_size=footer_sample_size,
            )


@pytest.mark.filterwarnings("ignore:.*Not enough data.*")
@FASTPARQUET_MARK
def test_with_filter(tmpdir):
    df = pd.DataFrame(
        {
            "a": [10, 1, 2, 3, 7, 8, 9],
            "b": ["a", "a", "a", "b", "b", "b", "b"],
        }
    )
    fn = os.path.join(str(tmpdir), "test.parquet")
    df.to_parquet(fn, engine="fastparquet", row_group_offsets=[0, 3], stats=True)

    expect = pd.read_parquet(fn, engine="fastparquet", filters=[["b", "==", "b"]])
    f = open_parquet_file(
        fn,
        engine="fastparquet",
        filters=[["b", "==", "b"]],
        max_gap=1,
        max_block=1,
        footer_sample_size=8,
    )
    assert (0, 4) in f.cache.data
    assert f.cache.size < os.path.getsize(fn)

    result = pd.read_parquet(f, engine="fastparquet", filters=[["b", "==", "b"]])
    pd.testing.assert_frame_equal(expect, result)


@pytest.mark.filterwarnings("ignore:.*Not enough data.*")
@FASTPARQUET_MARK
def test_multiple(tmpdir):
    df = pd.DataFrame(
        {
            "a": [10, 1, 2, 3, 7, 8, 9],
            "b": ["a", "a", "a", "b", "b", "b", "b"],
        }
    )
    fn = os.path.join(str(tmpdir), "test.parquet/")
    df.to_parquet(
        fn,
        engine="fastparquet",
        row_group_offsets=[0, 3],
        stats=True,
        file_scheme="hive",
    )  # partition_on="b"

    # path ending in "/"
    expect = pd.read_parquet(fn, engine="fastparquet")[["a"]]
    ofs = open_parquet_files(
        fn,
        engine="fastparquet",
        columns=["a"],
        max_gap=1,
        max_block=1,
        footer_sample_size=8,
    )
    dfs = [pd.read_parquet(f, engine="fastparquet", columns=["a"]) for f in ofs]
    result = pd.concat(dfs).reset_index(drop=True)
    assert expect.equals(result)

    # glob
    ofs = open_parquet_files(
        fn + "*.parquet",
        engine="fastparquet",
        columns=["a"],
        max_gap=1,
        max_block=1,
        footer_sample_size=8,
    )
    dfs = [pd.read_parquet(f, engine="fastparquet", columns=["a"]) for f in ofs]
    result = pd.concat(dfs).reset_index(drop=True)
    assert expect.equals(result)

    # explicit
    ofs = open_parquet_files(
        [f"{fn}part.0.parquet", f"{fn}part.1.parquet"],
        engine="fastparquet",
        columns=["a"],
        max_gap=1,
        max_block=1,
        footer_sample_size=8,
    )
    dfs = [pd.read_parquet(f, engine="fastparquet", columns=["a"]) for f in ofs]
    result = pd.concat(dfs).reset_index(drop=True)
    assert expect.equals(result)


@pytest.mark.parametrize("n", [100, 10_000, 1_000_000])
def test_nested(n, tmpdir, engine):
    path = os.path.join(str(tmpdir), "test.parquet")
    pa = pytest.importorskip("pyarrow")
    flat = pa.array([random.random() for _ in range(n)])
    a = random.random()
    b = random.random()
    nested = pa.array([{"a": a, "b": b} for _ in range(n)])
    table = pa.table({"flat": flat, "nested": nested})
    pq.write_table(table, path)
    with open_parquet_file(path, columns=["nested.a"], engine=engine) as fh:
        col = pd.read_parquet(fh, engine=engine, columns=["nested.a"])
    name = "a" if engine == "pyarrow" else "nested.a"
    assert (col[name] == a).all()
