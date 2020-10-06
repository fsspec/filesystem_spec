import pytest
import fsspec

pyarrow = pytest.importorskip("pyarrow")

basedir = "/tmp/test-fsspec"
data = b"\n".join([b"some test data"] * 1000)


@pytest.fixture
def hdfs(request):
    try:
        hdfs = pyarrow.hdfs.connect()
    except IOError:
        pytest.skip("No HDFS configured")

    if hdfs.exists(basedir):
        hdfs.rm(basedir, recursive=True)

    hdfs.mkdir(basedir)

    with hdfs.open(basedir + "/file", "wb") as f:
        f.write(data)

    yield hdfs

    if hdfs.exists(basedir):
        hdfs.rm(basedir, recursive=True)


def test_ls(hdfs):
    h = fsspec.filesystem("hdfs")
    out = [f["name"] for f in h.ls(basedir)]
    assert out == hdfs.ls(basedir)


def test_walk(hdfs):
    h = fsspec.filesystem("hdfs")
    out = h.walk(basedir)
    assert list(out) == list(hdfs.walk(basedir))


def test_isdir(hdfs):
    h = fsspec.filesystem("hdfs")
    assert h.isdir(basedir)
    assert not h.isdir(basedir + "/file")


def test_exists(hdfs):
    h = fsspec.filesystem("hdfs")
    assert not h.exists(basedir + "/notafile")


def test_read(hdfs):
    h = fsspec.filesystem("hdfs")
    out = basedir + "/file"
    with h.open(out, "rb") as f:
        assert f.read() == data
    with h.open(out, "rb", block_size=0) as f:
        assert f.read() == data
    with h.open(out, "rb") as f:
        assert f.read(100) + f.read() == data
