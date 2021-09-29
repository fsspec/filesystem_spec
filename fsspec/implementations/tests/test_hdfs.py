from pathlib import Path

import pytest

import fsspec

pyarrow = pytest.importorskip("pyarrow")

basedir = "/tmp/test-fsspec"
data = b"\n".join([b"some test data"] * 1000)


@pytest.fixture
def hdfs(request):
    try:
        hdfs = pyarrow.hdfs.HadoopFileSystem()
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
    fs = fsspec.filesystem("hdfs")
    fs.touch(basedir + "/file_2")
    fs.mkdir(basedir + "/dir_1")
    fs.touch(basedir + "/dir_1/file_1")
    fs.mkdir(basedir + "/dir_2")

    out = {(f["name"], f["kind"]) for f in fs.ls(basedir)}
    assert out == {
        (basedir + "/file", "file"),
        (basedir + "/file_2", "file"),
        (basedir + "/dir_1", "directory"),
        (basedir + "/dir_2", "directory"),
    }


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


def test_copying(hdfs):
    fs = fsspec.filesystem("hdfs")

    fs.mkdir(basedir + "/test_dir")
    fs.touch(basedir + "/test_dir/a")
    fs.touch(basedir + "/test_dir/b")
    fs.mkdir(basedir + "/test_dir/c")
    fs.touch(basedir + "/test_dir/c/d")

    fs.copy(basedir + "/test_dir", basedir + "/copy_dir", recursive=True)
    assert fs.find(basedir + "/copy_dir", detail=False) == [
        basedir + "/copy_dir" + "/a",
        basedir + "/copy_dir" + "/b",
        basedir + "/copy_dir" + "/c/d",
    ]


def test_getting(hdfs, tmpdir):
    fs = fsspec.filesystem("hdfs")

    src_dir = Path(tmpdir / "source")
    dst_dir = Path(tmpdir / "destination")

    src_dir.mkdir()
    (src_dir / "file_1.txt").write_text("file_1")
    (src_dir / "file_2.txt").write_text("file_2")
    (src_dir / "dir_1").mkdir()
    (src_dir / "dir_1" / "file_3.txt").write_text("file_3")
    (src_dir / "dir_1" / "file_4.txt").write_text("file_4")
    (src_dir / "dir_2").mkdir()

    fs.put(str(src_dir), basedir + "/src", recursive=True)
    fs.get(basedir + "/src", str(dst_dir), recursive=True)

    files = [file.relative_to(dst_dir) for file in dst_dir.glob("**/*")]

    assert set(map(str, files)) == {
        "file_1.txt",
        "file_2.txt",
        "dir_1/file_3.txt",
        "dir_1/file_4.txt",
        "dir_1",
        "dir_2",
    }

    assert {
        (dst_dir / file).read_text() for file in files if (dst_dir / file).is_file()
    } == {"file_1", "file_2", "file_3", "file_4"}
