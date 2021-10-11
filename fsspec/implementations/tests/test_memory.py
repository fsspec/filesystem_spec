import os

import pytest


def test_1(m):
    m.touch("/somefile")  # NB: is found with or without initial /
    m.touch("afiles/and/another")
    files = m.find("")
    assert files == ["/afiles/and/another", "/somefile"]

    files = sorted(m.get_mapper("/"))
    assert files == ["afiles/and/another", "somefile"]


def test_strip(m):
    assert m._strip_protocol("") == ""
    assert m._strip_protocol("memory://") == ""
    assert m._strip_protocol("afile") == "/afile"
    assert m._strip_protocol("/b/c") == "/b/c"
    assert m._strip_protocol("/b/c/") == "/b/c"


def test_put_single(m, tmpdir):
    fn = os.path.join(str(tmpdir), "dir")
    os.mkdir(fn)
    open(os.path.join(fn, "abc"), "w").write("text")
    m.put(fn, "/test")  # no-op, no files
    assert not m.exists("/test/abc")
    assert not m.exists("/test/dir")
    m.put(fn + "/", "/test", recursive=True)
    assert m.cat("/test/dir/abc") == b"text"


def test_ls(m):
    m.mkdir("/dir")
    m.mkdir("/dir/dir1")

    m.touch("/dir/afile")
    m.touch("/dir/dir1/bfile")
    m.touch("/dir/dir1/cfile")

    assert m.ls("/", False) == ["/dir"]
    assert m.ls("/dir", False) == ["/dir/afile", "/dir/dir1"]
    assert m.ls("/dir", True)[0]["type"] == "file"
    assert m.ls("/dir", True)[1]["type"] == "directory"

    assert len(m.ls("/dir/dir1")) == 2


def test_directories(m):
    m.mkdir("outer/inner")
    assert m.info("outer/inner")["type"] == "directory"

    assert m.ls("outer")
    assert m.ls("outer/inner") == []

    with pytest.raises(OSError):
        m.rmdir("outer")

    m.rmdir("outer/inner")
    m.rmdir("outer")

    assert not m.store


def test_mv_recursive(m):
    m.mkdir("src")
    m.touch("src/file.txt")
    m.mv("src", "dest", recursive=True)
    assert m.exists("dest/file.txt")
    assert not m.exists("src")


def test_rm_no_psuedo_dir(m):
    m.touch("/dir1/dir2/file")
    m.rm("/dir1", recursive=True)
    assert not m.exists("/dir1/dir2/file")
    assert not m.exists("/dir1/dir2")
    assert not m.exists("/dir1")

    with pytest.raises(FileNotFoundError):
        m.rm("/dir1", recursive=True)


def test_rewind(m):
    # https://github.com/intake/filesystem_spec/issues/349
    with m.open("src/file.txt", "w") as f:
        f.write("content")
    with m.open("src/file.txt") as f:
        assert f.tell() == 0


def test_empty_raises(m):
    with pytest.raises(FileNotFoundError):
        m.ls("nonexistent")

    with pytest.raises(FileNotFoundError):
        m.info("nonexistent")


def test_dir_errors(m):
    m.mkdir("/first")

    with pytest.raises(FileExistsError):
        m.mkdir("/first")
    with pytest.raises(FileExistsError):
        m.makedirs("/first", exist_ok=False)
    m.makedirs("/first", exist_ok=True)
    m.makedirs("/first/second/third")
    assert "/first/second" in m.pseudo_dirs

    m.touch("/afile")
    with pytest.raises(NotADirectoryError):
        m.mkdir("/afile/nodir")


def test_no_rewind_append_mode(m):
    # https://github.com/intake/filesystem_spec/issues/349
    with m.open("src/file.txt", "w") as f:
        f.write("content")
    with m.open("src/file.txt", "a") as f:
        assert f.tell() == 7


def test_moves(m):
    m.touch("source.txt")
    m.mv("source.txt", "target.txt")

    m.touch("source2.txt")
    m.mv("source2.txt", "target2.txt", recursive=True)
    assert m.find("") == ["/target.txt", "/target2.txt"]


def test_rm_reursive_empty_subdir(m):
    # https://github.com/intake/filesystem_spec/issues/500
    m.mkdir("recdir")
    m.mkdir("recdir/subdir2")
    m.rm("recdir/", recursive=True)
    assert not m.exists("dir")


def test_seekable(m):
    fn0 = "foo.txt"
    with m.open(fn0, "wb") as f:
        f.write(b"data")

    f = m.open(fn0, "rt")
    assert f.seekable(), "file is not seekable"
    f.seek(1)
    assert f.read(1) == "a"
    assert f.tell() == 2
