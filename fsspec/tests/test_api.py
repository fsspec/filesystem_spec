"""Tests the spec, using memoryfs"""

import contextlib
import os
import pickle
import tempfile

import pytest

import fsspec
from fsspec.implementations.memory import MemoryFile, MemoryFileSystem


def test_idempotent():
    MemoryFileSystem.clear_instance_cache()
    fs = MemoryFileSystem()
    fs2 = MemoryFileSystem()
    assert fs is fs2
    assert MemoryFileSystem.current() is fs2

    MemoryFileSystem.clear_instance_cache()
    assert not MemoryFileSystem._cache

    fs2 = MemoryFileSystem().current()
    assert fs == fs2


def test_pickle():
    fs = MemoryFileSystem()
    fs2 = pickle.loads(pickle.dumps(fs))
    assert fs == fs2


def test_class_methods():
    assert MemoryFileSystem._strip_protocol("memory://stuff") == "/stuff"
    assert MemoryFileSystem._strip_protocol("stuff") == "/stuff"
    assert MemoryFileSystem._strip_protocol("other://stuff") == "other://stuff"

    assert MemoryFileSystem._get_kwargs_from_urls("memory://user@thing") == {}


def test_get_put(tmpdir, m):
    tmpdir = str(tmpdir)
    fn = os.path.join(tmpdir, "one")
    open(fn, "wb").write(b"one")
    os.mkdir(os.path.join(tmpdir, "dir"))
    fn2 = os.path.join(tmpdir, "dir", "two")
    open(fn2, "wb").write(b"two")

    fs = MemoryFileSystem()
    fs.put(fn, "/afile")
    assert fs.cat("/afile") == b"one"

    fs.store["/bfile"] = MemoryFile(fs, "/bfile", b"data")
    fn3 = os.path.join(tmpdir, "three")
    fs.get("/bfile", fn3)
    assert open(fn3, "rb").read() == b"data"

    fs.put(tmpdir, "/more", recursive=True)
    assert fs.find("/more") == ["/more/dir/two", "/more/one", "/more/three"]

    @contextlib.contextmanager
    def tmp_chdir(path):
        curdir = os.getcwd()
        os.chdir(path)
        try:
            yield
        finally:
            os.chdir(curdir)

    with tmp_chdir(os.path.join(tmpdir, os.path.pardir)):
        fs.put(os.path.basename(tmpdir), "/moretwo", recursive=True)
        assert fs.find("/moretwo") == [
            "/moretwo/dir/two",
            "/moretwo/one",
            "/moretwo/three",
        ]

    with tmp_chdir(tmpdir):
        fs.put(os.path.curdir, "/morethree", recursive=True)
        assert fs.find("/morethree") == [
            "/morethree/dir/two",
            "/morethree/one",
            "/morethree/three",
        ]

    for f in [fn, fn2, fn3]:
        os.remove(f)
    os.rmdir(os.path.join(tmpdir, "dir"))

    fs.get("/more/", tmpdir + "/", recursive=True)
    assert open(fn3, "rb").read() == b"data"
    assert open(fn, "rb").read() == b"one"


def test_du(m):
    fs = MemoryFileSystem()
    fs.store.update(
        {
            "/dir/afile": MemoryFile(fs, "/afile", b"a"),
            "/dir/dirb/afile": MemoryFile(fs, "/afile", b"bb"),
            "/dir/dirb/bfile": MemoryFile(fs, "/afile", b"ccc"),
        }
    )
    assert fs.du("/dir") == 6
    assert fs.du("/dir", total=False)["/dir/dirb/afile"] == 2
    assert fs.du("/dir", maxdepth=0) == 1


def test_head_tail(m):
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"I had a nice big cabbage")
    assert fs.head("/myfile", 5) == b"I had"
    assert fs.tail("/myfile", 7) == b"cabbage"


def test_move(m):
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"I had a nice big cabbage")
    fs.move("/myfile", "/otherfile")
    assert not fs.exists("/myfile")
    assert fs.info("/otherfile")
    assert isinstance(fs.ukey("/otherfile"), str)


def test_recursive_get_put(tmpdir, m):
    fs = MemoryFileSystem()
    os.makedirs(f"{tmpdir}/nest")
    for file in ["one", "two", "nest/other"]:
        with open(f"{tmpdir}/{file}", "wb") as f:
            f.write(b"data")

    fs.put(str(tmpdir), "test", recursive=True)

    d = tempfile.mkdtemp()
    fs.get("test", d, recursive=True)
    for file in ["one", "two", "nest/other"]:
        with open(f"{d}/{file}", "rb") as f:
            f.read() == b"data"


def test_pipe_cat(m):
    fs = MemoryFileSystem()
    fs.pipe("afile", b"contents")
    assert fs.cat("afile") == b"contents"

    data = {"/bfile": b"more", "/cfile": b"stuff"}
    fs.pipe(data)
    assert fs.cat(list(data)) == data


def test_read_block_delimiter(m):
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"some\n" b"lines\n" b"of\n" b"text")
    assert fs.read_block("/myfile", 0, 2, b"\n") == b"some\n"
    assert fs.read_block("/myfile", 2, 6, b"\n") == b"lines\n"
    assert fs.read_block("/myfile", 6, 2, b"\n") == b""
    assert fs.read_block("/myfile", 2, 9, b"\n") == b"lines\nof\n"
    assert fs.read_block("/myfile", 12, 6, b"\n") == b"text"
    assert fs.read_block("/myfile", 0, None) == fs.cat("/myfile")


def test_open_text(m):
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"some\n" b"lines\n" b"of\n" b"text")
    f = fs.open("/myfile", "r", encoding="latin1")
    assert f.encoding == "latin1"


def test_chained_fs():
    d1 = tempfile.mkdtemp()
    d2 = tempfile.mkdtemp()
    f1 = os.path.join(d1, "f1")
    with open(f1, "wb") as f:
        f.write(b"test")

    of = fsspec.open(
        f"simplecache::file://{f1}",
        simplecache={"cache_storage": d2, "same_names": True},
    )
    with of as f:
        assert f.read() == b"test"

    assert os.listdir(d2) == ["f1"]


@pytest.mark.xfail(reason="see issue #334", strict=True)
def test_multilevel_chained_fs():
    """This test reproduces fsspec/filesystem_spec#334"""
    import zipfile

    d1 = tempfile.mkdtemp()
    f1 = os.path.join(d1, "f1.zip")
    with zipfile.ZipFile(f1, mode="w") as z:
        # filename, content
        z.writestr("foo.txt", "foo.txt")
        z.writestr("bar.txt", "bar.txt")

    # We expected this to be the correct syntax
    with pytest.raises(IsADirectoryError):
        of = fsspec.open_files(f"zip://*.txt::simplecache::file://{f1}")
        assert len(of) == 2

    # But this is what is actually valid...
    of = fsspec.open_files(f"zip://*.txt::simplecache://{f1}::file://")

    assert len(of) == 2
    for open_file in of:
        with open_file as f:
            assert f.read().decode("utf-8") == f.name


def test_multilevel_chained_fs_zip_zip_file():
    """This test reproduces fsspec/filesystem_spec#334"""
    import zipfile

    d1 = tempfile.mkdtemp()
    f1 = os.path.join(d1, "f1.zip")
    f2 = os.path.join(d1, "f2.zip")
    with zipfile.ZipFile(f1, mode="w") as z:
        # filename, content
        z.writestr("foo.txt", "foo.txt")
        z.writestr("bar.txt", "bar.txt")

    with zipfile.ZipFile(f2, mode="w") as z:
        with open(f1, "rb") as f:
            z.writestr("f1.zip", f.read())

    # We expected this to be the correct syntax
    of = fsspec.open_files(f"zip://*.txt::zip://f1.zip::file://{f2}")

    assert len(of) == 2
    for open_file in of:
        with open_file as f:
            assert f.read().decode("utf-8") == f.name


def test_chained_equivalent():
    d1 = tempfile.mkdtemp()
    d2 = tempfile.mkdtemp()
    f1 = os.path.join(d1, "f1")
    with open(f1, "wb") as f:
        f.write(b"test1")

    of = fsspec.open(
        f"simplecache::file://{f1}",
        simplecache={"cache_storage": d2, "same_names": True},
    )
    of2 = fsspec.open(
        f"simplecache://{f1}",
        cache_storage=d2,
        same_names=True,
        target_protocol="file",
        target_options={},
    )
    # the following line passes by fluke - they are not quite the same instance,
    #  since the parameters don't quite match. Also, the url understood by the two
    #  of s are not the same (path gets munged a bit differently)
    assert of.fs == of2.fs
    assert of.open().read() == of2.open().read()


def test_chained_fs_multi():
    d1 = tempfile.mkdtemp()
    d2 = tempfile.mkdtemp()
    f1 = os.path.join(d1, "f1")
    f2 = os.path.join(d1, "f2")
    with open(f1, "wb") as f:
        f.write(b"test1")
    with open(f2, "wb") as f:
        f.write(b"test2")

    of = fsspec.open_files(
        f"simplecache::file://{d1}/*",
        simplecache={"cache_storage": d2, "same_names": True},
    )
    with of[0] as f:
        assert f.read() == b"test1"
    with of[1] as f:
        assert f.read() == b"test2"

    assert sorted(os.listdir(d2)) == ["f1", "f2"]

    d2 = tempfile.mkdtemp()

    of = fsspec.open_files(
        [f"simplecache::file://{f1}", f"simplecache::file://{f2}"],
        simplecache={"cache_storage": d2, "same_names": True},
    )
    with of[0] as f:
        assert f.read() == b"test1"
    with of[1] as f:
        assert f.read() == b"test2"

    assert sorted(os.listdir(d2)) == ["f1", "f2"]


def test_chained_fo():
    import zipfile

    d1 = tempfile.mkdtemp()
    f1 = os.path.join(d1, "temp.zip")
    d3 = tempfile.mkdtemp()
    with zipfile.ZipFile(f1, mode="w") as z:
        z.writestr("afile", b"test")

    of = fsspec.open(f"zip://afile::file://{f1}")
    with of as f:
        assert f.read() == b"test"

    of = fsspec.open_files(f"zip://*::file://{f1}")
    with of[0] as f:
        assert f.read() == b"test"

    of = fsspec.open_files(
        f"simplecache::zip://*::file://{f1}",
        simplecache={"cache_storage": d3, "same_names": True},
    )
    with of[0] as f:
        assert f.read() == b"test"
    assert "afile" in os.listdir(d3)


def test_url_to_fs():
    url = "memory://a.txt"
    fs, url2 = fsspec.core.url_to_fs(url)

    assert isinstance(fs, MemoryFileSystem)
    assert url2 == "/a.txt"
