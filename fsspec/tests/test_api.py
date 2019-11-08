"""Tests the spec, using memoryfs"""

import os
import pickle
from fsspec.implementations.memory import MemoryFileSystem, MemoryFile


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
    assert MemoryFileSystem._strip_protocol("memory:stuff") == "stuff"
    assert MemoryFileSystem._strip_protocol("memory://stuff") == "stuff"
    assert MemoryFileSystem._strip_protocol("stuff") == "stuff"
    assert MemoryFileSystem._strip_protocol("other://stuff") == "other://stuff"

    assert MemoryFileSystem._get_kwargs_from_urls("memory://user@thing") == {}


def test_get_put(tmpdir):
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

    for f in [fn, fn2, fn3]:
        os.remove(f)
    os.rmdir(os.path.join(tmpdir, "dir"))

    fs.get("/more/", tmpdir + "/", recursive=True)
    assert open(fn3, "rb").read() == b"data"
    assert open(fn, "rb").read() == b"one"


def test_du():
    fs = MemoryFileSystem()
    fs.store = {
        "/dir/afile": MemoryFile(fs, "/afile", b"a"),
        "/dir/dirb/afile": MemoryFile(fs, "/afile", b"bb"),
        "/dir/dirb/bfile": MemoryFile(fs, "/afile", b"ccc"),
    }
    assert fs.du("/dir") == 6
    assert fs.du("/dir", total=False)["/dir/dirb/afile"] == 2
    assert fs.du("/dir", maxdepth=0) == 1


def test_head_tail():
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"I had a nice big cabbage")
    assert fs.head("/myfile", 5) == b"I had"
    assert fs.tail("/myfile", 7) == b"cabbage"


def test_move():
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"I had a nice big cabbage")
    fs.move("/myfile", "/otherfile")
    assert not fs.exists("/myfile")
    assert fs.info("/otherfile")
    assert isinstance(fs.ukey("/otherfile"), str)


def test_read_block_delimiter():
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"some\n" b"lines\n" b"of\n" b"text")
    assert fs.read_block("/myfile", 0, 2, b"\n") == b"some\n"
    assert fs.read_block("/myfile", 2, 6, b"\n") == b"lines\n"
    assert fs.read_block("/myfile", 6, 2, b"\n") == b""
    assert fs.read_block("/myfile", 2, 9, b"\n") == b"lines\nof\n"
    assert fs.read_block("/myfile", 12, 6, b"\n") == b"text"
    assert fs.read_block("/myfile", 0, None) == fs.cat("/myfile")


def test_open_text():
    fs = MemoryFileSystem()
    with fs.open("/myfile", "wb") as f:
        f.write(b"some\n" b"lines\n" b"of\n" b"text")
    f = fs.open("/myfile", "r", encoding="latin1")
    assert f.encoding == "latin1"
