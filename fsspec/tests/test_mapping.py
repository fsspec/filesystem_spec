import os
import fsspec
from fsspec.implementations.memory import MemoryFileSystem
import pickle
import pytest


def test_mapping_prefix(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")

    m = fsspec.get_mapper("file://" + tmpdir)
    assert "afile" in m
    assert m["afolder/anotherfile"] == b"test2"

    fs = fsspec.filesystem("file")
    m2 = fs.get_mapper(tmpdir)
    m3 = fs.get_mapper("file://" + tmpdir)

    assert m == m2 == m3


def test_getitems_errors(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")
    m = fsspec.get_mapper("file://" + tmpdir)
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}
    with pytest.raises(KeyError):
        m.getitems(["afile", "bfile"])
    out = m.getitems(["afile", "bfile"], on_error="return")
    assert isinstance(out["bfile"], KeyError)
    m = fsspec.get_mapper("file://" + tmpdir, missing_exceptions=())
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}
    with pytest.raises(FileNotFoundError):
        m.getitems(["afile", "bfile"])


def test_ops():
    MemoryFileSystem.store.clear()
    m = fsspec.get_mapper("memory://")
    assert not m
    assert list(m) == []

    with pytest.raises(KeyError):
        m["hi"]

    assert m.pop("key", 0) == 0

    m["key0"] = b"data"
    assert list(m) == ["key0"]
    assert m["key0"] == b"data"

    m.clear()

    assert list(m) == []


def test_pickle():
    m = fsspec.get_mapper("memory://")
    assert isinstance(m.fs, MemoryFileSystem)
    m["key"] = b"data"
    m2 = pickle.loads(pickle.dumps(m))
    assert list(m) == list(m2)
    assert m.missing_exceptions == m2.missing_exceptions


def test_keys_view():
    # https://github.com/intake/filesystem_spec/issues/186
    m = fsspec.get_mapper("memory://")
    m["key"] = b"data"

    keys = m.keys()
    assert len(keys) == 1
    # check that we don't consume the keys
    assert len(keys) == 1
    m.clear()


def test_multi():
    m = fsspec.get_mapper("memory://")
    data = {"a": b"data1", "b": b"data2"}
    m.setitems(data)

    assert m.getitems(list(data)) == data
    m.delitems(list(data))
    assert not list(m)


def test_setitem_types():
    import array

    m = fsspec.get_mapper("memory://")
    m["a"] = array.array("i", [1])
    assert m["a"] == b"\x01\x00\x00\x00"
    m["b"] = bytearray(b"123")
    assert m["b"] == b"123"
    m.setitems({"c": array.array("i", [1]), "d": bytearray(b"123")})
    assert m["c"] == b"\x01\x00\x00\x00"
    assert m["d"] == b"123"


def test_setitem_numpy():
    m = fsspec.get_mapper("memory://")
    np = pytest.importorskip("numpy")
    m["c"] = np.array(1, dtype="int32")  # scalar
    assert m["c"] == b"\x01\x00\x00\x00"
    m["c"] = np.array([1, 2], dtype="int32")  # array
    assert m["c"] == b"\x01\x00\x00\x00\x02\x00\x00\x00"
