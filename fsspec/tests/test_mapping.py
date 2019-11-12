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


def test_keys_view():
    # https://github.com/intake/filesystem_spec/issues/186
    m = fsspec.get_mapper("memory://")
    m["key"] = b"data"

    keys = m.keys()
    assert len(keys) == 1
    # check that we don't consume the keys
    assert len(keys) == 1
