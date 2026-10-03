import os
import pickle
import platform
import sys
import uuid

import pytest

import fsspec
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem


@pytest.fixture(params=["memory", "file", "async", "simplecache"])
def literal_mapper(request, tmp_path):
    if request.param == "file":
        fs = LocalFileSystem()
    else:
        fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
        if request.param == "async":
            fs = AsyncFileSystemWrapper(fs=fs, asynchronous=False)
        elif request.param == "simplecache":
            fs = fsspec.filesystem(
                "simplecache", fs=fs, cache_storage=str(tmp_path / "cache")
            )
    return fs.get_mapper(tmp_path.as_posix() + "/mapping", create=True)


def test_literal_mapping_key_reads(literal_mapper):
    mapper = literal_mapper
    values = {"chunk[0]": b"literal", "chunk0": b"other"}
    mapper.setitems(values)

    assert mapper["chunk[0]"] == b"literal"
    assert mapper.getitems(list(values)) == values
    assert mapper.getitems(["chunk[0]", "chunk[0]", "chunk0"]) == values


def test_literal_mapping_key_delete(literal_mapper):
    mapper = literal_mapper
    mapper.setitems({"chunk[0]": b"literal", "chunk0": b"other"})

    del mapper["chunk[0]"]

    assert "chunk[0]" not in mapper
    assert mapper["chunk0"] == b"other"


def test_literal_mapping_key_bulk_delete(literal_mapper):
    mapper = literal_mapper
    mapper.setitems({"chunk[0]": b"literal", "chunk0": b"other", "plain": b"data"})

    mapper.delitems(iter(["chunk[0]", "plain", "chunk[0]"]))

    assert "chunk[0]" not in mapper
    assert "plain" not in mapper
    assert mapper["chunk0"] == b"other"


def test_missing_literal_mapping_key(literal_mapper):
    mapper = literal_mapper
    mapper["chunk0"] = b"other"

    with pytest.raises(KeyError):
        mapper["chunk[0]"]
    with pytest.raises(KeyError):
        mapper.getitems(["chunk[0]"])
    assert mapper.getitems(["chunk[0]"], on_error="omit") == {}
    assert isinstance(
        mapper.getitems(["chunk[0]"], on_error="return")["chunk[0]"], KeyError
    )
    with pytest.raises(KeyError):
        del mapper["chunk[0]"]
    assert mapper["chunk0"] == b"other"


def test_literal_mapping_root(literal_mapper):
    fs = literal_mapper.fs
    root = literal_mapper.root + "[0]"
    mapper = fs.get_mapper(root, create=True)
    mapper["plain"] = b"literal root"
    fs.makedirs(literal_mapper.root + "0", exist_ok=True)
    fs.pipe_file(literal_mapper.root + "0/plain", b"other root")

    assert mapper["plain"] == b"literal root"
    assert mapper.getitems(["plain"]) == {"plain": b"literal root"}
    del mapper["plain"]
    assert fs.cat_file(literal_mapper.root + "0/plain") == b"other root"


def test_literal_mapping_root_clear(literal_mapper):
    fs = literal_mapper.fs
    mapper = fs.get_mapper(literal_mapper.root + "[0]", create=True)
    mapper.update({"plain": b"data", "nested/key": b"more data"})
    fs.makedirs(literal_mapper.root + "0", exist_ok=True)
    fs.pipe_file(literal_mapper.root + "0/plain", b"other root")

    mapper.clear()

    assert list(mapper) == []
    assert fs.cat_file(literal_mapper.root + "0/plain") == b"other root"


def test_missing_literal_mapping_bulk_delete(literal_mapper):
    mapper = literal_mapper
    mapper["chunk0"] = b"other"

    with pytest.raises(FileNotFoundError):
        mapper.delitems(["chunk[0]"])

    assert mapper["chunk0"] == b"other"


@pytest.mark.parametrize("on_error", ["raise", "omit", "return"])
def test_literal_mapping_mixed_getitems_errors(literal_mapper, on_error):
    mapper = literal_mapper
    mapper["present[0]"] = b"data"
    keys = ["present[0]", "missing[0]"]

    if on_error == "raise":
        with pytest.raises(KeyError):
            mapper.getitems(keys, on_error=on_error)
    else:
        out = mapper.getitems(keys, on_error=on_error)
        assert out["present[0]"] == b"data"
        if on_error == "omit":
            assert "missing[0]" not in out
        else:
            assert isinstance(out["missing[0]"], KeyError)


@pytest.mark.parametrize("operation", ["delete", "pop"])
def test_mapping_delete_with_rm_only_filesystem(operation):
    class RmOnlyFileSystem(MemoryFileSystem):
        def _rm(self, path):
            raise NotImplementedError

        def rm(self, path, recursive=False):
            MemoryFileSystem._rm(self, path)

    fs = RmOnlyFileSystem(global_store=False, skip_instance_cache=True)
    mapper = fs.get_mapper("/mapping", create=True)
    mapper["plain"] = b"data"

    if operation == "delete":
        del mapper["plain"]
    else:
        assert mapper.pop("plain") == b"data"
    assert "plain" not in mapper


def test_mapping_prefix(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")

    m = fsspec.get_mapper(f"file://{tmpdir}")
    assert "afile" in m
    assert m["afolder/anotherfile"] == b"test2"

    fs = fsspec.filesystem("file")
    m2 = fs.get_mapper(tmpdir)
    m3 = fs.get_mapper(f"file://{tmpdir}")

    assert m == m2 == m3


@pytest.mark.parametrize("protocol", ["file", "memory"])
@pytest.mark.parametrize("key", ["a", "a/nested"])
def test_check_preserves_contents(tmp_path, protocol, key):
    url = f"{protocol}://{tmp_path.as_posix()}/mapping"
    mapper = fsspec.get_mapper(url, create=True)
    contents = {key: b"existing data", "other": b"more data"}
    mapper.update(contents)

    checked = fsspec.get_mapper(url, check=True)

    assert dict(checked) == contents


def test_getitems_errors(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")
    m = fsspec.get_mapper(f"file://{tmpdir}")
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}
    with pytest.raises(KeyError):
        m.getitems(["afile", "bfile"])
    out = m.getitems(["afile", "bfile"], on_error="return")
    assert isinstance(out["bfile"], KeyError)
    m = fsspec.get_mapper(f"file://{tmpdir}", missing_exceptions=())
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
    # https://github.com/fsspec/filesystem_spec/issues/186
    m = fsspec.get_mapper("memory://")
    m["key"] = b"data"

    keys = m.keys()
    assert len(keys) == 1
    # check that we don't consume the keys
    assert len(keys) == 1
    m.clear()


def test_multi():
    m = fsspec.get_mapper("memory:///")
    data = {"a": b"data1", "b": b"data2"}
    m.setitems(data)

    assert m.getitems(list(data)) == data
    m.delitems(list(data))
    assert not list(m)


def test_setitem_types():
    import array

    m = fsspec.get_mapper("memory://")
    m["a"] = array.array("i", [1])
    if sys.byteorder == "little":
        assert m["a"] == b"\x01\x00\x00\x00"
    else:
        assert m["a"] == b"\x00\x00\x00\x01"
    m["b"] = bytearray(b"123")
    assert m["b"] == b"123"
    m.setitems({"c": array.array("i", [1]), "d": bytearray(b"123")})
    if sys.byteorder == "little":
        assert m["c"] == b"\x01\x00\x00\x00"
    else:
        assert m["c"] == b"\x00\x00\x00\x01"
    assert m["d"] == b"123"


def test_setitem_numpy():
    m = fsspec.get_mapper("memory://")
    np = pytest.importorskip("numpy")
    m["c"] = np.array(1, dtype="<i4")  # scalar
    assert m["c"] == b"\x01\x00\x00\x00"
    m["c"] = np.array([1, 2], dtype="<i4")  # array
    assert m["c"] == b"\x01\x00\x00\x00\x02\x00\x00\x00"
    m["c"] = np.array(
        np.datetime64("2000-01-01T23:59:59.999999999"), dtype="<M8[ns]"
    )  # datetime64 scalar
    assert m["c"] == b"\xff\xff\x91\xe3c\x9b#\r"
    m["c"] = np.array(
        [
            np.datetime64("1900-01-01T23:59:59.999999999"),
            np.datetime64("2000-01-01T23:59:59.999999999"),
        ],
        dtype="<M8[ns]",
    )  # datetime64 array
    assert m["c"] == b"\xff\xff}p\xf8fX\xe1\xff\xff\x91\xe3c\x9b#\r"
    m["c"] = np.array(
        np.timedelta64(3155673612345678901, "ns"), dtype="<m8[ns]"
    )  # timedelta64 scalar
    assert m["c"] == b"5\x1c\xf0Rn4\xcb+"
    m["c"] = np.array(
        [
            np.timedelta64(450810516049382700, "ns"),
            np.timedelta64(3155673612345678901, "ns"),
        ],
        dtype="<m8[ns]",
    )  # timedelta64 scalar
    assert m["c"] == b',M"\x9e\xc6\x99A\x065\x1c\xf0Rn4\xcb+'


def test_empty_url():
    m = fsspec.get_mapper()
    assert isinstance(m.fs, LocalFileSystem)


def test_fsmap_access_with_root_prefix(tmp_path):
    # "/a" and "a" are the same for LocalFileSystem
    tmp_path.joinpath("a").write_bytes(b"data")
    m = fsspec.get_mapper(f"file://{tmp_path}")
    assert m["/a"] == m["a"] == b"data"

    # "/a" and "a" differ for MemoryFileSystem
    m = fsspec.get_mapper(f"memory://{uuid.uuid4()}")
    m["/a"] = b"data"

    assert m["/a"] == b"data"
    with pytest.raises(KeyError):
        _ = m["a"]


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(b"k", id="bytes"),
        pytest.param(1234, id="int"),
        pytest.param((1,), id="tuple"),
        pytest.param([""], id="list"),
    ],
)
def test_fsmap_non_str_keys(key):
    m = fsspec.get_mapper()

    # Once the deprecation period passes
    # FSMap.__getitem__ should raise TypeError for non-str keys
    #   with pytest.raises(TypeError):
    #       _ = m[key]

    with pytest.warns(DeprecationWarning):
        with pytest.raises(KeyError):
            _ = m[key]


def test_fsmap_error_on_protocol_keys():
    root = uuid.uuid4()
    m = fsspec.get_mapper(f"memory://{root}", create=True)
    m["a"] = b"data"

    assert m["a"] == b"data"
    with pytest.raises(KeyError):
        _ = m[f"memory://{root}/a"]


def test_fsmap_access_with_suffix(tmp_path):
    tmp_path.joinpath("b").mkdir()
    tmp_path.joinpath("b", "a").write_bytes(b"data")
    if platform.system() == "Windows":
        # on Windows opening a directory will raise PermissionError
        # see: https://bugs.python.org/issue43095
        missing_exceptions = (
            FileNotFoundError,
            IsADirectoryError,
            NotADirectoryError,
            PermissionError,
        )
    else:
        missing_exceptions = None
    m = fsspec.get_mapper(f"file://{tmp_path}", missing_exceptions=missing_exceptions)
    with pytest.raises(KeyError):
        _ = m["b/"]
    assert m["b/a/"] == b"data"


def test_fsmap_dirfs():
    m = fsspec.get_mapper("memory://")

    fs = m.dirfs
    assert isinstance(fs, fsspec.implementations.dirfs.DirFileSystem)
    assert fs.path == m.root


@pytest.mark.parametrize("protocol", ["memory", "file"])
@pytest.mark.parametrize("default", [None, False, 0, b"", []])
def test_pop_missing_key_returns_explicit_default(tmp_path, protocol, default):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )

    assert mapper.pop("missing", default) is default


@pytest.mark.parametrize("protocol", ["memory", "file"])
def test_pop_without_default_raises_for_missing_key(tmp_path, protocol):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )

    with pytest.raises(KeyError, match="missing"):
        mapper.pop("missing")


@pytest.mark.parametrize("protocol", ["memory", "file"])
def test_pop_existing_key_returns_and_removes_value(tmp_path, protocol):
    mapper = fsspec.get_mapper(
        f"{protocol}://{tmp_path.as_posix()}/mapping", create=True
    )
    mapper["key"] = b"value"

    assert mapper.pop("key", None) == b"value"
    assert "key" not in mapper
