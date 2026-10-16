import array
import os
import pickle
import platform
import sys
import uuid

import pytest

import fsspec
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem


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


def _local_mapper(tmp_path, root_exists):
    """Mapper over a local root, with parents NOT created on write."""
    fs = LocalFileSystem(auto_mkdir=False, skip_instance_cache=True)
    root = tmp_path / "mapping"
    if root_exists:
        root.mkdir(parents=True)
    return fs.get_mapper(root.as_posix())


SETITEMS_BATCHES = {
    "flat": {"a": b"1", "b": b"2"},
    "nested": {"top/key": b"1"},
    "shared_parent": {"p/one": b"1", "p/two": b"2", "p/three": b""},
    "mixed_depths": {
        "flat": b"0",
        "d1/k": b"1",
        "d1/d2/k": b"2",
        "d1/d2/d3/d4/k": b"4",
        "other/d2/k": b"x",
    },
}


@pytest.mark.parametrize("root_exists", [True, False])
@pytest.mark.parametrize("batch", list(SETITEMS_BATCHES))
def test_setitems_local_creates_parents_like_setitem(tmp_path, root_exists, batch):
    # GH#1041: setitems must write nested keys wherever __setitem__ can
    values = SETITEMS_BATCHES[batch]
    single = _local_mapper(tmp_path / "single", root_exists)
    bulk = _local_mapper(tmp_path / "bulk", root_exists)

    for k, v in values.items():
        single[k] = v
    bulk.setitems(values)

    assert dict(bulk) == dict(single) == values
    assert not bulk.fs.auto_mkdir


@pytest.mark.parametrize("batch", list(SETITEMS_BATCHES))
def test_setitems_memory_nested(batch):
    # control: memory already accepted nested keys before GH#1041
    fs = MemoryFileSystem(skip_instance_cache=True)
    m = fs.get_mapper(f"/setitems-{uuid.uuid4().hex}")
    m.setitems(SETITEMS_BATCHES[batch])
    assert dict(m) == SETITEMS_BATCHES[batch]


def test_setitems_local_overwrite_and_partial_batch(tmp_path):
    m = _local_mapper(tmp_path, root_exists=True)
    m["existing"] = b"old"
    # "existing" and "flat" can be written before the missing parent fails,
    # so the retry rewrites them; the final bytes must be the new ones
    batch = {"existing": b"new", "flat": b"f", "missing/k": b"m"}
    m.setitems(batch)
    assert dict(m) == batch
    m.setitems({"missing/k": bytearray(b"again"), "existing": array.array("b", [7])})
    assert m["missing/k"] == b"again"
    assert m["existing"] == bytes(array.array("b", [7]))


def test_setitems_empty_batch_creates_nothing(tmp_path):
    m = _local_mapper(tmp_path, root_exists=False)
    m.setitems({})
    assert not (tmp_path / "mapping").exists()


def test_setitems_parent_is_file_still_errors(tmp_path):
    m = _local_mapper(tmp_path, root_exists=True)
    m["blocker"] = b"file"
    # the exact error is platform dependent (NotADirectoryError on POSIX;
    # Windows may report FileNotFoundError then FileExistsError from mkdirs)
    with pytest.raises(OSError):
        m.setitems({"blocker/child": b"x"})
    assert m["blocker"] == b"file"
    assert (tmp_path / "mapping" / "blocker").is_file()


class _RecordingFS(MemoryFileSystem):
    """Memory FS that records bulk writes and directory creation."""

    protocol = "recording"

    def __init__(self, *args, pipe_errors=(), mkdirs_error=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []
        self.pipe_errors = list(pipe_errors)
        self.mkdirs_error = mkdirs_error

    def pipe(self, path, value=None, **kwargs):
        self.calls.append(("pipe", dict(path)))
        if self.pipe_errors:
            raise self.pipe_errors.pop(0)
        return super().pipe(path, value, **kwargs)

    def makedirs(self, path, exist_ok=False):
        self.calls.append(("makedirs", path))
        if self.mkdirs_error is not None:
            raise self.mkdirs_error
        return super().makedirs(path, exist_ok=exist_ok)


def _recording_mapper(**kwargs):
    fs = _RecordingFS(skip_instance_cache=True, **kwargs)
    return fs.get_mapper(f"/rec-{uuid.uuid4().hex}")


def test_setitems_success_makes_no_directory_calls():
    # directory-free backends must keep one bulk write and no mkdirs
    m = _recording_mapper(mkdirs_error=AssertionError("mkdirs must not run"))
    m.setitems({"a/b": b"1", "c": b"2"})
    assert [c[0] for c in m.fs.calls] == ["pipe"]
    assert m["a/b"] == b"1"


def test_setitems_retries_once_after_missing_parent():
    m = _recording_mapper(pipe_errors=[FileNotFoundError("parent")])
    m.setitems({"x/a": b"1", "x/b": b"2", "y/z/c": b"3", "d": b"4"})
    root = m.root
    assert m.fs.calls == [
        (
            "pipe",
            {
                f"{root}/x/a": b"1",
                f"{root}/x/b": b"2",
                f"{root}/y/z/c": b"3",
                f"{root}/d": b"4",
            },
        ),
        ("makedirs", f"{root}/x"),
        ("makedirs", f"{root}/y/z"),
        ("makedirs", root),
        (
            "pipe",
            {
                f"{root}/x/a": b"1",
                f"{root}/x/b": b"2",
                f"{root}/y/z/c": b"3",
                f"{root}/d": b"4",
            },
        ),
    ]


def test_setitems_second_missing_error_propagates():
    m = _recording_mapper(pipe_errors=[FileNotFoundError("1"), FileNotFoundError("2")])
    with pytest.raises(FileNotFoundError, match="2"):
        m.setitems({"x/a": b"1"})
    assert [c[0] for c in m.fs.calls] == ["pipe", "makedirs", "pipe"]


def test_setitems_mkdirs_error_propagates():
    m = _recording_mapper(
        pipe_errors=[FileNotFoundError("1")], mkdirs_error=PermissionError("mk")
    )
    with pytest.raises(PermissionError, match="mk"):
        m.setitems({"x/a": b"1"})
    assert [c[0] for c in m.fs.calls] == ["pipe", "makedirs"]


@pytest.mark.parametrize("exc", [PermissionError, NotADirectoryError, ValueError])
def test_setitems_other_errors_not_retried(exc):
    m = _recording_mapper(
        pipe_errors=[exc("first")], mkdirs_error=AssertionError("no mkdirs")
    )
    with pytest.raises(exc, match="first"):
        m.setitems({"x/a": b"1"})
    assert [c[0] for c in m.fs.calls] == ["pipe"]


def test_setitems_local_transaction_parent_gap(tmp_path):
    # Known limitation, unchanged by GH#1041: in a local transaction pipe
    # defers to temp files, so a missing parent only fails at commit, after
    # setitems returned. __setitem__ creates parents up front and succeeds.
    m = _local_mapper(tmp_path, root_exists=True)
    with pytest.raises(FileNotFoundError):
        with m.fs.transaction:
            m.setitems({"txn/k": b"v"})
    assert "txn/k" not in m
    assert not m.fs._intrans
    with m.fs.transaction:
        m["txn/k"] = b"v"
    assert m["txn/k"] == b"v"
