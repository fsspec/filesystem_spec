import pickle

import pytest

import fsspec
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.implementations.mount import MountFileSystem


@pytest.fixture
def mfs(m):
    m.pipe(
        {
            "/one/a.txt": b"a",
            "/one/sub/b.txt": b"bb",
            "/two/c.txt": b"ccc",
        }
    )
    return MountFileSystem({"/data/one": "memory://one", "/two": (m, "/two")})


@pytest.fixture
def local_mfs(mfs, tmp_path):
    mfs.mount("/local", tmp_path.as_posix())
    return mfs


def test_registered(m):
    assert fsspec.get_filesystem_class("mount") is MountFileSystem
    m.pipe("/one/a.txt", b"a")
    fs = fsspec.filesystem("mount", mounts={"/m": "memory://one"})
    assert fs.cat_file("/m/a.txt") == b"a"
    # mounts can change, so instances are never shared
    assert fsspec.filesystem("mount", mounts={"/m": "memory://one"}) is not fs


def test_mounts(mfs):
    assert sorted(mfs.mounts) == ["/data/one", "/two"]
    assert all(isinstance(fs, MemoryFileSystem) for fs in mfs.mounts.values())


def test_ls_virtual_directories(mfs):
    assert mfs.ls("/", detail=False) == ["/data", "/two"]
    assert mfs.ls("/data") == [{"name": "/data/one", "size": 0, "type": "directory"}]
    assert mfs.ls("mount://data/", detail=False) == ["/data/one"]
    with pytest.raises(FileNotFoundError):
        mfs.ls("/missing")
    with pytest.raises(FileNotFoundError):
        mfs.ls("/dat")


def test_ls_inside_mount(mfs):
    assert sorted(mfs.ls("/data/one", detail=False)) == [
        "/data/one/a.txt",
        "/data/one/sub",
    ]
    [entry] = mfs.ls("/two")
    assert entry["name"] == "/two/c.txt"
    assert entry["size"] == 3
    assert entry["type"] == "file"


def test_empty():
    fs = MountFileSystem()
    assert fs.ls("/") == []
    assert fs.find("/") == []
    assert fs.info("/")["type"] == "directory"


def test_info(mfs):
    assert mfs.info("/") == {"name": "/", "size": 0, "type": "directory"}
    assert mfs.info("/data") == {"name": "/data", "size": 0, "type": "directory"}
    assert mfs.info("/data/one")["type"] == "directory"
    info = mfs.info("/data/one/sub/b.txt")
    assert (info["name"], info["size"], info["type"]) == (
        "/data/one/sub/b.txt",
        2,
        "file",
    )
    assert mfs.isdir("/data/one/sub")
    assert mfs.isfile("/two/c.txt")
    assert not mfs.exists("/data/one/missing")
    assert not mfs.exists("/elsewhere")
    with pytest.raises(FileNotFoundError):
        mfs.info("/elsewhere")


def test_paths_are_normalized(mfs):
    assert mfs.cat_file("data//one/./sub/../a.txt") == b"a"
    assert mfs.cat_file("mount://two/c.txt") == b"ccc"
    assert mfs.ls("/two/", detail=False) == ["/two/c.txt"]


def test_read(mfs):
    assert mfs.cat_file("/data/one/sub/b.txt") == b"bb"
    assert mfs.cat_file("/two/c.txt", start=1, end=-1) == b"c"
    assert mfs.cat(["/data/one/a.txt", "/two/c.txt"]) == {
        "/data/one/a.txt": b"a",
        "/two/c.txt": b"ccc",
    }
    with mfs.open("/two/c.txt", "rb") as f:
        assert f.read() == b"ccc"
    with mfs.open("/two/c.txt", "rt") as f:
        assert f.read() == "ccc"
    assert mfs.head("/two/c.txt", 2) == b"cc"


@pytest.mark.parametrize(
    "path, exc",
    [
        ("/data/one/missing", FileNotFoundError),
        ("/elsewhere/file", FileNotFoundError),
        ("/data", IsADirectoryError),
        ("/data/one", IsADirectoryError),
        ("/", IsADirectoryError),
    ],
)
def test_read_errors(mfs, path, exc):
    with pytest.raises(exc):
        mfs.cat_file(path)
    with pytest.raises(exc):
        mfs.open(path, "rb")


def test_write(mfs, m):
    mfs.pipe_file("/data/one/new.txt", b"new")
    assert m.cat_file("/one/new.txt") == b"new"

    with mfs.open("/two/d.txt", "wb") as f:
        f.write(b"dd")
    assert m.cat_file("/two/d.txt") == b"dd"

    mfs.touch("/two/empty")
    assert m.cat_file("/two/empty") == b""

    mfs.makedirs("/data/one/deep/er", exist_ok=True)
    mfs.pipe_file("/data/one/deep/er/f", b"f")
    assert m.cat_file("/one/deep/er/f") == b"f"

    mfs.rm_file("/two/d.txt")
    assert not m.exists("/two/d.txt")
    mfs.rm("/data/one/deep", recursive=True)
    assert not m.exists("/one/deep/er/f")


@pytest.mark.parametrize("path", ["/elsewhere/file", "/data", "/data/one", "/"])
def test_cannot_write_outside_mounts(mfs, path):
    with pytest.raises(PermissionError):
        mfs.pipe_file(path, b"x")
    with pytest.raises(PermissionError):
        mfs.open(path, "wb")
    with pytest.raises(PermissionError):
        mfs.touch(path)


@pytest.mark.parametrize("path", ["/data/one", "/data", "/"])
def test_cannot_remove_mount_points(mfs, m, path):
    with pytest.raises(PermissionError):
        mfs.rm(path, recursive=True)
    with pytest.raises(PermissionError):
        mfs.rmdir(path)
    assert m.cat_file("/one/a.txt") == b"a"


def test_mkdir(mfs, m):
    mfs.mkdir("/data/one/newdir")
    assert m.isdir("/one/newdir")
    with pytest.raises(FileExistsError):
        mfs.mkdir("/data")
    with pytest.raises(FileExistsError):
        mfs.makedirs("/data/one")
    mfs.makedirs("/data", exist_ok=True)
    mfs.makedirs("/data/one", exist_ok=True)
    with pytest.raises(PermissionError):
        mfs.mkdir("/elsewhere")
    with pytest.raises(PermissionError):
        mfs.makedirs("/elsewhere/deeper", exist_ok=True)


def test_find(mfs):
    assert mfs.find("/") == ["/data/one/a.txt", "/data/one/sub/b.txt", "/two/c.txt"]
    assert mfs.find("/data") == ["/data/one/a.txt", "/data/one/sub/b.txt"]
    assert mfs.find("/data/one/sub") == ["/data/one/sub/b.txt"]
    assert mfs.find("/", withdirs=True) == [
        "/data",
        "/data/one",
        "/data/one/a.txt",
        "/data/one/sub",
        "/data/one/sub/b.txt",
        "/two",
        "/two/c.txt",
    ]
    assert mfs.find("/missing") == []
    detail = mfs.find("/two", detail=True)
    assert list(detail) == ["/two/c.txt"]
    assert detail["/two/c.txt"]["name"] == "/two/c.txt"
    assert detail["/two/c.txt"]["size"] == 3


@pytest.mark.parametrize("withdirs", [False, True])
@pytest.mark.parametrize("maxdepth", [None, 1, 2, 3, 4])
@pytest.mark.parametrize("path", ["/", "/data", "/data/one", "/two"])
def test_find_matches_a_single_filesystem(mfs, m, path, maxdepth, withdirs):
    # The same tree laid out on one filesystem gives the reference answer.
    m.pipe(
        {
            "/mirror/data/one/a.txt": b"a",
            "/mirror/data/one/sub/b.txt": b"bb",
            "/mirror/two/c.txt": b"ccc",
        }
    )
    mirror = "/mirror" + path.rstrip("/")
    expected = [
        name[len("/mirror") :]
        for name in m.find(mirror, maxdepth=maxdepth, withdirs=withdirs)
        if name != "/mirror"
    ]
    assert mfs.find(path, maxdepth=maxdepth, withdirs=withdirs) == expected


def test_glob_walk_du(mfs):
    assert mfs.glob("/**/*.txt") == [
        "/data/one/a.txt",
        "/data/one/sub/b.txt",
        "/two/c.txt",
    ]
    assert mfs.glob("/*") == ["/data", "/two"]
    assert mfs.glob("/data/one/*.txt") == ["/data/one/a.txt"]
    assert mfs.du("/") == 6
    assert mfs.du("/data", total=False) == {
        "/data/one/a.txt": 1,
        "/data/one/sub/b.txt": 2,
    }
    assert next(iter(mfs.walk("/"))) == ("/", ["data", "two"], [])


def test_copy_within_one_filesystem(mfs, m, mocker):
    spy = mocker.spy(m, "cp_file")
    mfs.cp("/data/one/a.txt", "/data/one/copy.txt")
    spy.assert_called_once_with("/one/a.txt", "/one/copy.txt")
    assert m.cat_file("/one/copy.txt") == b"a"


def test_copy_between_filesystems(local_mfs, tmp_path):
    local_mfs.cp("/data/one", "/local/copied", recursive=True)
    assert (tmp_path / "copied" / "a.txt").read_bytes() == b"a"
    assert (tmp_path / "copied" / "sub" / "b.txt").read_bytes() == b"bb"

    local_mfs.cp("/local/copied/sub/b.txt", "/two/from_local.txt")
    assert local_mfs.cat_file("/two/from_local.txt") == b"bb"


def test_move_within_one_filesystem(mfs, m, mocker):
    spy = mocker.spy(m, "mv")
    mfs.mv("/two/c.txt", "/data/one/moved.txt")
    spy.assert_called_once()
    assert m.cat_file("/one/moved.txt") == b"ccc"
    assert not m.exists("/two/c.txt")


def test_move_between_filesystems(local_mfs, m, tmp_path):
    local_mfs.mv("/data/one/sub", "/local/moved", recursive=True)
    assert (tmp_path / "moved" / "b.txt").read_bytes() == b"bb"
    assert not m.exists("/one/sub/b.txt")


def test_get_and_put(mfs, m, tmp_path):
    mfs.get("/data/one", (tmp_path / "got").as_posix(), recursive=True)
    assert (tmp_path / "got" / "sub" / "b.txt").read_bytes() == b"bb"

    mfs.get("/two/c.txt", (tmp_path / "c.txt").as_posix())
    assert (tmp_path / "c.txt").read_bytes() == b"ccc"

    mfs.put((tmp_path / "got").as_posix(), "/two/put", recursive=True)
    assert m.cat_file("/two/put/sub/b.txt") == b"bb"

    with pytest.raises(PermissionError):
        mfs.put_file((tmp_path / "c.txt").as_posix(), "/elsewhere/c.txt")


def test_get_whole_tree(mfs, tmp_path):
    target = tmp_path / "everything"
    mfs.get("/", target.as_posix(), recursive=True)
    assert (target / "data" / "one" / "a.txt").read_bytes() == b"a"
    assert (target / "data" / "one" / "sub" / "b.txt").read_bytes() == b"bb"
    assert (target / "two" / "c.txt").read_bytes() == b"ccc"


def test_cat_ranges(mfs):
    out = mfs.cat_ranges(
        ["/two/c.txt", "/data/one/sub/b.txt", "/elsewhere", "/two/c.txt"],
        [0, 1, 0, 1],
        [1, 2, 1, None],
    )
    assert out[0] == b"c"
    assert out[1] == b"b"
    assert isinstance(out[2], FileNotFoundError)
    assert out[3] == b"cc"

    with pytest.raises(FileNotFoundError):
        mfs.cat_ranges(["/two/c.txt", "/elsewhere"], [0, 0], [1, 1], on_error="raise")


def test_cat_ranges_batches_each_filesystem(mfs, m, mocker):
    spy = mocker.spy(m, "cat_ranges")
    assert mfs.cat_ranges(["/two/c.txt", "/data/one/a.txt"], 0, 1) == [b"c", b"a"]
    spy.assert_called_once()
    assert spy.call_args.args[0] == ["/two/c.txt", "/one/a.txt"]


def test_mount_validation(m):
    fs = MountFileSystem()
    with pytest.raises(ValueError, match="root"):
        fs.mount("/", m)
    fs.mount("/a/b", m)
    for point in ["/a/b", "/a", "/a/b/c", "a/b/"]:
        with pytest.raises(ValueError, match="overlaps"):
            fs.mount(point, m)
    # a sibling sharing a prefix of the name does not overlap
    fs.mount("/a/bc", m)
    with pytest.raises(ValueError, match="itself"):
        fs.mount("/self", fs)
    with pytest.raises(TypeError):
        fs.mount("/x", object())
    with pytest.raises(TypeError, match="Storage options"):
        fs.mount("/x", m, auto_mkdir=True)
    with pytest.raises(ValueError, match="root"):
        fs.mount("/x", "memory://x", root="/y")


def test_unmount(mfs, m):
    assert mfs.unmount("/two/") is m
    assert mfs.ls("/", detail=False) == ["/data"]
    with pytest.raises(FileNotFoundError):
        mfs.cat_file("/two/c.txt")
    with pytest.raises(ValueError, match="No filesystem"):
        mfs.unmount("/two")
    mfs.mount("/two", m, root="/two")
    assert mfs.cat_file("/two/c.txt") == b"ccc"


def test_mount_url_with_storage_options(tmp_path):
    fs = MountFileSystem()
    fs.mount("/local", (tmp_path / "new").as_posix(), auto_mkdir=True)
    fs.pipe_file("/local/deep/file", b"data")
    assert (tmp_path / "new" / "deep" / "file").read_bytes() == b"data"
    assert fs.ls("/local/deep", detail=False) == ["/local/deep/file"]


def test_nested_mount_filesystem(mfs):
    outer = MountFileSystem({"/inner": mfs})
    assert outer.ls("/inner", detail=False) == ["/inner/data", "/inner/two"]
    assert outer.cat_file("/inner/data/one/a.txt") == b"a"
    assert outer.find("/") == [
        "/inner/data/one/a.txt",
        "/inner/data/one/sub/b.txt",
        "/inner/two/c.txt",
    ]


def test_pickle_and_json_roundtrip(mfs, m):
    mfs.mount("/later", m, root="/one/sub")
    restored_fs = [
        pickle.loads(pickle.dumps(mfs)),
        fsspec.AbstractFileSystem.from_json(mfs.to_json()),
    ]
    for restored in restored_fs:
        assert isinstance(restored, MountFileSystem)
        assert sorted(restored.mounts) == ["/data/one", "/later", "/two"]
        assert restored.cat_file("/later/b.txt") == b"bb"
        assert restored.cat_file("/data/one/a.txt") == b"a"


def test_invalidate_cache(mfs, m, mocker):
    spy = mocker.spy(m, "invalidate_cache")
    mfs.invalidate_cache("/two/c.txt")
    spy.assert_called_once_with("/two/c.txt")

    spy.reset_mock()
    mfs.invalidate_cache("/data")
    spy.assert_called_once_with("/one")

    spy.reset_mock()
    mfs.invalidate_cache()
    assert sorted(call.args[0] for call in spy.call_args_list) == ["/one", "/two"]


def test_transaction(local_mfs, tmp_path):
    # Files opened for writing inside a transaction are committed at its end
    # by the filesystem they belong to.
    with local_mfs.transaction:
        with local_mfs.open("/local/txn.txt", "wb") as f:
            f.write(b"t")
        assert not (tmp_path / "txn.txt").exists()
    assert (tmp_path / "txn.txt").read_bytes() == b"t"
