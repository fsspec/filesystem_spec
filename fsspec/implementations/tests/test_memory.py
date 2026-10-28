import os
import pickle
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

import fsspec
from fsspec import filesystem
from fsspec.config import conf
from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.implementations.memory import MemoryFileSystem


@pytest.mark.parametrize(
    "mode, method, args, expected",
    [
        ("ab", "write", (b"def",), b"abcdef"),
        ("r+b", "write", (b"XY",), b"XYc"),
        ("ab", "writelines", ([b"d", b"ef"],), b"abcdef"),
        ("r+b", "truncate", (2,), b"ab"),
    ],
)
def test_modified_after_write(m, monkeypatch, mode, method, args, expected):
    m.pipe_file("file", b"abc")
    created = m.created("file")
    modified = datetime(2100, 1, 1, tzinfo=UTC)

    class Clock:
        @staticmethod
        def now(tz):
            assert tz is UTC
            return modified

    with m.open("file", mode) as f:
        monkeypatch.setattr("fsspec.implementations.memory.datetime", Clock)
        getattr(f, method)(*args)

    assert m.cat_file("file") == expected
    assert m.modified("file") == modified
    assert m.created("file") == created


def test_modified_after_partial_writelines(m):
    m.pipe_file("file", b"abc")
    original = datetime(2000, 1, 1, tzinfo=UTC)

    with m.open("file", "ab") as f:
        f.modified = original
        with pytest.raises(TypeError):
            f.writelines([b"d", None])

    assert m.cat_file("file") == b"abcd"
    assert m.modified("file") > original


def test_modified_unchanged_without_write(m):
    m.pipe_file("file", b"abc")
    original = m.modified("file")

    with m.open("file", "r+b") as f:
        assert f.read() == b"abc"
        f.seek(0)
        assert f.write(b"") == 0
        assert f.writelines([]) is None
        with pytest.raises(TypeError):
            f.write(None)
        with pytest.raises(ValueError):
            f.truncate(-1)

    assert m.modified("file") == original


def test_independent_stores(m):
    first = filesystem("memory", global_store=False, skip_instance_cache=True)
    second = filesystem("memory", global_store=False, skip_instance_cache=True)
    for fs, data in [(m, b"global"), (first, b"first"), (second, b"second")]:
        fs.pipe("same/path", data)
        fs.mkdir("empty")

    assert m.cat("same/path") == b"global"
    assert first.cat("same/path") == b"first"
    assert second.cat("same/path") == b"second"

    first.rm("same", recursive=True)
    first.rmdir("empty")
    assert first.ls("") == []
    assert second.isdir("empty")
    assert m.isdir("empty")
    assert second.cat("same/path") == b"second"
    assert m.cat("same/path") == b"global"


def test_independent_store_identity():
    first = filesystem("memory", global_store=False, skip_instance_cache=True)
    second = filesystem("memory", global_store=False, skip_instance_cache=True)
    assert first != second
    assert len({first, second}) == 2
    assert first.__dask_tokenize__() != second.__dask_tokenize__()
    token = first.__dask_tokenize__()
    first.pipe("file", b"data")
    assert first.__dask_tokenize__() == token
    assert not MemoryFileSystem._cache


def test_independent_store_uses_instance_cache(m):
    fs = filesystem("memory", global_store=False)
    assert fs is filesystem("memory", global_store=False)
    fs.pipe("file", b"private")
    snapshot = pickle.dumps(fs)
    fs.pipe("file", b"changed after snapshot")
    restored = pickle.loads(snapshot)
    assert restored is not fs
    assert restored.cat("file") == b"private"
    assert fs.cat("file") == b"changed after snapshot"
    assert fs is filesystem("memory", global_store=False)
    assert not m.exists("file")


def test_default_store_is_shared(m):
    other = filesystem("memory")
    m.pipe("file", b"shared")
    m.mkdir("empty")
    assert other.cat("file") == b"shared"
    assert other.isdir("empty")
    assert other == m
    assert other is m


@pytest.mark.parametrize("use_mapper", [False, True])
@pytest.mark.parametrize("rollback", [False, True])
def test_default_store_transaction_helpers(m, use_mapper, rollback):
    def write():
        if use_mapper:
            fsspec.get_mapper("memory://")["file"] = b"data"
        else:
            with fsspec.open("memory://file", "wb") as f:
                f.write(b"data")
        assert not m.exists("file")

    if rollback:
        with pytest.raises(RuntimeError), m.transaction:
            write()
            raise RuntimeError("discard transaction")
        assert not m.exists("file")
    else:
        with m.transaction:
            write()
        assert m.cat("file") == b"data"


def test_independent_store_from_config(m, monkeypatch):
    monkeypatch.setitem(
        conf, "memory", {"global_store": False, "skip_instance_cache": True}
    )
    first = filesystem("memory")
    second = filesystem("memory", skip_instance_cache=False)
    first.pipe("file", b"private")
    assert not second.exists("file")
    assert second is filesystem("memory", skip_instance_cache=False)
    assert not m.exists("file")

    shared = filesystem("memory", global_store=True, skip_instance_cache=False)
    assert shared is filesystem("memory", global_store=True, skip_instance_cache=False)
    assert shared.store is m.store


def test_independent_store_transaction(m):
    fs = filesystem("memory", global_store=False, skip_instance_cache=True)
    with fs.transaction:
        fs.pipe("committed", b"data")
        assert not fs.exists("committed")
    assert fs.cat("committed") == b"data"
    assert not m.exists("committed")

    with pytest.raises(RuntimeError), fs.transaction:
        fs.pipe("discarded", b"data")
        raise RuntimeError("discard transaction")
    assert not fs.exists("discarded")
    assert not m.exists("discarded")


@pytest.mark.parametrize(
    "cache_options", [{}, {"skip_instance_cache": False}, {"skip_instance_cache": True}]
)
def test_independent_store_pickle(m, cache_options):
    fs = filesystem("memory", global_store=False, **cache_options)
    fs.pipe("file", b"original")
    fs.mkdir("empty")

    restored = pickle.loads(pickle.dumps(fs))
    assert restored is not fs
    assert restored.cat("file") == b"original"
    assert restored.isdir("empty")
    assert restored.store["/file"].fs is restored
    assert restored.info("file") == fs.info("file")

    with restored.open("file", "ab") as f:
        f.write(b" appended")
    restored.rmdir("empty")
    assert restored.cat("file") == b"original appended"
    assert fs.cat("file") == b"original"
    assert fs.isdir("empty")
    assert m.ls("") == []


def test_default_store_pickle(m):
    m.pipe("file", b"shared")
    restored = pickle.loads(pickle.dumps(m))
    assert restored is m
    assert restored.store is m.store
    assert restored.cat("file") == b"shared"


def test_1(m):
    m.touch("/somefile")  # NB: is found with or without initial /
    m.touch("afiles/and/another")
    files = m.find("")
    assert files == ["/afiles/and/another", "/somefile"]

    files = sorted(m.get_mapper())
    assert files == ["afiles/and/another", "somefile"]


def test_strip(m):
    assert m._strip_protocol("") == ""
    assert m._strip_protocol("memory://") == ""
    assert m._strip_protocol("afile") == "/afile"
    assert m._strip_protocol("/b/c") == "/b/c"
    assert m._strip_protocol("/b/c/") == "/b/c"


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
    assert m.ls("/dir/afile", False) == ["/dir/afile"]
    assert m.ls("/dir/afile", True)[0]["type"] == "file"

    assert len(m.ls("/dir/dir1")) == 2
    assert len(m.ls("/dir/afile")) == 1


def test_info_created_matches_ls(m):
    m.pipe_file("/dir/afile", b"data")

    info = m.info("/dir/afile")
    assert info["created"] == m.created("/dir/afile").timestamp()
    assert info == m.ls("/dir/afile")[0]
    assert info == m.find("/dir", detail=True)["/dir/afile"]


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


def test_exists_isdir_isfile(m):
    m.mkdir("/root")
    m.touch("/root/a")

    assert m.exists("/root")
    assert m.isdir("/root")
    assert not m.isfile("/root")

    assert m.exists("/root/a")
    assert m.isfile("/root/a")
    assert not m.isdir("/root/a")

    assert not m.exists("/root/not-exists")
    assert not m.isfile("/root/not-exists")
    assert not m.isdir("/root/not-exists")

    m.rm("/root/a")
    m.rmdir("/root")

    assert not m.exists("/root")

    m.touch("/a/b")
    assert m.isfile("/a/b")

    assert m.exists("/a")
    assert m.isdir("/a")
    assert not m.isfile("/a")


def test_touch(m):
    m.touch("/root/a")
    with pytest.raises(FileExistsError):
        m.touch("/root/a/b")
    with pytest.raises(FileExistsError):
        m.touch("/root/a/b/c")
    assert not m.exists("/root/a/b/")


@pytest.mark.parametrize("method", ["cp_file", "copy", "mv"])
@pytest.mark.parametrize("destination", ["/parent/child", "memory://parent/sub/child"])
def test_copy_rejects_file_parent(m, method, destination):
    m.pipe("/src", b"source")
    m.pipe("/parent", b"parent")
    store = m.store.copy()
    directories = m.pseudo_dirs.copy()

    with pytest.raises(FileExistsError):
        getattr(m, method)("/src", destination)

    assert m.store == store
    assert m.pseudo_dirs == directories
    assert m.cat("/src") == b"source"
    assert m.cat("/parent") == b"parent"
    assert not m.exists(destination)


@pytest.mark.parametrize("explicit", [False, True])
def test_cp_file_rejects_directory_destination(m, explicit):
    m.pipe("/src", b"source")
    if explicit:
        m.mkdir("/destination")
    else:
        m.pipe("/destination/child", b"child")
    store = m.store.copy()
    directories = m.pseudo_dirs.copy()

    with pytest.raises(IsADirectoryError):
        m.cp_file("/src", "/destination")

    assert m.store == store
    assert m.pseudo_dirs == directories
    assert m.isdir("/destination")
    assert not m.isfile("/destination")


def test_cp_directory_rejects_file_parent(m):
    m.mkdir("/src")
    m.pipe("/parent", b"parent")
    store = m.store.copy()
    directories = m.pseudo_dirs.copy()

    with pytest.raises(FileExistsError):
        m.cp_file("/src", "/parent/sub/destination")

    assert m.store == store
    assert m.pseudo_dirs == directories


def test_cp_directory_rejects_file_destination(m):
    m.mkdir("/src")
    m.pipe("/destination", b"destination")
    store = m.store.copy()
    directories = m.pseudo_dirs.copy()

    with pytest.raises(FileExistsError):
        m.cp_file("/src", "/destination")

    assert m.store == store
    assert m.pseudo_dirs == directories
    assert m.cat("/destination") == b"destination"


@pytest.mark.parametrize("method", ["cp_file", "copy"])
def test_copy_creates_missing_parents_and_overwrites_file(m, method):
    m.pipe("/src", b"source")
    copy = getattr(m, method)
    copy("/src", "/new/sub/destination")
    assert m.cat("/new/sub/destination") == b"source"
    m.pipe("/src", b"updated")
    copy("/src", "/new/sub/destination")
    assert m.cat("/new/sub/destination") == b"updated"
    assert m.cat("/src") == b"updated"


def test_copy_to_existing_directory(m):
    m.pipe("/src", b"source")
    m.mkdir("/destination")
    m.copy("/src", "/destination")
    assert m.cat("/destination/src") == b"source"
    m.mkdir("/empty")
    m.cp_file("/empty", "/new/empty")
    m.cp_file("/empty", "/new/empty")
    assert m.isdir("/new/empty")


def test_mv_recursive(m):
    m.mkdir("src")
    m.touch("src/file.txt")
    m.mv("src", "dest", recursive=True)
    assert m.exists("dest/file.txt")
    assert not m.exists("src")


def test_mv_recursive_maxdepth_keeps_deeper_files(m):
    m.pipe(
        {
            "/src/top.txt": b"top",
            "/src/sub/mid.txt": b"mid",
            "/src/sub/deeper/low.txt": b"low",
        }
    )
    m.mv("/src", "/dst", recursive=True, maxdepth=2)
    assert m.cat("/dst/top.txt") == b"top"
    assert m.cat("/dst/sub/mid.txt") == b"mid"
    assert not m.exists("/src/top.txt")
    assert not m.exists("/src/sub/mid.txt")
    # beyond maxdepth: not copied, so it must not be deleted either
    assert m.cat("/src/sub/deeper/low.txt") == b"low"


def test_mv_same_paths(m):
    m.mkdir("src")
    m.touch("src/file.txt")
    m.mv("src", "src", recursive=True)
    assert m.exists("src/file.txt")


def test_mv_recursive_propagates_cp_file_errors(m, monkeypatch):
    m.mkdir("src")
    m.touch("src/file.txt")

    def failing_cp_file(path1, path2, **kwargs):
        raise FileNotFoundError(path1)

    monkeypatch.setattr(m, "cp_file", failing_cp_file)

    with pytest.raises(FileNotFoundError):
        m.mv("/src", "/dst", recursive=True)
    assert m.exists("/src/file.txt")


def test_rm_no_pseudo_dir(m):
    m.touch("/dir1/dir2/file")
    m.rm("/dir1", recursive=True)
    assert not m.exists("/dir1/dir2/file")
    assert not m.exists("/dir1/dir2")
    assert not m.exists("/dir1")

    with pytest.raises(FileNotFoundError):
        m.rm("/dir1", recursive=True)


def test_rewind(m):
    # https://github.com/fsspec/filesystem_spec/issues/349
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


@pytest.mark.parametrize("path", ["afile", "/afile", "memory://afile"])
@pytest.mark.parametrize("exist_ok", [False, True])
@pytest.mark.parametrize("method", ["makedirs", "mkdirs"])
def test_makedirs_existing_file(m, path, exist_ok, method):
    m.pipe_file("/afile", b"payload")

    with pytest.raises(FileExistsError):
        getattr(m, method)(path, exist_ok=exist_ok)

    assert m.cat_file(path) == b"payload"
    assert not m.isdir(path)


@pytest.mark.parametrize(
    "path", ["missing/child", "/missing/child", "memory://missing/child"]
)
def test_mkdir_without_parents_missing(m, path):
    with pytest.raises(FileNotFoundError):
        m.mkdir(path, create_parents=False)

    assert m.pseudo_dirs == [""]
    assert m.find("", withdirs=True) == []


@pytest.mark.parametrize("parent", ["root", "explicit", "implicit"])
def test_mkdir_without_parents_existing(m, parent):
    if parent == "root":
        path = "/child"
    else:
        path = "/parent/child"
        if parent == "explicit":
            m.mkdir("/parent")
        else:
            m.pipe_file("/parent/file", b"payload")

    m.mkdir(path, create_parents=False)

    assert path in m.pseudo_dirs
    assert m.isdir(path)


def test_no_rewind_append_mode(m):
    # https://github.com/fsspec/filesystem_spec/issues/349
    with m.open("src/file.txt", "w") as f:
        f.write("content")
    with m.open("src/file.txt", "a") as f:
        assert f.tell() == 7


@pytest.mark.parametrize("mode", ["a", "ab", "a+b"])
def test_append_creates_missing_file(m, mode):
    # append modes create the file if it does not exist, matching builtin
    # open() and LocalFileSystem
    filename = "newfile.txt"
    assert not m.exists(filename)
    with m.open(filename, mode) as f:
        # append mode must position at the end of the (newly created) file
        assert f.tell() == f.seek(0, 2)
        f.write(b"data" if "b" in mode else "data")
    assert m.cat(filename) == b"data"


@pytest.mark.parametrize("global_store", [False, True])
@pytest.mark.parametrize("mode", ["ab", "a+b"])
@pytest.mark.parametrize("rollback", [False, True])
def test_transaction_append_existing_file(m, global_store, mode, rollback):
    fs = filesystem("memory", global_store=global_store, skip_instance_cache=True)
    fs.pipe_file("file", b"original")

    with pytest.raises(RuntimeError) if rollback else nullcontext():
        with fs.transaction:
            with fs.open("file", mode) as f:
                f.write(b"-appended")
            assert fs.cat_file("file") == b"original"
            if rollback:
                raise RuntimeError("discard transaction")

    expected = b"original" if rollback else b"original-appended"
    assert fs.cat_file("file") == expected


def test_moves(m):
    m.touch("source.txt")
    m.mv("source.txt", "target.txt")

    m.touch("source2.txt")
    m.mv("source2.txt", "target2.txt", recursive=True)
    assert m.find("") == ["/target.txt", "/target2.txt"]


def test_rm_reursive_empty_subdir(m):
    # https://github.com/fsspec/filesystem_spec/issues/500
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


# https://github.com/fsspec/filesystem_spec/issues/1425
@pytest.mark.parametrize("mode", ["r", "rb", "w", "wb", "ab", "r+b"])
def test_open_mode(m, mode):
    filename = "mode.txt"
    m.touch(filename)
    with m.open(filename, mode=mode) as _:
        pass


@pytest.mark.parametrize(
    "mode", ["r+b", "rb+", "w+b", "wb+", "a+b", "ab+", "x+b", "xb+"]
)
@pytest.mark.parametrize("exists", [False, True])
def test_binary_update_modes(m, mode, exists):
    path = "update.txt"
    if exists:
        m.pipe_file(path, b"abc")
    if mode.startswith("r") and not exists:
        with pytest.raises(FileNotFoundError):
            m.open(path, mode)
        return
    if mode.startswith("x") and exists:
        with pytest.raises(FileExistsError):
            m.open(path, mode)
        assert m.cat_file(path) == b"abc"
        return
    expected = b"abcXY" if mode.startswith("a") and exists else b"XY"
    if mode.startswith("r"):
        expected = b"XYc"
    with m.open(path, mode) as f:
        assert f.tell() == (3 if mode.startswith("a") and exists else 0)
        f.write(b"XY")
        f.seek(0)
        assert f.read() == expected
    assert m.cat_file(path) == expected


@pytest.mark.parametrize("mode", ["rbb+", "rw+b", "rb++"])
def test_invalid_update_modes(m, mode):
    m.pipe_file("update.txt", b"abc")
    with pytest.raises(ValueError, match="unsupported file mode"):
        m.open("update.txt", mode)
    assert m.cat_file("update.txt") == b"abc"


def test_remove_all(m):
    m.touch("afile")
    m.rm("/", recursive=True)
    assert not m.ls("/")


def test_cp_directory_recursive(m):
    # https://github.com/fsspec/filesystem_spec/issues/1062
    # Recursive cp/get/put of source directory into non-existent target directory.
    src = "/src"
    src_file = src + "/file"
    m.mkdir(src)
    m.touch(src_file)

    target = "/target"

    # cp without slash
    assert not m.exists(target)
    for loop in range(2):
        m.cp(src, target, recursive=True)
        assert m.isdir(target)

        if loop == 0:
            correct = [target + "/file"]
            assert m.find(target) == correct
        else:
            correct = [target + "/file", target + "/src/file"]
            assert sorted(m.find(target)) == correct

    m.rm(target, recursive=True)

    # cp with slash
    assert not m.exists(target)
    for loop in range(2):
        m.cp(src + "/", target, recursive=True)
        assert m.isdir(target)
        correct = [target + "/file"]
        assert m.find(target) == correct


class _LocalPathLike:
    def __init__(self, path):
        self.path = path

    def __fspath__(self):
        return str(self.path)


@pytest.mark.parametrize("path_type", [str, Path, _LocalPathLike])
@pytest.mark.parametrize("source_kind", ["file", "directory", "glob", "list", "pairs"])
def test_get_local_pathlike(m, tmp_path, path_type, source_kind):
    m.pipe({"/src/a": b"a", "/src/b": b"b"})
    target = tmp_path / "target"
    if source_kind == "file":
        source = "/src/a"
        destination = path_type(target / "renamed")
        expected = {"renamed": b"a"}
    else:
        target.mkdir()
        source = "/src/a" if source_kind == "directory" else "/src/*"
        destination = path_type(target)
        expected = {"a": b"a"}
        if source_kind in ("glob", "list", "pairs"):
            expected["b"] = b"b"
        if source_kind in ("list", "pairs"):
            source = ["/src/a", "/src/b"]
        if source_kind == "pairs":
            destination = [path_type(target / "a"), path_type(target / "b")]

    m.get(source, destination)

    assert {
        path.relative_to(target).as_posix(): path.read_bytes()
        for path in target.rglob("*")
        if path.is_file()
    } == expected


@pytest.mark.parametrize("path_type", [str, Path, _LocalPathLike])
def test_get_directory_recursive(m, tmpdir, path_type):
    # https://github.com/fsspec/filesystem_spec/issues/1062
    # Recursive cp/get/put of source directory into non-existent target directory.
    src = "/src"
    src_file = src + "/file"
    m.mkdir(src)
    m.touch(src_file)

    target = path_type(os.path.join(tmpdir, "target"))
    target_fs = LocalFileSystem()

    # get without slash
    assert not target_fs.exists(target)
    for loop in range(2):
        m.get(src, target, recursive=True)
        assert target_fs.isdir(target)

        if loop == 0:
            correct = [make_path_posix(os.path.join(target, "file"))]
            assert target_fs.find(target) == correct
        else:
            correct = [
                make_path_posix(os.path.join(target, "file")),
                make_path_posix(os.path.join(target, "src", "file")),
            ]
            assert sorted(target_fs.find(target)) == correct

    target_fs.rm(target, recursive=True)

    # get with slash
    assert not target_fs.exists(target)
    for loop in range(2):
        m.get(src + "/", target, recursive=True)
        assert target_fs.isdir(target)
        correct = [make_path_posix(os.path.join(target, "file"))]
        assert target_fs.find(target) == correct


def test_put_directory_recursive(m, tmpdir):
    # https://github.com/fsspec/filesystem_spec/issues/1062
    # Recursive cp/get/put of source directory into non-existent target directory.
    src = os.path.join(tmpdir, "src")
    src_file = os.path.join(src, "file")
    source_fs = LocalFileSystem()
    source_fs.mkdir(src)
    source_fs.touch(src_file)

    target = "/target"

    # put without slash
    assert not m.exists(target)
    for loop in range(2):
        m.put(src, target, recursive=True)
        assert m.isdir(target)

        if loop == 0:
            correct = [target + "/file"]
            assert m.find(target) == correct
        else:
            correct = [target + "/file", target + "/src/file"]
            assert sorted(m.find(target)) == correct

    m.rm(target, recursive=True)

    # put with slash
    assert not m.exists(target)
    for loop in range(2):
        m.put(src + "/", target, recursive=True)
        assert m.isdir(target)
        correct = [target + "/file"]
        assert m.find(target) == correct


def test_cp_empty_directory(m):
    # https://github.com/fsspec/filesystem_spec/issues/1198
    # cp/get/put of empty directory.
    empty = "/src/empty"
    m.mkdir(empty)

    target = "/target"
    m.mkdir(target)

    # cp without slash, target directory exists
    assert m.isdir(target)
    m.cp(empty, target)
    assert m.find(target, withdirs=True) == [target]

    # cp with slash, target directory exists
    assert m.isdir(target)
    m.cp(empty + "/", target)
    assert m.find(target, withdirs=True) == [target]

    m.rmdir(target)

    # cp without slash, target directory doesn't exist
    assert not m.isdir(target)
    m.cp(empty, target)
    assert not m.isdir(target)

    # cp with slash, target directory doesn't exist
    assert not m.isdir(target)
    m.cp(empty + "/", target)
    assert not m.isdir(target)


def test_cp_two_files(m):
    src = "/src"
    file0 = src + "/file0"
    file1 = src + "/file1"
    m.mkdir(src)
    m.touch(file0)
    m.touch(file1)

    target = "/target"
    assert not m.exists(target)

    m.cp([file0, file1], target)

    assert m.isdir(target)
    assert sorted(m.find(target)) == [
        "/target/file0",
        "/target/file1",
    ]


def test_open_path_posix(m):
    path = PurePosixPath("/myfile/foo/bar")
    with m.open(path, "wb") as f:
        f.write(b"some\nlines\nof\ntext")

    assert m.read_text(path) == "some\nlines\nof\ntext"


def test_open_path_windows(m):
    path = PureWindowsPath("C:\\myfile\\foo\\bar")
    with m.open(path, "wb") as f:
        f.write(b"some\nlines\nof\ntext")

    assert m.read_text(path) == "some\nlines\nof\ntext"


def test_find_matches_generic(m):
    # MemoryFileSystem overrides find() with a single-pass implementation; make
    # sure it agrees with the generic ls()-based AbstractFileSystem.find() across
    # roots, maxdepth, withdirs and detail.
    from fsspec.spec import AbstractFileSystem

    for path in [
        "/data/a/f1.txt",
        "/data/a/f2.txt",
        "/data/a/b/deep.txt",
        "/data/a/b/c/deepest.txt",
        "/data/x.txt",
        "/data/y/z.txt",
        "/other/o.txt",
    ]:
        m.pipe_file(path, b"hello")
    m.mkdir("/data/emptydir")  # empty (pseudo) directory
    m.mkdir("/data/a/b/emptysub")

    for root in ["", "/data", "/data/a", "/data/a/b", "/data/x.txt", "/nope"]:
        for maxdepth in [None, 1, 2, 3]:
            for withdirs in [False, True]:
                for detail in [False, True]:
                    got = m.find(
                        root, maxdepth=maxdepth, withdirs=withdirs, detail=detail
                    )
                    expected = AbstractFileSystem.find(
                        m, root, maxdepth=maxdepth, withdirs=withdirs, detail=detail
                    )
                    assert got == expected, (root, maxdepth, withdirs, detail)


@pytest.mark.parametrize("root", ["", "/data"])
@pytest.mark.parametrize("maxdepth", [1, 2, 5, None])
@pytest.mark.parametrize("detail", [False, True])
def test_find_deep_ancestors(m, root, maxdepth, detail):
    from fsspec.spec import AbstractFileSystem

    deep = "/data/" + "/".join(f"level{i}" for i in range(40))
    m.pipe_file(deep + "/file", b"content")
    m.mkdir(deep + "/empty")
    m.pipe_file("/database/unrelated", b"other")
    assert m.find(root, maxdepth=maxdepth, withdirs=True, detail=detail) == (
        AbstractFileSystem.find(
            m, root, maxdepth=maxdepth, withdirs=True, detail=detail
        )
    )


def test_find_snapshots_store_before_iterating(m):
    # `store` is a class attribute shared by every MemoryFileSystem instance, so
    # another instance can add or remove a path while find() is walking it.
    # ls() iterates over a snapshot for this reason; find() must too, or it dies
    # with "dictionary changed size during iteration".
    for f in range(5):
        m.pipe_file(f"/data/file{f}.txt", b"x")

    class MutatesStoreOnRead:
        """Stands in for a real entry, but writes to the store when read.

        Reading ``size`` happens inside find()'s loop, so this reproduces a
        concurrent write landing mid-iteration without needing a second thread.
        """

        def __init__(self, real, store):
            self._real = real
            self._store = store
            self._fired = False

        @property
        def created(self):
            return self._real.created

        @property
        def size(self):
            if not self._fired:
                self._fired = True
                self._store["/data/concurrent.txt"] = self._real
            return self._real.size

    real = m.store["/data/file0.txt"]
    m.store["/data/file0.txt"] = MutatesStoreOnRead(real, m.store)

    out = m.find("/data")

    # The snapshot is taken before the concurrent write, so the new path is not
    # part of this result; the point is that find() completes instead of raising.
    assert "/data/file4.txt" in out
    assert len(out) == 5


def test_find_does_not_scan_per_directory(m):
    # Regression guard: the old find() called ls() once per directory and each
    # ls() re-scanned the whole (global) store, giving O(n_dirs * n_files) work.
    # The single-pass implementation must not call ls() at all, so total work
    # stays O(n_files) regardless of how many directories there are.
    from unittest import mock

    for d in range(20):
        for f in range(5):
            m.pipe_file(f"/data/dir{d}/file{f}.txt", b"x")

    with mock.patch.object(MemoryFileSystem, "ls", wraps=m.ls) as spy:
        out = m.find("/data")

    assert len(out) == 100
    assert spy.call_count == 0


def test_rm_missing_path_raises(m):
    with pytest.raises(FileNotFoundError):
        m.rm("/missing")


def test_rm_list_with_missing_path_raises(m):
    m.pipe("/present", b"data")
    with pytest.raises(FileNotFoundError):
        m.rm(["/present", "/missing"])


def test_rm_recursive_still_removes_implicit_parents(m):
    # Files written without their parent directories: the parents only exist
    # while those files do, and vanish partway through a recursive delete.
    m.pipe("/implicit/nested/file", b"data")
    m.rm("/implicit", recursive=True)
    assert not m.exists("/implicit")


@pytest.mark.parametrize("explicit_dirs", [False, True])
def test_rm_recursive_maxdepth_keeps_deeper_files(m, explicit_dirs):
    if explicit_dirs:
        m.mkdir("/src/sub/deeper")
    m.pipe(
        {
            "/src/top.txt": b"top",
            "/src/sub/mid.txt": b"mid",
            "/src/sub/deeper/low.txt": b"low",
        }
    )
    m.rm("/src", recursive=True, maxdepth=2)
    assert not m.exists("/src/top.txt")
    assert not m.exists("/src/sub/mid.txt")
    # beyond maxdepth, so it is kept along with its parent directories
    assert m.cat("/src/sub/deeper/low.txt") == b"low"

    m.rm("/src", recursive=True, maxdepth=3)
    assert not m.exists("/src")


def test_mapper_delitem_missing_key_raises_keyerror(m):
    mapper = m.get_mapper("/mapper")
    mapper["present"] = b"data"
    del mapper["present"]
    with pytest.raises(KeyError):
        del mapper["missing"]


@pytest.mark.parametrize("mode", ["rb", "wb", "ab"])
def test_open_implicit_directory_raises(m, mode):
    # "/dir" exists only because a file lives under it
    m.pipe("/dir/file", b"data")
    with pytest.raises(IsADirectoryError):
        m.open("/dir", mode)
    assert m.isdir("/dir")
    assert m.cat("/dir/file") == b"data"
