import os

import pytest

from fsspec.implementations.local import LocalFileSystem, make_path_posix


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


def test_put_single(m, tmpdir):
    fn = os.path.join(str(tmpdir), "dir")
    os.mkdir(fn)
    open(os.path.join(fn, "abc"), "w").write("text")
    m.put(fn, "/test")  # no-op, no files
    assert m.isdir("/test")
    assert not m.exists("/test/abc")
    assert not m.exists("/test/dir")
    m.put(fn, "/test", recursive=True)
    assert m.isdir("/test/dir")
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
    assert m.ls("/dir/afile", False) == ["/dir/afile"]
    assert m.ls("/dir/afile", True)[0]["type"] == "file"

    assert len(m.ls("/dir/dir1")) == 2
    assert len(m.ls("/dir/afile")) == 1


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


def test_mv_same_paths(m):
    m.mkdir("src")
    m.touch("src/file.txt")
    m.mv("src", "src", recursive=True)
    assert m.exists("src/file.txt")


def test_rm_no_psuedo_dir(m):
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


def test_no_rewind_append_mode(m):
    # https://github.com/fsspec/filesystem_spec/issues/349
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


def test_get_directory_recursive(m, tmpdir):
    # https://github.com/fsspec/filesystem_spec/issues/1062
    # Recursive cp/get/put of source directory into non-existent target directory.
    src = "/src"
    src_file = src + "/file"
    m.mkdir(src)
    m.touch(src_file)

    target = os.path.join(tmpdir, "target")
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
    assert m.find(target, withdirs=True) == [target + "/empty"]

    m.rm(target + "/empty", recursive=True)

    # cp with slash, target directory exists
    assert m.isdir(target)
    m.cp(empty + "/", target)
    assert m.find(target, withdirs=True) == []

    m.rm(target, recursive=True)

    # cp without slash, target directory doesn't exist
    assert not m.isdir(target)
    m.cp(empty, target)
    assert m.isdir(target)
    assert m.find(target, withdirs=True) == []

    m.rm(target, recursive=True)

    # cp with slash, target directory doesn't exist
    assert not m.isdir(target)
    m.cp(empty + "/", target)
    assert m.isdir(target)
    assert m.find(target, withdirs=True) == []


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
