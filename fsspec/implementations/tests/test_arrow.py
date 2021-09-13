import secrets

import pytest
from pyarrow.fs import FileSystem

from fsspec.implementations.arrow import ArrowFSWrapper


@pytest.fixture(scope="function")
def fs():
    fs, _ = FileSystem.from_uri("mock://")
    return ArrowFSWrapper(fs)


@pytest.fixture(scope="function")
def remote_dir(fs):
    directory = secrets.token_hex(16)
    fs.makedirs(directory)
    yield directory
    fs.rm(directory, recursive=True)


def strip_keys(original_entry):
    entry = original_entry.copy()
    entry.pop("mtime")
    return entry


def test_info(fs, remote_dir):
    fs.touch(remote_dir + "/a.txt")
    details = fs.info(remote_dir + "/a.txt")
    assert details["type"] == "file"
    assert details["name"] == remote_dir + "/a.txt"
    assert details["size"] == 0

    fs.mkdir(remote_dir + "/dir")
    details = fs.info(remote_dir + "/dir")
    assert details["type"] == "directory"
    assert details["name"] == remote_dir + "/dir"

    details = fs.info(remote_dir + "/dir/")
    assert details["name"] == remote_dir + "/dir/"


def test_move(fs, remote_dir):
    fs.touch(remote_dir + "/a.txt")
    initial_info = fs.info(remote_dir + "/a.txt")

    fs.move(remote_dir + "/a.txt", remote_dir + "/b.txt")
    secondary_info = fs.info(remote_dir + "/b.txt")

    assert not fs.exists(remote_dir + "/a.txt")
    assert fs.exists(remote_dir + "/b.txt")

    initial_info.pop("name")
    secondary_info.pop("name")
    assert initial_info == secondary_info


def test_copy(fs, remote_dir):
    fs.touch(remote_dir + "/a.txt")
    initial_info = fs.info(remote_dir + "/a.txt")

    fs.copy(remote_dir + "/a.txt", remote_dir + "/b.txt")
    secondary_info = fs.info(remote_dir + "/b.txt")

    assert fs.exists(remote_dir + "/a.txt")
    assert fs.exists(remote_dir + "/b.txt")

    initial_info.pop("name")
    secondary_info.pop("name")
    assert strip_keys(initial_info) == strip_keys(secondary_info)


def test_rm(fs, remote_dir):
    fs.touch(remote_dir + "/a.txt")
    fs.rm(remote_dir + "/a.txt", recursive=True)
    assert not fs.exists(remote_dir + "/a.txt")

    fs.mkdir(remote_dir + "/dir")
    fs.rm(remote_dir + "/dir", recursive=True)
    assert not fs.exists(remote_dir + "/dir")

    fs.mkdir(remote_dir + "/dir")
    fs.touch(remote_dir + "/dir/a")
    fs.touch(remote_dir + "/dir/b")
    fs.mkdir(remote_dir + "/dir/c/")
    fs.touch(remote_dir + "/dir/c/a/")
    fs.rm(remote_dir + "/dir", recursive=True)
    assert not fs.exists(remote_dir + "/dir")


def test_ls(fs, remote_dir):
    fs.mkdir(remote_dir + "dir/")
    files = set()
    for no in range(8):
        file = remote_dir + f"dir/test_{no}"
        fs.touch(file)
        files.add(file)

    assert set(fs.ls(remote_dir + "dir/")) == files

    dirs = fs.ls(remote_dir + "dir/", detail=True)
    expected = [fs.info(file) for file in files]

    by_name = lambda details: details["name"]
    dirs.sort(key=by_name)
    expected.sort(key=by_name)

    assert dirs == expected


def test_mkdir(fs, remote_dir):
    fs.mkdir(remote_dir + "dir/")
    assert fs.isdir(remote_dir + "dir/")
    assert len(fs.ls(remote_dir + "dir/")) == 0

    fs.mkdir(remote_dir + "dir/sub", create_parents=False)
    assert fs.isdir(remote_dir + "dir/sub")


def test_makedirs(fs, remote_dir):
    fs.makedirs(remote_dir + "dir/a/b/c/")
    assert fs.isdir(remote_dir + "dir/a/b/c/")
    assert fs.isdir(remote_dir + "dir/a/b/")
    assert fs.isdir(remote_dir + "dir/a/")

    fs.makedirs(remote_dir + "dir/a/b/c/", exist_ok=True)


def test_exceptions(fs, remote_dir):
    with pytest.raises(FileNotFoundError):
        with fs.open(remote_dir + "/a.txt"):
            ...

    with pytest.raises(FileNotFoundError):
        fs.copy(remote_dir + "/u.txt", remote_dir + "/y.txt")


def test_open_rw(fs, remote_dir):
    data = b"dvc.org"

    with fs.open(remote_dir + "/a.txt", "wb") as stream:
        stream.write(data)

    with fs.open(remote_dir + "/a.txt") as stream:
        assert stream.read() == data


def test_open_rw_flush(fs, remote_dir):
    data = b"dvc.org"

    with fs.open(remote_dir + "/b.txt", "wb") as stream:
        for _ in range(200):
            stream.write(data)
            stream.write(data)
            stream.flush()

    with fs.open(remote_dir + "/b.txt", "rb") as stream:
        assert stream.read() == data * 400
