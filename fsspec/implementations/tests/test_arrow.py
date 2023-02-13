import secrets

import pytest

pyarrow_fs = pytest.importorskip("pyarrow.fs")
FileSystem = pyarrow_fs.FileSystem

from fsspec.implementations.arrow import ArrowFSWrapper  # noqa


@pytest.fixture(scope="function")
def fs():
    fs, _ = FileSystem.from_uri("mock://")
    return ArrowFSWrapper(fs)


@pytest.fixture(scope="function", params=[False, True])
def remote_dir(fs, request):
    directory = secrets.token_hex(16)
    fs.makedirs(directory)
    yield ("hdfs://" if request.param else "/") + directory
    fs.rm(directory, recursive=True)


def test_protocol():
    fs, _ = FileSystem.from_uri("mock://")
    fss = ArrowFSWrapper(fs)
    assert fss.protocol == "mock"


def strip_keys(original_entry):
    entry = original_entry.copy()
    entry.pop("mtime")
    return entry


def test_strip(fs):
    assert fs._strip_protocol("/a/file") == "/a/file"
    assert fs._strip_protocol("hdfs:///a/file") == "/a/file"
    assert fs._strip_protocol("hdfs://1.1.1.1/a/file") == "/a/file"
    assert fs._strip_protocol("hdfs://1.1.1.1:8888/a/file") == "/a/file"


def test_info(fs, remote_dir):
    fs.touch(remote_dir + "/a.txt")
    remote_dir_strip_protocol = fs._strip_protocol(remote_dir)
    details = fs.info(remote_dir + "/a.txt")
    assert details["type"] == "file"
    assert details["name"] == remote_dir_strip_protocol + "/a.txt"
    assert details["size"] == 0

    fs.mkdir(remote_dir + "/dir")
    details = fs.info(remote_dir + "/dir")
    assert details["type"] == "directory"
    assert details["name"] == remote_dir_strip_protocol + "/dir"

    details = fs.info(remote_dir + "/dir/")
    assert details["name"] == remote_dir_strip_protocol + "/dir/"


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


def test_move_recursive(fs, remote_dir):
    src = remote_dir + "/src"
    dest = remote_dir + "/dest"

    assert fs.isdir(src) is False
    fs.mkdir(src)
    assert fs.isdir(src)

    fs.touch(src + "/a.txt")
    fs.mkdir(src + "/b")
    fs.touch(src + "/b/c.txt")
    fs.move(src, dest, recursive=True)

    assert fs.isdir(src) is False
    assert not fs.exists(src)

    assert fs.isdir(dest)
    assert fs.exists(dest)
    assert fs.cat(dest + "/b/c.txt") == fs.cat(dest + "/a.txt") == b""


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
    fs.touch(remote_dir + "/dir/c/a")
    fs.rm(remote_dir + "/dir", recursive=True)
    assert not fs.exists(remote_dir + "/dir")


def test_ls(fs, remote_dir):
    if remote_dir != "/":
        remote_dir = remote_dir + "/"
    remote_dir_strip_protocol = fs._strip_protocol(remote_dir)
    fs.mkdir(remote_dir + "dir/")
    files = set()
    for no in range(8):
        file = remote_dir + f"dir/test_{no}"
        # we also want to make sure `fs.touch` works with protocol
        fs.touch(file)
        files.add(remote_dir_strip_protocol + f"dir/test_{no}")

    assert set(fs.ls(remote_dir + "dir/")) == files

    dirs = fs.ls(remote_dir + "dir/", detail=True)
    expected = [fs.info(file) for file in files]

    by_name = lambda details: details["name"]
    dirs.sort(key=by_name)
    expected.sort(key=by_name)

    assert dirs == expected


def test_mkdir(fs, remote_dir):
    if remote_dir != "/":
        remote_dir = remote_dir + "/"
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


def test_open_append(fs, remote_dir):
    data = b"dvc.org"

    with fs.open(remote_dir + "/a.txt", "wb") as stream:
        stream.write(data)

    with fs.open(remote_dir + "/a.txt", "ab") as stream:
        stream.write(data)

    with fs.open(remote_dir + "/a.txt") as stream:
        assert stream.read() == 2 * data


def test_open_seekable(fs, remote_dir):
    data = b"dvc.org"

    with fs.open(remote_dir + "/a.txt", "wb") as stream:
        stream.write(data)

    with fs.open(remote_dir + "/a.txt", "rb", seekable=True) as file:
        file.seek(2)
        assert file.read() == data[2:]


def test_seekable(fs, remote_dir):
    data = b"dvc.org"

    with fs.open(remote_dir + "/a.txt", "wb") as stream:
        stream.write(data)

    for seekable in [True, False]:
        with fs.open(remote_dir + "/a.txt", "rb", seekable=seekable) as file:
            assert file.seekable() == seekable
            assert file.read() == data

    with fs.open(remote_dir + "/a.txt", "rb", seekable=False) as file:
        with pytest.raises(IOError):
            file.seek(5)
