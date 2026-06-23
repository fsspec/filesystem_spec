import pickle
import shlex
import subprocess
import time
from datetime import datetime

import pytest

import fsspec

requests = pytest.importorskip("requests")

from fsspec.implementations.webhdfs import WebHDFS  # noqa: E402


@pytest.fixture(scope="module")
def hdfs_cluster():
    cmd0 = shlex.split("htcluster shutdown")
    try:
        subprocess.check_output(cmd0, stderr=subprocess.STDOUT)
    except FileNotFoundError:
        pytest.skip("htcluster not found")
    except subprocess.CalledProcessError as ex:
        pytest.skip(f"htcluster failed: {ex.output.decode()}")
    cmd1 = shlex.split("htcluster startup --image base")
    subprocess.check_output(cmd1)
    try:
        while True:
            t = 90
            try:
                requests.get("http://localhost:50070/webhdfs/v1/?op=LISTSTATUS")
            except:  # noqa: E722
                t -= 1
                assert t > 0, "Timeout waiting for HDFS"
                time.sleep(1)
                continue
            break
        time.sleep(7)
        yield "localhost"
    finally:
        subprocess.check_output(cmd0)


def test_pickle(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user="testuser")
    w2 = pickle.loads(pickle.dumps(w))
    assert w == w2


def test_simple(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user="testuser")
    home = w.home_directory()
    assert home == "/user/testuser"
    with pytest.raises(PermissionError):
        w.mkdir("/root")


def test_url(hdfs_cluster):
    url = "webhdfs://testuser@localhost:50070/user/testuser/myfile"
    fo = fsspec.open(url, "wb", data_proxy={"worker.example.com": "localhost"})
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open(url, "rb", data_proxy={"worker.example.com": "localhost"})
    with fo as f:
        assert f.read() == b"hello"


def test_workflow(hdfs_cluster):
    w = WebHDFS(
        hdfs_cluster, user="testuser", data_proxy={"worker.example.com": "localhost"}
    )
    fn = "/user/testuser/testrun/afile"
    w.mkdir("/user/testuser/testrun")
    with w.open(fn, "wb") as f:
        f.write(b"hello")
    assert w.exists(fn)
    info = w.info(fn)
    assert info["size"] == 5
    assert w.isfile(fn)
    assert w.cat(fn) == b"hello"
    w.rm("/user/testuser/testrun", recursive=True)
    assert not w.exists(fn)


def test_with_gzip(hdfs_cluster):
    from gzip import GzipFile

    w = WebHDFS(
        hdfs_cluster, user="testuser", data_proxy={"worker.example.com": "localhost"}
    )
    fn = "/user/testuser/gzfile"
    with w.open(fn, "wb") as f:
        gf = GzipFile(fileobj=f, mode="w")
        gf.write(b"hello")
        gf.close()
    with w.open(fn, "rb") as f:
        gf = GzipFile(fileobj=f, mode="r")
        assert gf.read() == b"hello"


def test_workflow_transaction(hdfs_cluster):
    w = WebHDFS(
        hdfs_cluster, user="testuser", data_proxy={"worker.example.com": "localhost"}
    )
    fn = "/user/testuser/testrun/afile"
    w.mkdirs("/user/testuser/testrun")
    with w.transaction:
        with w.open(fn, "wb") as f:
            f.write(b"hello")
        assert not w.exists(fn)
    assert w.exists(fn)
    assert w.ukey(fn)
    files = w.ls("/user/testuser/testrun", True)
    summ = w.content_summary("/user/testuser/testrun")
    assert summ["length"] == files[0]["size"]
    assert summ["fileCount"] == 1

    w.rm("/user/testuser/testrun", recursive=True)
    assert not w.exists(fn)


def test_webhdfs_cp_file(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster, user="testuser", data_proxy={"worker.example.com": "localhost"}
    )

    src, dst = "/user/testuser/testrun/f1", "/user/testuser/testrun/f2"

    fs.mkdir("/user/testuser/testrun")

    with fs.open(src, "wb") as f:
        f.write(b"hello")

    fs.cp_file(src, dst)

    assert fs.exists(src)
    assert fs.exists(dst)
    assert fs.cat(src) == fs.cat(dst)


def test_path_with_equals(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster, user="testuser", data_proxy={"worker.example.com": "localhost"}
    )
    path_with_equals = "/user/testuser/some_table/datestamp=2023-11-11"

    fs.mkdir(path_with_equals)

    result = fs.ls(path_with_equals)
    assert result is not None
    assert fs.exists(path_with_equals)


def test_error_handling_with_equals_in_path(hdfs_cluster):
    fs = WebHDFS(hdfs_cluster, user="testuser")
    invalid_path_with_equals = (
        "/user/testuser/some_table/invalid_path=datestamp=2023-11-11"
    )

    with pytest.raises(FileNotFoundError):
        fs.ls(invalid_path_with_equals)


def test_create_and_touch_file_with_equals(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster,
        user="testuser",
        data_proxy={"worker.example.com": "localhost"},
    )
    base_path = "/user/testuser/some_table/datestamp=2023-11-11"
    file_path = f"{base_path}/testfile.txt"

    fs.mkdir(base_path)
    fs.touch(file_path, "wb")
    assert fs.exists(file_path)


def test_write_read_verify_file_with_equals(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster,
        user="testuser",
        data_proxy={"worker.example.com": "localhost"},
    )
    base_path = "/user/testuser/some_table/datestamp=2023-11-11"
    file_path = f"{base_path}/testfile.txt"
    content = b"This is some content!"

    fs.mkdir(base_path)
    with fs.open(file_path, "wb") as f:
        f.write(content)

    with fs.open(file_path, "rb") as f:
        assert f.read() == content

    file_info = fs.ls(base_path, detail=True)
    assert len(file_info) == 1
    assert file_info[0]["name"] == file_path
    assert file_info[0]["size"] == len(content)


def test_protocol_prefixed_path(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster, user="testuser", data_proxy={"worker.example.com": "localhost"}
    )
    protocol_prefixed_path = "webhdfs://localhost:50070/user/testuser/test_dir"

    fs.mkdir(protocol_prefixed_path)
    assert fs.exists(protocol_prefixed_path)

    file_info = fs.ls(protocol_prefixed_path, detail=True)
    assert len(file_info) == 0


def test_modified_nonexistent_path(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster,
        user="testuser",
        data_proxy={"worker.example.com": "localhost"},
    )
    nonexistent_path = "/user/testuser/nonexistent_file.txt"

    with pytest.raises(FileNotFoundError):
        fs.modified(nonexistent_path)


def test_modified_time(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster,
        user="testuser",
        data_proxy={"worker.example.com": "localhost"},
    )
    dir_path = "/user/testuser/"
    file_path = f"{dir_path}/testfile.txt"

    fs.mkdir(dir_path)

    # Check first modified time for directories
    modified_dir_date: datetime = fs.modified(dir_path)

    # I think it is the only thing we can assume, but I'm not sure if the server has a different time
    assert modified_dir_date <= datetime.now()

    # Create a file and check modified time again
    with fs.open(file_path, "wb") as f:
        f.write(b"test content")

    modified_file_date: datetime = fs.modified(file_path)
    assert modified_file_date >= modified_dir_date
    assert modified_file_date <= datetime.now()


# NOTE: These following two tests are a copy of the modified ones, as
# WebHDFS does not have a created time API, we are using modified as a proxy.


def test_created_nonexistent_path(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster,
        user="testuser",
        data_proxy={"worker.example.com": "localhost"},
    )
    nonexistent_path = "/user/testuser/nonexistent_file.txt"

    with pytest.raises(FileNotFoundError):
        fs.created(nonexistent_path)


def test_created_time(hdfs_cluster):
    fs = WebHDFS(
        hdfs_cluster,
        user="testuser",
        data_proxy={"worker.example.com": "localhost"},
    )
    dir_path = "/user/testuser/"
    file_path = f"{dir_path}/testfile.txt"

    fs.mkdir(dir_path)

    time.sleep(1)

    # Check first created time for directories
    created_dir_date: datetime = fs.created(dir_path)

    # I think it is the only thing we can assume, but I'm not sure if the server has a different time
    assert created_dir_date < datetime.now()

    # Create a file and check created time again
    with fs.open(file_path, "wb") as f:
        f.write(b"test content")

    time.sleep(1)

    created_file_date: datetime = fs.created(file_path)
    assert created_file_date > created_dir_date
    assert created_file_date < datetime.now()
