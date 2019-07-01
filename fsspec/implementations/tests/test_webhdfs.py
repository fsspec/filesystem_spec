import pytest
import subprocess
import time

requests = pytest.importorskip("requests")
from fsspec.implementations.webhdfs import WebHDFS


@pytest.fixture(scope='module')
def hdfs_cluster():
    cmd0 = "htcluster shutdown".split()
    subprocess.check_output(cmd0)
    cmd1 = "htcluster startup --image base".split()
    subprocess.check_output(cmd1)
    try:
        while True:
            t = 90
            try:
                requests.get('http://localhost:50070/webhdfs/v1/?op=LISTSTATUS')
            except:
                t -= 1
                assert t > 0, "Timeout waiting for HDFS"
                time.sleep(1)
                continue
            break
        time.sleep(7)
        yield "localhost"
    finally:
        subprocess.check_output(cmd0)


def test_simple(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user='testuser')
    home = w.home_directory()
    assert home == '/user/testuser'


def test_workflow(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user='testuser',
                data_proxy={'worker.example.com': 'localhost'})
    fn = '/user/testuser/testrun/afile'
    w.mkdir('/user/testuser/testrun')
    with w.open(fn, 'wb') as f:
        f.write(b'hello')
    assert w.exists(fn)
    info = w.info(fn)
    assert info['size'] == 5
    assert w.isfile(fn)
    assert w.cat(fn) == b'hello'
    w.rm('/user/testuser/testrun', recursive=True)
    assert not w.exists(fn)


def test_workflow_transaction(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user='testuser',
                data_proxy={'worker.example.com': 'localhost'})
    fn = '/user/testuser/testrun/afile'
    w.mkdir('/user/testuser/testrun')
    with w.transaction:
        with w.open(fn, 'wb') as f:
            f.write(b'hello')
        assert not w.exists(fn)
    assert w.exists(fn)
    w.rm('/user/testuser/testrun', recursive=True)
    assert not w.exists(fn)
