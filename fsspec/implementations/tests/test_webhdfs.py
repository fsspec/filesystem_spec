import pickle
import pytest
import subprocess
import time

requests = pytest.importorskip("requests")
from fsspec.implementations.webhdfs import WebHDFS
import fsspec


@pytest.fixture(scope='module')
def hdfs_cluster():
    cmd0 = "htcluster shutdown".split()
    try:
        subprocess.check_output(cmd0)
    except FileNotFoundError:
        pytest.skip("htcluster not found")
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


def test_pickle(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user='testuser')
    w2 = pickle.loads(pickle.dumps(w))
    assert w == w2


def test_simple(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user='testuser')
    home = w.home_directory()
    assert home == '/user/testuser'
    with pytest.raises(PermissionError):
        w.mkdir('/root')


def test_url(hdfs_cluster):
    url = 'webhdfs://testuser@localhost:50070/user/testuser/myfile'
    fo = fsspec.open(url, 'wb', data_proxy={'worker.example.com': 'localhost'})
    with fo as f:
        f.write(b'hello')
    fo = fsspec.open(url, 'rb', data_proxy={'worker.example.com': 'localhost'})
    with fo as f:
        assert f.read() == b'hello'


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
    w.mkdirs('/user/testuser/testrun')
    with w.transaction:
        with w.open(fn, 'wb') as f:
            f.write(b'hello')
        assert not w.exists(fn)
    assert w.exists(fn)
    assert w.ukey(fn)
    files = w.ls('/user/testuser/testrun', True)
    summ = w.content_summary('/user/testuser/testrun')
    assert summ['length'] == files[0]['size']
    assert summ['fileCount'] == 1

    w.rm('/user/testuser/testrun', recursive=True)
    assert not w.exists(fn)

