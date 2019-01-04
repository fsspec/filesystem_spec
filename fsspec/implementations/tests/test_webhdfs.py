import pytest
import requests
import subprocess
import time

from fsspec.implementations.webhdfs import WebHDFS


@pytest.fixture()
def hdfs_cluster():
    cmd0 = "htcluster shutdown".split()
    subprocess.check_output(cmd0)
    cmd1 = "htcluster startup --image base".split()
    subprocess.check_output(cmd1)
    try:
        while True:
            t = 10
            try:
                requests.get('http://localhost:50070/webhdfs/v1/?op=LISTSTATUS')
            except:
                t -= 1
                assert t > 0, "Timeout waiting for HDFS"
                time.sleep(1)
                continue
            break
        yield "localhost"
    finally:
        subprocess.check_output(cmd0)


def test_simple(hdfs_cluster):
    w = WebHDFS(hdfs_cluster, user='testuser')
    home = w.home_directory()
    assert home == '/user/testuser'
