import glob
import os
import pytest
import shlex
import subprocess
import time
import fsspec

requests = pytest.importorskip('requests')
fn = "test_http.py"
d = os.path.dirname(__file__)
data = open(__file__, 'rb').read()


@pytest.fixture(scope='module')
def server():
    cmd = shlex.split("python -m http.server 8000 --directory %s" % d)
    try:
        P = subprocess.Popen(cmd)
        retries = 10
        url = 'http://localhost:8000/'
        while True:
            try:
                requests.get(url)
                break
            except:
                retries -= 1
                assert retries > 0, "Ran out of retries waiting for HTTP server"
                time.sleep(0.1)
        yield url
    finally:
        P.terminate()
        P.wait()


def test_list(server):
    h = fsspec.filesystem('http')
    out = h.glob(server + '/*.py')
    expected = [os.path.basename(f) for f in glob.glob('%s/*.py' % d)]
    for myfile in expected:
        assert any(myfile in os.path.basename(f) for f in out)


def test_read(server):
    h = fsspec.filesystem('http')
    out = server + '/' + fn
    with h.open(out, 'rb') as f:
        assert f.read() == data
    with h.open(out, 'rb', block_size=0) as f:
        assert f.read() == data
    with h.open(out, 'rb', size_policy='head') as f:
        assert f.size == len(data)


def test_methods(server):
    h = fsspec.filesystem('http')
    url = server + fn
    assert h.exists(url)
    assert h.cat(url) == data


def test_random_access(server):
    h = fsspec.filesystem('http')
    url = server + '/' + fn
    with h.open(url, 'rb') as f:
        assert f.read(5) == data[:5]
        # python server does not respect bytes range request
        # we actually get all the data
        f.seek(5, 1)
        assert f.read(5) == data[10:15]

