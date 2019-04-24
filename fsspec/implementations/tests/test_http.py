import glob
import pytest
import requests
import subprocess
import time
import fsspec

pytest.importorskip('requests')


@pytest.fixture(scope='module')
def server():
    cmd = "python -m http.server 8000".split()
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
    expected = glob.glob('*.py')
    for fn in expected:
        assert any(fn in f for f in out)


def test_read(server):
    h = fsspec.filesystem('http')
    out = h.glob(server + '/*.py')[0]
    expected = glob.glob('*.py')[0]
    with h.open(out, 'rb') as f:
        assert f.read() == open(expected, 'rb').read()
