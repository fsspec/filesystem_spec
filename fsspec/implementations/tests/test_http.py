import glob
import pytest
import subprocess
import fsspec


@pytest.fixture(scope='module')
def server():
    cmd = "python -m http.server 8000".split()
    try:
        P = subprocess.Popen(cmd)
        yield "http://localhost:8000/"
    finally:
        P.terminate()
        P.wait()


def test_list(server):
    h = fsspec.get_filesystem_class('http')()
    out = h.glob(server + '/*.py')
    expected = glob.glob('*.py')
    for fn in expected:
        assert any(fn in f for f in out)
