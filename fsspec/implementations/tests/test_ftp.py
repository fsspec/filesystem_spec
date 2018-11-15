import os
import pytest
import subprocess
import time
from fsspec.implementations.ftp import FTPFileSystem

pytest.importorskip('pyftpdlib')
here = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture()
def ftp(tmpdir):
    P = subprocess.Popen(['python', '-m', 'pyftpdlib', '-d', here])
    try:
        time.sleep(1)
        yield 'localhost', 2121
    finally:
        P.terminate()
        P.wait()


def test_basic(ftp):
    host, port = ftp
    fs = FTPFileSystem(host, port)
    assert fs.ls('/', detail=False) == sorted(
        ['/' + f for f in os.listdir(here)])
    out = fs.cat('/' + os.path.basename(__file__))
    assert out == open(__file__, 'rb').read()
