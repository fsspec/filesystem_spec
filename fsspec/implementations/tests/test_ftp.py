import os
import pytest
import shutil
import subprocess
import time
from fsspec.implementations.ftp import FTPFileSystem
from fsspec import open_files

pytest.importorskip('pyftpdlib')
here = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture()
def ftp():
    P = subprocess.Popen(['python', '-m', 'pyftpdlib', '-d', here])
    try:
        time.sleep(1)
        yield 'localhost', 2121
    finally:
        P.terminate()
        P.wait()

@pytest.fixture()
def ftp_writable(tmpdir):
    d = str(tmpdir)
    with open(os.path.join(d, 'out'), 'wb') as f:
        f.write(b'hello' * 10000)
    P = subprocess.Popen(['python', '-m', 'pyftpdlib', '-d', d,
                          '-u', 'user', '-P', 'pass'])
    try:
        time.sleep(1)
        yield 'localhost', 2121, 'user', 'pass'
    finally:
        P.terminate()
        P.wait()
        try:
            shutil.rmtree(tmpdir)
        except:
            pass


def test_basic(ftp):
    host, port = ftp
    fs = FTPFileSystem(host, port)
    assert fs.ls('/', detail=False) == sorted(
        ['/' + f for f in os.listdir(here)])
    out = fs.cat('/' + os.path.basename(__file__))
    assert out == open(__file__, 'rb').read()


def test_complex(ftp_writable):
    host, port, user, pw = ftp_writable
    files = open_files('ftp:///ou*', host=host, port=port,
                       username=user, password=pw,
                       block_size=10000)
    assert len(files) == 1
    with files[0] as fo:
        assert fo.read(10) == b'hellohello'
        assert len(fo.cache) == 10010
        assert fo.read(2) == b'he'
        assert fo.tell() == 12
