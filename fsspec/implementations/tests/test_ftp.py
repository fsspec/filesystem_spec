import os
import pytest
import shutil
import subprocess
import sys
import time

from fsspec.implementations.cached import CachingFileSystem
from fsspec.implementations.ftp import FTPFileSystem
from fsspec import open_files
import fsspec

pytest.importorskip("pyftpdlib")
here = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture()
def ftp():
    P = subprocess.Popen(
        [sys.executable, "-m", "pyftpdlib", "-d", here],
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )
    try:
        time.sleep(1)
        yield "localhost", 2121
    finally:
        P.terminate()
        P.wait()


@pytest.fixture()
def ftp_writable(tmpdir):
    FTPFileSystem.clear_instance_cache()  # remove lingering connections
    CachingFileSystem.clear_instance_cache()
    d = str(tmpdir)
    with open(os.path.join(d, "out"), "wb") as f:
        f.write(b"hello" * 10000)
    P = subprocess.Popen(
        [sys.executable, "-m", "pyftpdlib", "-d", d, "-u", "user", "-P", "pass", "-w"]
    )
    try:
        time.sleep(1)
        yield "localhost", 2121, "user", "pass"
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
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))
    out = fs.cat("/" + os.path.basename(__file__))
    assert out == open(__file__, "rb").read()


def test_not_cached(ftp):
    host, port = ftp
    fs = FTPFileSystem(host, port)
    fs2 = FTPFileSystem(host, port)
    assert fs is not fs2


@pytest.mark.parametrize("cache_type", ["bytes", "mmap"])
def test_complex(ftp_writable, cache_type):
    from fsspec.core import BytesCache

    host, port, user, pw = ftp_writable
    files = open_files(
        "ftp:///ou*",
        host=host,
        port=port,
        username=user,
        password=pw,
        block_size=10000,
        cache_type=cache_type,
    )
    assert len(files) == 1
    with files[0] as fo:
        assert fo.read(10) == b"hellohello"
        if isinstance(fo.cache, BytesCache):
            assert len(fo.cache.cache) == 10010
        assert fo.read(2) == b"he"
        assert fo.tell() == 12


def test_write_small(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out2", "wb") as f:
        f.write(b"oi")
    assert fs.cat("/out2") == b"oi"


def test_with_url(ftp_writable):
    host, port, user, pw = ftp_writable
    fo = fsspec.open("ftp://{}:{}@{}:{}/out".format(user, pw, host, port), "wb")
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open("ftp://{}:{}@{}:{}/out".format(user, pw, host, port), "rb")
    with fo as f:
        assert f.read() == b"hello"


@pytest.mark.parametrize("cache_type", ["bytes", "mmap"])
def test_write_big(ftp_writable, cache_type):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw, block_size=1000, cache_type=cache_type)
    fn = "/bigger"
    with fs.open(fn, "wb") as f:
        f.write(b"o" * 500)
        assert not fs.exists(fn)
        f.write(b"o" * 1000)
        fs.invalidate_cache()
        assert fs.exists(fn)
        f.write(b"o" * 200)
        f.flush()

    assert fs.info(fn)["size"] == 1700
    assert fs.cat(fn) == b"o" * 1700


def test_transaction(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    fs.mkdir("/tmp")
    fn = "/tr"
    with fs.transaction:
        with fs.open(fn, "wb") as f:
            f.write(b"not")
        assert not fs.exists(fn)
    assert fs.exists(fn)
    assert fs.cat(fn) == b"not"

    fs.rm(fn)
    assert not fs.exists(fn)
