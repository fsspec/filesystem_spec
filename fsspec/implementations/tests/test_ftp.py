import os
import subprocess
import sys
import time
from ftplib import FTP, FTP_TLS

import pytest

import fsspec
from fsspec import open_files
from fsspec.implementations.ftp import FTPFileSystem

ftplib = pytest.importorskip("ftplib")
here = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture()
def ftp():
    pytest.importorskip("pyftpdlib")
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


@pytest.mark.parametrize(
    "tls,exp_cls",
    (
        (False, FTP),
        (True, FTP_TLS),
    ),
)
def test_tls(ftp, tls, exp_cls):
    host, port = ftp
    fs = FTPFileSystem(host, port, tls=tls)
    assert isinstance(fs.ftp, exp_cls)


def test_basic(ftp):
    host, port = ftp
    fs = FTPFileSystem(host, port)
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))
    out = fs.cat(f"/{os.path.basename(__file__)}")
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
    fo = fsspec.open(f"ftp://{user}:{pw}@{host}:{port}/out", "wb")
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open(f"ftp://{user}:{pw}@{host}:{port}/out", "rb")
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


def test_transaction_with_cache(ftp_writable, tmpdir):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    fs.mkdir("/tmp")
    fs.mkdir("/tmp/dir")
    assert "dir" in fs.ls("/tmp", detail=False)

    with fs.transaction:
        fs.rmdir("/tmp/dir")

    assert "dir" not in fs.ls("/tmp", detail=False)
    assert not fs.exists("/tmp/dir")


def test_cat_get(ftp_writable, tmpdir):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw, block_size=500)
    fs.mkdir("/tmp")
    data = b"hello" * 500
    fs.pipe("/tmp/myfile", data)
    assert fs.cat_file("/tmp/myfile") == data

    fn = os.path.join(tmpdir, "lfile")
    fs.get_file("/tmp/myfile", fn)
    assert open(fn, "rb").read() == data


def test_mkdir(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with pytest.raises(ftplib.error_perm):
        fs.mkdir("/tmp/not/exist", create_parents=False)
    fs.mkdir("/tmp/not/exist")
    assert fs.exists("/tmp/not/exist")
    fs.makedirs("/tmp/not/exist", exist_ok=True)
    with pytest.raises(FileExistsError):
        fs.makedirs("/tmp/not/exist", exist_ok=False)
    fs.makedirs("/tmp/not/exist/inner/inner")
    assert fs.isdir("/tmp/not/exist/inner/inner")


def test_rm_get_recursive(ftp_writable, tmpdir):
    tmpdir = str(tmpdir)
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    fs.mkdir("/tmp/topdir")
    fs.mkdir("/tmp/topdir/underdir")
    fs.touch("/tmp/topdir/afile")
    fs.touch("/tmp/topdir/underdir/afile")

    fs.get("/tmp/topdir", tmpdir, recursive=True)

    with pytest.raises(ftplib.error_perm):
        fs.rmdir("/tmp/topdir")

    fs.rm("/tmp/topdir", recursive=True)
    assert not fs.exists("/tmp/topdir")
