import os
import subprocess
import sys
import time

import pytest

import fsspec
from fsspec import open_files
from fsspec.implementations.ftp import FTPFileSystem

ftplib = pytest.importorskip("ftplib")
here = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture()
def ftp():
    P = subprocess.Popen(
        [sys.executable, os.path.join(os.path.dirname(__file__), "ftp_tls.py")],
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )
    try:
        time.sleep(1)
        yield "localhost", 2121, "user", "pass"
    finally:
        P.terminate()
        P.wait()


def test_basic(ftp):
    host, port, _, _ = ftp
    fs = FTPFileSystem(host, port, timeout=1, ssl=True)
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))
    out = fs.cat(f"/{os.path.basename(__file__)}")
    assert out == open(__file__, "rb").read()


def test_basic_prot_p(ftp):
    host, port, _, _ = ftp
    fs = FTPFileSystem(host, port, ssl=True, prot_p=True)
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))
    out = fs.cat(f"/{os.path.basename(__file__)}")
    assert out == open(__file__, "rb").read()


def test_not_cached(ftp):
    host, port, _, _ = ftp
    fs = FTPFileSystem(host, port, ssl=True)
    fs2 = FTPFileSystem(host, port, ssl=True)
    assert fs is not fs2


@pytest.mark.parametrize("cache_type", ["bytes", "mmap"])
def test_complex(ftp, cache_type):
    from fsspec.core import BytesCache

    host, port, user, pw = ftp
    files = open_files(
        "ftp:///ou*",
        host=host,
        port=port,
        username=user,
        password=pw,
        block_size=10000,
        cache_type=cache_type,
        ssl=True,
    )
    assert len(files) == 1
    with files[0] as fo:
        assert fo.read(10) == b"hellohello"
        if isinstance(fo.cache, BytesCache):
            assert len(fo.cache.cache) == 10010
        assert fo.read(2) == b"he"
        assert fo.tell() == 12


def test_write_small(ftp):
    host, port, user, pw = ftp
    fs = FTPFileSystem(host, port, user, pw, ssl=True)
    with fs.open("/out_tls2", "wb") as f:
        f.write(b"oi")
    assert fs.cat("/out_tls2") == b"oi"


def test_with_url(ftp):
    host, port, user, pw = ftp
    fo = fsspec.open(f"ftp://{user}:{pw}@{host}:{port}/out_tls", "wb")
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open(f"ftp://{user}:{pw}@{host}:{port}/out_tls", "rb")
    with fo as f:
        assert f.read() == b"hello"


@pytest.mark.parametrize("cache_type", ["bytes", "mmap"])
def test_write_big(ftp, cache_type):
    host, port, user, pw = ftp
    fs = FTPFileSystem(
        host, port, user, pw, block_size=1000, cache_type=cache_type, ssl=True
    )
    fn = f"/bigger_tls_{cache_type}"
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
    fs.rm(fn)


def test_transaction(ftp):
    host, port, user, pw = ftp
    fs = FTPFileSystem(host, port, user, pw, ssl=True)
    fs.mkdir("tmp_tls")
    fn = "tr"
    with fs.transaction:
        with fs.open(fn, "wb") as f:
            f.write(b"not")
        assert not fs.exists(fn)
    assert fs.exists(fn)
    assert fs.cat(fn) == b"not"

    fs.rm(fn)
    assert not fs.exists(fn)


def test_transaction_with_cache(ftp, tmpdir):
    host, port, user, pw = ftp
    fs = FTPFileSystem(host, port, user, pw, ssl=True)
    fs.mkdirs("tmp_tls", exist_ok=True)
    fs.mkdir("tmp_tls/dir")
    assert "dir" in fs.ls("tmp_tls", detail=False)

    with fs.transaction:
        fs.rmdir("tmp_tls/dir")

    assert "dir" not in fs.ls("tmp_tls", detail=False)
    assert not fs.exists("tmp_tls/dir")


def test_cat_get(ftp, tmpdir):
    host, port, user, pw = ftp
    fs = FTPFileSystem(host, port, user, pw, block_size=500, ssl=True)
    fs.mkdirs("tmp_tls", exist_ok=True)
    data = b"hello" * 500
    fs.pipe("tmp_tls/myfile_tls", data)
    assert fs.cat_file("tmp_tls/myfile_tls") == data

    fn = os.path.join(tmpdir, "lfile")
    fs.get_file("tmp_tls/myfile_tls", fn)
    assert open(fn, "rb").read() == data


def test_mkdir(ftp):
    host, port, user, pw = ftp
    fs = FTPFileSystem(host, port, user, pw, ssl=True)
    with pytest.raises(ftplib.error_perm):
        fs.mkdir("tmp_tls/not/exist_tls", create_parents=False)
    fs.mkdir("tmp_tls/not/exist")
    assert fs.exists("tmp_tls/not/exist")
    fs.makedirs("tmp_tls/not/exist", exist_ok=True)
    with pytest.raises(FileExistsError):
        fs.makedirs("tmp_tls/not/exist", exist_ok=False)
    fs.makedirs("tmp_tls/not/exist/inner/inner")
    assert fs.isdir("tmp_tls/not/exist/inner/inner")


def test_rm_get_recursive(ftp, tmpdir):
    tmpdir = str(tmpdir)
    host, port, user, pw = ftp
    fs = FTPFileSystem(host, port, user, pw, ssl=True)
    fs.mkdir("tmp_tls/topdir")
    fs.mkdir("tmp_tls/topdir/underdir")
    fs.touch("tmp_tls/topdir/afile")
    fs.touch("tmp_tls/topdir/underdir/afile")

    fs.get("tmp_tls/topdir", tmpdir, recursive=True)

    with pytest.raises(ftplib.error_perm):
        fs.rmdir("tmp_tls/topdir")

    fs.rm("tmp_tls/topdir", recursive=True)
    assert not fs.exists("tmp_tls/topdir")
