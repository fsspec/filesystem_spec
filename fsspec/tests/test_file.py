"""Tests abstract buffered file API, using FTP implementation"""
import pickle
import sys
import pytest
from fsspec.implementations.tests.test_ftp import FTPFileSystem

data = b"hello" * 10000


@pytest.mark.xfail(
    sys.version_info < (3, 6),
    reason="py35 error, see https://github.com/intake/filesystem_spec/issues/147",
)
def test_pickle(ftp_writable):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    f = ftp.open("/out", "rb")

    f2 = pickle.loads(pickle.dumps(f))
    assert f == f2


def test_file_read_attributes(ftp_writable):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    f = ftp.open("/out", "rb")
    assert f.info()["size"] == len(data)
    assert f.tell() == 0
    assert f.seekable()
    assert f.readable()
    assert not f.writable()
    out = bytearray(len(data))

    assert f.read() == data
    assert f.read() == b""
    f.seek(0)
    assert f.readuntil(b"l") == b"hel"
    assert f.tell() == 3

    f.readinto1(out)
    assert out[:-3] == data[3:]
    with pytest.raises(ValueError):
        f.write(b"")
    f.close()
    with pytest.raises(ValueError):
        f.read()(b"")


def test_seek(ftp_writable):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    f = ftp.open("/out", "rb")

    assert f.seek(-10, 2) == len(data) - 10
    assert f.tell() == len(data) - 10
    assert f.seek(-1, 1) == len(data) - 11
    with pytest.raises(ValueError):
        f.seek(-1)
    with pytest.raises(ValueError):
        f.seek(0, 7)


def test_file_idempotent(ftp_writable):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    f = ftp.open("/out", "rb")
    f2 = ftp.open("/out", "rb")
    assert hash(f) == hash(f2)
    assert f == f2
    ftp.touch("/out2")
    f2 = ftp.open("/out2", "rb")
    assert hash(f2) != hash(f)
    assert f != f2
    f2 = ftp.open("/out", "wb")
    assert hash(f2) != hash(f)


def test_file_text_attributes(ftp_writable):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    data = b"hello\n" * 1000
    with ftp.open("/out2", "wb") as f:
        f.write(data)

    f = ftp.open("/out2", "rb")
    assert f.readline() == b"hello\n"
    f.seek(0)
    assert list(f) == [d + b"\n" for d in data.split()]
    f.seek(0)
    assert f.readlines() == [d + b"\n" for d in data.split()]

    f = ftp.open("/out2", "rt")
    assert f.readline() == "hello\n"
    assert f.encoding


def test_file_write_attributes(ftp_writable):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)
    f = ftp.open("/out2", "wb")
    with pytest.raises(ValueError):
        f.info()
    with pytest.raises(OSError):
        f.seek(0)
    with pytest.raises(ValueError):
        f.read(0)
    assert not f.readable()
    assert f.writable()

    f.flush()  # no-op

    assert f.write(b"hello") == 5
    assert f.write(b"hello") == 5
    assert not f.closed
    f.close()
    assert f.closed
    with pytest.raises(ValueError):
        f.write(b"")
    with pytest.raises(ValueError):
        f.flush()


def test_midread_cache(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host=host, port=port, username=user, password=pw)
    fn = "/myfile"
    with fs.open(fn, "wb") as f:
        f.write(b"a" * 175627146)
    with fs.open(fn, "rb") as f:
        f.seek(175561610)
        d1 = f.read(65536)
        assert len(d1) == 65536

        f.seek(4)
        size = 17562198
        d2 = f.read(size)
        assert len(d2) == size

        f.seek(17562288)
        size = 17562187
        d3 = f.read(size)
        assert len(d3) == size


def test_read_block(ftp_writable):
    # not the same as test_read_block in test_utils, this depends on the
    # behaviour of the bytest caching
    from fsspec.utils import read_block

    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host=host, port=port, username=user, password=pw)
    fn = "/myfile"
    with fs.open(fn, "wb") as f:
        f.write(b"a,b\n1,2")
    f = fs.open(fn, "rb", cache_type="bytes")
    assert read_block(f, 0, 6400, b"\n") == b"a,b\n1,2"


def test_with_gzip(ftp_writable):
    import gzip

    data = b"some compressable stuff"
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host=host, port=port, username=user, password=pw)
    fn = "/myfile"
    with fs.open(fn, "wb") as f:
        gf = gzip.GzipFile(fileobj=f, mode="w")
        gf.write(data)
        gf.close()
    with fs.open(fn, "rb") as f:
        gf = gzip.GzipFile(fileobj=f, mode="r")
        assert gf.read() == data


def test_with_zip(ftp_writable):
    import zipfile

    data = b"hello zip"
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host=host, port=port, username=user, password=pw)
    fn = "/myfile"
    with fs.open(fn, "wb") as f:
        zf = zipfile.ZipFile(fileobj=f, mode="w")
        zf.write(data)
        zf.close()
    with fs.open(fn, "rb") as f:
        zf = zipfile.ZipFile(fileobj=f, mode="r")
        assert zf.read() == data
