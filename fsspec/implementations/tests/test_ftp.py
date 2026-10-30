import io
import os
import ssl
import sys
from ftplib import FTP, FTP_TLS

import pytest

import fsspec
from fsspec import open_files
from fsspec.conftest import _ftp_server
from fsspec.implementations.ftp import FTPFileSystem, ImplicitFTPTLS, _mlsd2

ftplib = pytest.importorskip("ftplib")
here = os.path.dirname(os.path.abspath(__file__))


def trust_test_cert():
    """A context that accepts the self-signed certificate of the test servers"""
    ctx = ssl.create_default_context(cafile=os.path.join(here, "keycert.pem"))
    ctx.check_hostname = False  # the test certificate names no host
    return ctx


@pytest.fixture()
def ftp():
    pytest.importorskip("pyftpdlib")
    pytest.importorskip("OpenSSL")
    with _ftp_server([sys.executable, os.path.join(here, "ftp_tls.py")]):
        yield "localhost", 2121


@pytest.fixture()
def ftp_tlsv12():
    pytest.importorskip("pyftpdlib")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with _ftp_server(
        [sys.executable, os.path.join(here, "ftp_tlsv12.py")],
        port=2122,
        ssl_context=context,
    ):
        yield "localhost", 2122


@pytest.mark.parametrize(
    "tls,exp_cls",
    (
        (False, FTP),
        (True, FTP_TLS),
    ),
)
def test_tls(ftp, tls, exp_cls):
    host, port = ftp
    fs = FTPFileSystem(host, port, tls=tls, ssl_context=trust_test_cert())
    assert isinstance(fs.ftp, exp_cls)


@pytest.mark.parametrize(
    "tls,exp_cls",
    (("tlsv1_2", ImplicitFTPTLS),),
)
def test_tls_v12(ftp_tlsv12, tls, exp_cls):
    host, port = ftp_tlsv12
    fs = FTPFileSystem(host, port, tls=tls, ssl_context=trust_test_cert())
    assert isinstance(fs.ftp, exp_cls)
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))
    out = fs.cat(f"/{os.path.basename(__file__)}")
    assert out == open(__file__, "rb").read()


def test_tls_reconnect_keeps_data_channel_protected(ftp, monkeypatch):
    host, port = ftp
    fs = FTPFileSystem(host, port, tls=True, ssl_context=trust_test_cert())
    first = fs.ftp

    def abort():
        raise ftplib.error_proto("500 Unknown command.")

    # a server that rejects ABOR makes a partial read reconnect
    monkeypatch.setattr(first, "abort", abort)
    with fs.open(f"/{os.path.basename(__file__)}", block_size=100) as f:
        f.read(100)
    assert fs.ftp is not first

    tls_data = []
    ntransfercmd = fs.ftp.ntransfercmd

    def spy(cmd, rest=None):
        conn, size = ntransfercmd(cmd, rest)
        tls_data.append(isinstance(conn, ssl.SSLSocket))
        return conn, size

    monkeypatch.setattr(fs.ftp, "ntransfercmd", spy)
    out = fs.cat(f"/{os.path.basename(__file__)}")
    assert out == open(__file__, "rb").read()
    assert tls_data == [True]


def test_tls_warns_on_untrusted_certificate(ftp):
    host, port = ftp
    # the test server's certificate is not in the default trust store; for one
    # release this still connects, but warns that it will be refused
    with pytest.warns(FutureWarning, match="could not be verified"):
        fs = FTPFileSystem(host, port, "user", "pass", tls=True)
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))


def test_tls_trusted_certificate_does_not_warn(ftp, recwarn):
    host, port = ftp
    FTPFileSystem(host, port, "user", "pass", tls=True, ssl_context=trust_test_cert())
    assert not [w for w in recwarn if issubclass(w.category, FutureWarning)]


@pytest.mark.parametrize(
    "tls,username,password",
    (
        (False, "", ""),
        (True, "", ""),
        (False, "user", "pass"),
        (True, "user", "pass"),
    ),
)
def test_basic(ftp, tls, username, password):
    host, port = ftp
    fs = FTPFileSystem(
        host, port, username, password, tls=tls, ssl_context=trust_test_cert()
    )
    assert fs.ls("/", detail=False) == sorted(os.listdir(here))
    out = fs.cat(f"/{os.path.basename(__file__)}")
    assert out == open(__file__, "rb").read()


def test_not_cached(ftp):
    host, port = ftp
    fs = FTPFileSystem(host, port)
    fs2 = FTPFileSystem(host, port)
    assert fs is not fs2


def test_ls_root_dircache(ftp):
    host, port = ftp
    fs = FTPFileSystem(host, port)

    files = fs.ls("/", detail=False)

    assert "/" in fs.dircache

    ftp_tmp = fs.ftp
    fs.ftp = None  # Ensure no ftp action will be done after

    files2 = fs.ls("/", detail=False)

    assert files == files2

    fs.ftp = ftp_tmp


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


@pytest.mark.parametrize("start,end", [(None, None), (3, None), (-3, None), (2, 8)])
def test_cat_file_with_protocol(ftp_writable, start, end):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    data = b"0123456789"
    fs.pipe("/url-file", data)
    url = f"ftp://{host}:{port}/url-file"
    assert fs.cat_file(url, start=start, end=end) == data[start:end]


@pytest.mark.parametrize("file_like", [False, True])
def test_get_file_with_protocol(ftp_writable, tmp_path, file_like):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    data = b"download through an FTP URL"
    fs.pipe("/url-file", data)
    target = io.BytesIO() if file_like else tmp_path / "download"
    fs.get_file(f"ftp://{host}:{port}/url-file", target)
    assert (target.getvalue() if file_like else target.read_bytes()) == data


@pytest.mark.parametrize("start", [-3, -10, -100, 0, 3, None])
def test_cat_file_start(ftp_writable, start):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    data = b"0123456789"
    fs.pipe("/slice", data)
    assert fs.cat_file("/slice", start=start) == data[start:]
    assert fs.cat_file("/slice", start=start, end=8) == data[start:8]


def test_cat_file_negative_start_empty_or_missing(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    fs.pipe("/empty", b"")
    assert fs.cat_file("/empty", start=-3) == b""
    with pytest.raises(FileNotFoundError):
        fs.cat_file("/missing", start=-3)


@pytest.mark.parametrize("start", [None, 3])
def test_cat_file_reads_past_cached_size(ftp_writable, start):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    fs.pipe("/growing", b"short")
    assert fs.info("/growing")["size"] == 5
    data = b"short and longer"
    # An external write does not invalidate this filesystem's directory cache.
    with FTP() as writer:
        writer.connect(host, port)
        writer.login(user, pw)
        writer.storbinary("STOR /growing", io.BytesIO(data))
    assert fs.cat_file("/growing", start=start) == data[start:]


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


class _FakeDirFTP:
    """Minimal stand-in for ftplib.FTP whose .dir(path, callback) replays
    a canned `ls -l` style listing — enough to exercise the _mlsd2 parser
    without needing an FTP server that disables MLSD."""

    def __init__(self, lines):
        self._lines = lines

    def dir(self, path, callback):
        for line in self._lines:
            callback(line)


def test_mlsd2_parses_filename_without_spaces():
    ftp = _FakeDirFTP(["-rw-r--r-- 1 owner group 38283 Jan 12 07:08 plain.pdf"])
    out = _mlsd2(ftp, "/data")
    assert out == [
        (
            "plain.pdf",
            {
                "modify": "Jan 12 07:08",
                "unix.owner": "owner",
                "unix.group": "group",
                "unix.mode": "-rw-r--r--",
                "size": "38283",
                "type": "file",
            },
        )
    ]


def test_mlsd2_parses_filename_with_spaces():
    # Regression test: when the FTP server doesn't support MLSD, _mlsd2 falls
    # back to parsing `dir` output. Previously it took split_line[-1] as the
    # filename, which truncated names containing whitespace.
    ftp = _FakeDirFTP(
        [
            "-rw-r--r-- 1 owner group 38283 Jan 12 07:08 my file.pdf",
            "drwxr-xr-x 2 owner group  4096 Mar 15 10:30 a dir with spaces",
            "-rw-r--r-- 1 owner group   100 Feb  3 09:15 trailing   spaces.txt",
        ]
    )
    out = _mlsd2(ftp, "/data")
    names = [name for name, _ in out]
    assert names == [
        "my file.pdf",
        "a dir with spaces",
        "trailing   spaces.txt",
    ]
    assert out[0][1]["type"] == "file"
    # _mlsd2 itself reports "dir"; ls() promotes it to "directory" later.
    assert out[1][1]["type"] == "dir"
    assert out[2][1]["size"] == "100"


def test_mlsd2_parses_symlink_name_without_target():
    # `ls -l` formats symlinks as "<name> -> <target>". Only the link's own
    # name should be returned; the previous parser took split_line[-1] and so
    # returned the target instead of the name.
    ftp = _FakeDirFTP(
        ["lrwxrwxrwx 1 owner group 10 Jan 12 07:08 link name -> some/target"]
    )
    out = _mlsd2(ftp, "/data")
    assert len(out) == 1
    name, info = out[0]
    assert name == "link name"
    assert info["unix.mode"].startswith("l")


def test_mlsd2_skips_short_lines():
    # The `total N` summary line emitted by some servers has fewer than 9
    # fields and should be ignored.
    ftp = _FakeDirFTP(
        [
            "total 4",
            "-rw-r--r-- 1 owner group 1 Jan 12 07:08 keep.txt",
        ]
    )
    out = _mlsd2(ftp, "/data")
    assert [name for name, _ in out] == ["keep.txt"]


@pytest.mark.parametrize("initial", [None, b"", b"original-content"])
@pytest.mark.parametrize("chunks", [[], [b"ADD"], [b"12345", b"67890", b"Z"]])
def test_append_preserves_existing_data(ftp_writable, initial, chunks):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw, skip_instance_cache=True)
    path = "/append-target"
    if initial is not None:
        fs.pipe_file(path, initial)
    with fs.open(path, "ab", block_size=4) as f:
        assert f.tell() == len(initial or b"")
        for chunk in chunks:
            f.write(chunk)
        assert f.tell() == len(initial or b"") + sum(map(len, chunks))
    assert fs.cat_file(path) == (initial or b"") + b"".join(chunks)


@pytest.mark.parametrize("initial", [None, b"", b"original-content"])
@pytest.mark.parametrize("transaction", [False, True])
def test_transactional_append_is_rejected(ftp_writable, initial, transaction):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw, skip_instance_cache=True)
    path = "/append-target"
    if initial is not None:
        fs.pipe_file(path, initial)
    if transaction:
        with fs.transaction:
            with pytest.raises(NotImplementedError, match="append.*transactions"):
                fs.open(path, "ab")
    else:
        with pytest.raises(NotImplementedError, match="append.*transactions"):
            fs.open(path, "ab", autocommit=False)
    if initial is None:
        assert not fs.exists(path)
    else:
        assert fs.cat_file(path) == initial


def test_append_multiple_flushes_and_sessions(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw, skip_instance_cache=True)
    path = "/append-target"
    fs.pipe_file(path, b"original")
    with fs.open(path, "ab", block_size=3) as f:
        f.write(b"123")
        f.flush()
        f.flush()
        f.write(b"45")
        f.flush()
    with fs.open(path, "ab", block_size=3) as f:
        assert f.tell() == len(b"original12345")
        f.write(b"678")
    assert fs.cat_file(path) == b"original12345678"
    with fs.open(path, "wb") as f:
        f.write(b"replacement")
    assert fs.cat_file(path) == b"replacement"
