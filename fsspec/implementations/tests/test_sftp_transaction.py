import contextlib
import os
import socket
import threading

import pytest

from fsspec.implementations.tests.test_sftp import _Server, _write_known_hosts

paramiko = pytest.importorskip("paramiko")


class _PasswordOnly(paramiko.auth_strategy.AuthStrategy):
    def get_sources(self):
        yield paramiko.auth_strategy.Password("user", lambda: "password")


class _FileServer(paramiko.SFTPServerInterface):
    """Serve ordinary files under the test directory over real SFTP."""

    def __init__(self, server, *, root, events):
        super().__init__(server)
        self.root = root
        self.events = events

    def local(self, path):
        target = (self.root / path.lstrip("/")).resolve()
        target.relative_to(self.root)
        return target

    def open(self, path, flags, attr):
        self.events.append(("open", path, flags))
        try:
            fd = os.open(self.local(path), flags, 0o600)
            mode = "rb" if not flags & (os.O_WRONLY | os.O_RDWR) else "r+b"
            file = os.fdopen(fd, mode)
            handle = paramiko.SFTPHandle(flags)
            handle.readfile = handle.writefile = file
            return handle
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def stat(self, path):
        try:
            return paramiko.SFTPAttributes.from_stat(self.local(path).stat())
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def posix_rename(self, oldpath, newpath):
        self.events.append(("posix_rename", oldpath, newpath))
        os.replace(self.local(oldpath), self.local(newpath))
        return paramiko.SFTP_OK

    def remove(self, path):
        self.events.append(("remove", path))
        self.local(path).unlink()
        return paramiko.SFTP_OK


@pytest.fixture(scope="module")
def sftp_files(tmp_path_factory):
    from fsspec.implementations.sftp import SFTPFileSystem

    root = tmp_path_factory.mktemp("sftp-transaction")
    (root / "staging").mkdir()
    key = paramiko.RSAKey.generate(2048)
    events = []
    transports = []
    errors = []
    stopped = threading.Event()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(0.2)
    port = listener.getsockname()[1]

    def serve():
        while not stopped.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            transport = paramiko.Transport(conn)
            transports.append(transport)
            transport.add_server_key(key)
            transport.set_subsystem_handler(
                "sftp", paramiko.SFTPServer, _FileServer, root=root, events=events
            )
            try:
                transport.start_server(server=_Server())
            except Exception as exc:
                errors.append(exc)

    thread = threading.Thread(target=serve)
    thread.start()
    fs = None
    try:
        known_hosts = root / "known_hosts"
        known_hosts.touch(mode=0o600, exist_ok=False)
        _write_known_hosts(known_hosts, port, key)
        fs = SFTPFileSystem(
            "127.0.0.1",
            port=port,
            known_hosts=str(known_hosts),
            host_key_policy="reject",
            auth_strategy=_PasswordOnly(ssh_config=None),
            allow_agent=False,
            look_for_keys=False,
            temppath="/staging",
            skip_instance_cache=True,
            timeout=5,
            banner_timeout=5,
            auth_timeout=5,
            channel_timeout=5,
        )
        fs.ftp.get_channel().settimeout(5)
        yield fs, root, events
    finally:
        if fs is not None:
            fs.ftp.close()
            fs.client.close()
        stopped.set()
        listener.close()
        thread.join(timeout=5)
        for transport in transports:
            transport.close()
            transport.join(timeout=5)
        assert not thread.is_alive()
        assert all(not transport.is_alive() for transport in transports)
        assert not errors


@pytest.mark.parametrize("mode", ["r", "rb", "rt", "br"])
@pytest.mark.parametrize(
    "buffering", [{}, {"block_size": None}, {"block_size": 0}, {"block_size": 4096}]
)
@pytest.mark.parametrize("in_transaction", [False, True])
@pytest.mark.parametrize("options", [{}, {"autocommit": False}, {"autocommit": True}])
def test_read_committed_file(sftp_files, mode, buffering, in_transaction, options):
    fs, root, events = sftp_files
    content = "transaction input \N{SNOWMAN}\n"
    (root / "input").write_text(content, encoding="utf-8")
    before = fs.info("/input")
    events.clear()
    reader = None
    expected = content.encode() if "b" in mode else content
    try:
        context = fs.transaction if in_transaction else contextlib.nullcontext()
        with context:
            reader = fs.open("/input", mode, encoding="utf-8", **buffering, **options)
            assert reader.read() == expected
            assert not fs.transaction.files
            assert list((root / "staging").iterdir()) == []
        # Finishing a transaction leaves the caller's reader open.
        assert not reader.closed
        reader.seek(0)
        assert reader.read() == expected
    finally:
        if reader is not None:
            reader.close()
    assert reader.closed
    assert events == [("open", "/input", os.O_RDONLY)]
    after = fs.info("/input")
    for field in ("name", "size", "type", "uid", "gid", "mtime"):
        assert after[field] == before[field]
    assert (root / "input").read_text(encoding="utf-8") == content


@pytest.mark.parametrize("commit", [False, True])
def test_transaction_read_then_write(sftp_files, commit):
    fs, root, events = sftp_files
    (root / "input").write_bytes(b"input")
    (root / "output").write_bytes(b"previous")
    events.clear()
    outcome = contextlib.nullcontext() if commit else pytest.raises(ValueError)
    with outcome, fs.transaction:
        with fs.open("/input", "rb") as reader:
            content = reader.read()
        with fs.open("/output", "wb") as writer:
            writer.write(content + b" processed")
        assert list(fs.transaction.files) == [writer]
        assert (root / "output").read_bytes() == b"previous"
        assert [p.name for p in (root / "staging").iterdir()] == [
            writer.temppath.rsplit("/", 1)[1]
        ]
        if not commit:
            raise ValueError("rollback")
    assert (root / "input").read_bytes() == b"input"
    assert (root / "output").read_bytes() == (
        b"input processed" if commit else b"previous"
    )
    assert not fs._intrans
    assert fs._transaction is None
    assert list((root / "staging").iterdir()) == []
    assert events[0] == ("open", "/input", os.O_RDONLY)
    assert events[1][1] == writer.temppath
    assert events[-1] == (
        ("posix_rename", writer.temppath, "/output")
        if commit
        else ("remove", writer.temppath)
    )


@pytest.mark.parametrize("mode", ["wb", "w+b", "wb+", "ab", "a+b", "ab+", "xb", "x+b"])
@pytest.mark.parametrize("autocommit", [False, True])
def test_write_modes_keep_staging(sftp_files, mode, autocommit):
    fs, root, events = sftp_files
    target = root / "write-mode"
    target.unlink(missing_ok=True)
    events.clear()
    with fs.open("/write-mode", mode, autocommit=autocommit) as writer:
        requested = "/write-mode" if autocommit else writer.temppath
        assert events[0][1] == requested
        if not autocommit:
            assert writer.targetpath == "/write-mode"
            assert list(fs.transaction.files) == [writer]
            assert not target.exists()
    if not autocommit:
        fs.end_transaction()
        assert events[-1] == ("posix_rename", requested, "/write-mode")
    assert target.exists()
    assert list((root / "staging").iterdir()) == []


@pytest.mark.parametrize("mode", ["r+", "rb+", "r+b"])
@pytest.mark.parametrize("autocommit", [False, True])
def test_read_update_modes_keep_paths(sftp_files, mode, autocommit):
    fs, root, events = sftp_files
    (root / "update").write_bytes(b"original")
    events.clear()
    if autocommit:
        with fs.open("/update", mode, autocommit=True) as writer:
            writer.write(b"new" if "b" in mode else "new")
        assert events[0][1] == "/update"
        assert (root / "update").read_bytes() == b"newginal"
    else:
        # Update mode requires an existing file, including at a staged path.
        with pytest.raises(FileNotFoundError):
            fs.open("/update", mode, autocommit=False)
        assert events[0][1].startswith("/staging/")
        assert (root / "update").read_bytes() == b"original"
    assert not fs.transaction.files
    assert list((root / "staging").iterdir()) == []
