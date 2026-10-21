import base64
import hashlib
import logging
import multiprocessing
import os
import pickle
import shlex
import socket
import subprocess
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from tarfile import TarFile

import pytest

import fsspec

paramiko = pytest.importorskip("paramiko")


def stop_docker(name):
    cmd = shlex.split(f'docker ps -a -q --filter "name={name}"')
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", cid])


@pytest.fixture(scope="module")
def ssh():
    try:
        pchk = ["docker", "run", "--name", "fsspec_test_sftp", "hello-world"]
        subprocess.check_call(pchk)
        stop_docker("fsspec_test_sftp")
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("docker run not available")
        return

    # requires docker
    cmds = [
        r"apt-get update",
        r"apt-get install -y openssh-server",
        r"mkdir /var/run/sshd",
        "bash -c \"echo 'root:pass' | chpasswd\"",
        (
            r"sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' "
            r"/etc/ssh/sshd_config"
        ),
        (
            r"sed 's@session\s*required\s*pam_loginuid.so@session optional "
            r"pam_loginuid.so@g' -i /etc/pam.d/sshd"
        ),
        r'bash -c "echo \"export VISIBLE=now\" >> /etc/profile"',
        r"/usr/sbin/sshd",
    ]
    name = "fsspec_sftp"
    stop_docker(name)
    cmd = f"docker run -d -p 9200:22 --name {name} ubuntu:16.04 sleep 9000"
    try:
        cid = subprocess.check_output(shlex.split(cmd)).strip().decode()
        for cmd in cmds:
            subprocess.call(["docker", "exec", cid] + shlex.split(cmd))
        time.sleep(1)
        yield {
            "host": "localhost",
            "port": 9200,
            "username": "root",
            "password": "pass",
        }
    finally:
        stop_docker(name)


@pytest.fixture(scope="module")
def root_path():
    return "/home/someuser/"


def test_simple(ssh, root_path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.mkdirs(root_path + "deeper")
    try:
        f.touch(root_path + "deeper/afile")
        assert f.find(root_path) == [root_path + "deeper/afile"]
        assert f.ls(root_path + "deeper/") == [root_path + "deeper/afile"]
        assert f.info(root_path + "deeper/afile")["type"] == "file"
        assert f.info(root_path + "deeper/afile")["size"] == 0
        assert f.exists(root_path)
    finally:
        f.rm(root_path, recursive=True)
        assert not f.exists(root_path)


@pytest.mark.parametrize("protocol", ["sftp", "ssh"])
def test_with_url(protocol, ssh):
    fo = fsspec.open(
        protocol
        + "://{username}:{password}@{host}:{port}/home/someuserout".format(**ssh),
        "wb",
    )
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open(
        protocol
        + "://{username}:{password}@{host}:{port}/home/someuserout".format(**ssh),
        "rb",
    )
    with fo as f:
        assert f.read() == b"hello"


@pytest.mark.parametrize("protocol", ["sftp", "ssh"])
def test_get_dir(protocol, ssh, root_path, tmpdir):
    path = str(tmpdir)
    f = fsspec.filesystem(protocol, **ssh)
    f.mkdirs(root_path + "deeper", exist_ok=True)
    f.touch(root_path + "deeper/afile")
    f.get(root_path, path, recursive=True)

    assert os.path.isdir(f"{path}/deeper")
    assert os.path.isfile(f"{path}/deeper/afile")

    f.get(
        protocol
        + "://{username}:{password}@{host}:{port}{root_path}".format(
            root_path=root_path, **ssh
        ),
        f"{path}/test2",
        recursive=True,
    )

    assert os.path.isdir(f"{path}/test2/deeper")
    assert os.path.isfile(f"{path}/test2/deeper/afile")


@pytest.fixture(scope="module")
def netloc(ssh):
    username = ssh.get("username")
    password = ssh.get("password")
    host = ssh.get("host")
    port = ssh.get("port")
    userpass = (
        f"{username}:{password if password is not None else ''}@"
        if username is not None
        else ""
    )
    netloc = f"{host}:{port if port is not None else ''}"
    return userpass + netloc


def test_put_file(ssh, tmp_path, root_path):
    tmp_file = tmp_path / "a.txt"
    with open(tmp_file, mode="w") as fd:
        fd.write("blabla")

    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.put_file(lpath=tmp_file, rpath=root_path + "a.txt")
    assert f.exists(root_path + "a.txt")


def test_put_many_files(ssh, tmp_path, root_path):
    tmp_file_a = tmp_path / "a.txt"
    with open(tmp_file_a, mode="w") as fd:
        fd.write("blabla")
    tmp_file_b = tmp_path / "b.txt"
    with open(tmp_file_b, mode="w") as fd:
        fd.write("blabla")

    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.put(lpath=[tmp_file_a, tmp_file_b], rpath=root_path)
    assert f.exists(root_path + "a.txt")
    assert f.exists(root_path + "b.txt")


def test_simple_with_tar(ssh, netloc, tmp_path, root_path):
    files_to_pack = ["a.txt", "b.txt"]

    tar_filename = make_tarfile(files_to_pack, tmp_path)

    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.mkdirs(f"{root_path}deeper", exist_ok=True)
    try:
        remote_tar_filename = f"{root_path}deeper/somefile.tar"
        with f.open(remote_tar_filename, mode="wb") as wfd:
            with open(tar_filename, mode="rb") as rfd:
                wfd.write(rfd.read())
        fs = fsspec.open(f"tar::ssh://{netloc}{remote_tar_filename}").fs
        files = fs.find("/")
        assert files == files_to_pack
    finally:
        f.rm(root_path, recursive=True)


def make_tarfile(files_to_pack, tmp_path):
    """Create a tarfile with some files."""
    tar_filename = tmp_path / "sometarfile.tar"
    for filename in files_to_pack:
        with open(tmp_path / filename, mode="w") as fd:
            fd.write("")
    with TarFile(tar_filename, mode="w") as tf:
        for filename in files_to_pack:
            tf.add(tmp_path / filename, arcname=filename)
    return tar_filename


def test_transaction(ssh, root_path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.mkdirs(root_path + "deeper", exist_ok=True)
    try:
        f.start_transaction()
        f.touch(root_path + "deeper/afile")
        assert f.find(root_path) == []
        f.end_transaction()
        assert f.find(root_path) == [root_path + "deeper/afile"]

        with f.transaction:
            assert f._intrans
            f.touch(root_path + "deeper/afile2")
            assert f.find(root_path) == [root_path + "deeper/afile"]
        assert f.find(root_path) == [
            root_path + "deeper/afile",
            root_path + "deeper/afile2",
        ]
    finally:
        f.rm(root_path, recursive=True)


@pytest.mark.parametrize("path", ["/a/b/c", "a/b/c"])
def test_mkdir_create_parent(ssh, path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    with pytest.raises(FileNotFoundError):
        f.mkdir(path, create_parents=False)

    f.mkdir(path)
    assert f.exists(path)

    with pytest.raises(FileExistsError, match=path):
        f.mkdir(path)

    f.rm(path, recursive=True)
    assert not f.exists(path)


@pytest.mark.parametrize("path", ["/a/b/c", "a/b/c"])
def test_makedirs_exist_ok(ssh, path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    f.makedirs(path, exist_ok=False)

    with pytest.raises(FileExistsError, match=path):
        f.makedirs(path, exist_ok=False)

    f.makedirs(path, exist_ok=True)
    f.rm(path, recursive=True)
    assert not f.exists(path)


def test_modified_nonexistent_path(ssh, root_path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    nonexistent_path = root_path + "nonexistent_file.txt"

    with pytest.raises(FileNotFoundError):
        f.modified(nonexistent_path)


def test_modified_time(ssh, root_path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)
    dir_path = root_path + "modified_dir/"
    file_path = dir_path + "testfile_modified.txt"

    f.mkdir(dir_path)

    # Check first modified time for directories
    modified_dir_date: datetime = f.modified(dir_path)

    # I think it is the only thing we can assume, but I'm not sure if the server has a different time
    assert modified_dir_date <= datetime.now(timezone.utc)

    # Create a file and check modified time again
    with f.open(file_path, "wb") as wf:
        wf.write(b"test content")

    modified_file_date: datetime = f.modified(file_path)
    assert modified_file_date >= modified_dir_date
    assert modified_file_date <= datetime.now(timezone.utc)


# NOTE: These following two tests are a copy of the modified ones, as we are using
# modified as a proxy for created. This is due to paramiko only returning st_atime
# and st_mtime.


def test_created_nonexistent_path(ssh, root_path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    nonexistent_path = root_path + "nonexistent_file.txt"

    with pytest.raises(FileNotFoundError):
        f.created(nonexistent_path)


def test_created_time(ssh, root_path):
    f = fsspec.get_filesystem_class("sftp")(**ssh)
    dir_path = root_path + "created_dir/"
    file_path = dir_path + "testfile_created.txt"

    f.mkdir(dir_path)

    # Check first created time for directories
    created_dir_date: datetime = f.created(dir_path)

    # I think it is the only thing we can assume, but I'm not sure if the server has a different time
    assert created_dir_date <= datetime.now(timezone.utc)

    # Create a file and check modified time again
    with f.open(file_path, "wb") as wf:
        wf.write(b"test content")

    created_file_date: datetime = f.created(file_path)
    assert created_file_date >= created_dir_date
    assert created_file_date <= datetime.now(timezone.utc)


# The tests below do not need a server: they check how the client is set up
# before connecting, using a stand-in for ``paramiko.SSHClient``.


class _FakeSSHClient:
    """Record what is requested from ``paramiko.SSHClient``."""

    def __init__(self):
        self.policy = None
        self.host = None
        self.connect_kwargs = None
        self.system_host_keys_file = None

    def load_system_host_keys(self, filename):
        self.system_host_keys_file = filename

    def set_missing_host_key_policy(self, policy):
        self.policy = policy

    def connect(self, host, **kwargs):
        self.host = host
        self.connect_kwargs = kwargs

    def open_sftp(self):
        return object()


@pytest.fixture
def fake_client(monkeypatch):
    monkeypatch.setattr(paramiko, "SSHClient", _FakeSSHClient)
    return _FakeSSHClient


@pytest.mark.parametrize("protocol", ["sftp", "ssh"])
def test_makedirs_with_url(fake_client, tmp_path, protocol):
    class LocalSFTP:
        def stat(self, path):
            return (tmp_path / path.lstrip("/")).stat()

        def mkdir(self, path, mode=0o777):
            (tmp_path / path.lstrip("/")).mkdir(mode=mode)

    fs = fsspec.filesystem("sftp", host="host", skip_instance_cache=True)
    fs.ftp = LocalSFTP()
    url = f"{protocol}://host/parent/child"
    fs.makedirs(url)
    assert (tmp_path / "parent/child").is_dir()
    assert {path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")} == {
        "parent",
        "parent/child",
    }
    fs.makedirs(url, exist_ok=True)
    with pytest.raises(FileExistsError):
        fs.makedirs(url)


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("path", ["", ".", "/", "parent", "parent/", "/parent/"])
def test_ls_preserves_parent_path(fake_client, tmp_path, path, detail):
    (tmp_path / "afile").write_bytes(b"content")
    (tmp_path / "subdir").mkdir()
    entries = [
        paramiko.SFTPAttributes.from_stat((tmp_path / name).stat(), name)
        for name in ["subdir", "afile"]
    ]

    class FakeSFTP:
        def listdir_iter(self, requested_path):
            assert requested_path == path
            return iter(entries)

    fs = fsspec.filesystem("sftp", host="host", skip_instance_cache=True)
    fs.ftp = FakeSFTP()
    result = fs.ls(path, detail=detail)
    prefix = path.rstrip("/") + "/" if path else ""
    if detail:
        assert [item["name"] for item in result] == [
            prefix + "subdir",
            prefix + "afile",
        ]
        assert [item["type"] for item in result] == ["directory", "file"]
        assert result[1]["size"] == len(b"content")
    else:
        assert result == [prefix + "afile", prefix + "subdir"]


def test_info_without_stat_filename(fake_client, tmp_path):
    path = tmp_path / "afile"
    path.write_bytes(b"content")
    attributes = paramiko.SFTPAttributes.from_stat(path.stat())
    assert not hasattr(attributes, "filename")

    class FakeSFTP:
        def stat(self, requested_path):
            assert requested_path == "afile"
            return attributes

    fs = fsspec.filesystem("sftp", host="host", skip_instance_cache=True)
    fs.ftp = FakeSFTP()
    result = fs.info("afile")
    assert result["name"] == "afile"
    assert result["size"] == len(b"content")
    assert result["type"] == "file"


def test_host_key_policy_default(fake_client):
    fs = fsspec.filesystem("sftp", host="host", skip_instance_cache=True)

    assert isinstance(fs.client.policy, paramiko.AutoAddPolicy)


@pytest.mark.parametrize(
    ("name", "cls"),
    [
        ("auto_add", paramiko.AutoAddPolicy),
        ("reject", paramiko.RejectPolicy),
        ("warning", paramiko.WarningPolicy),
    ],
)
def test_host_key_policy_by_name(fake_client, name, cls):
    fs = fsspec.filesystem(
        "sftp", host="host", host_key_policy=name, skip_instance_cache=True
    )

    assert isinstance(fs.client.policy, cls)


@pytest.mark.parametrize("as_instance", [False, True])
def test_host_key_policy_passed_through(fake_client, as_instance):
    policy = paramiko.RejectPolicy() if as_instance else paramiko.RejectPolicy
    fs = fsspec.filesystem(
        "sftp", host="host", host_key_policy=policy, skip_instance_cache=True
    )

    assert isinstance(fs.client.policy, paramiko.RejectPolicy)


def test_host_key_policy_not_passed_to_connect(fake_client):
    fs = fsspec.filesystem(
        "sftp",
        host="host",
        host_key_policy="reject",
        username="user",
        port=2222,
        skip_instance_cache=True,
    )

    assert fs.client.host == "host"
    assert fs.client.connect_kwargs == {"username": "user", "port": 2222}


def test_host_key_policy_unknown_name(fake_client):
    with pytest.raises(ValueError, match="Unknown host_key_policy 'letmein'"):
        fsspec.filesystem(
            "sftp", host="host", host_key_policy="letmein", skip_instance_cache=True
        )


def test_host_key_policy_wrong_type(fake_client):
    with pytest.raises(TypeError, match="host_key_policy must be one of"):
        fsspec.filesystem(
            "sftp", host="host", host_key_policy=42, skip_instance_cache=True
        )


def test_known_hosts_are_loaded(fake_client):
    fs = fsspec.filesystem("sftp", host="host", skip_instance_cache=True)

    assert fs.client.system_host_keys_file == os.path.expanduser("~/.ssh/known_hosts")


def test_known_hosts_file(fake_client):
    fs = fsspec.filesystem(
        "sftp",
        host="host",
        known_hosts="/some/known_hosts",
        host_key_policy="reject",
        skip_instance_cache=True,
    )

    assert fs.client.system_host_keys_file == "/some/known_hosts"
    assert "known_hosts" not in fs.client.connect_kwargs


# The tests below connect to an SSH server in this process, to see what
# paramiko does with the keys that are known.


class _Server(paramiko.ServerInterface):
    attempts = []  # every authentication attempt that reached the server

    def check_auth_publickey(self, username, key):
        self.attempts.append("publickey")
        return paramiko.AUTH_FAILED

    def check_auth_password(self, username, password):
        self.attempts.append("password")
        return paramiko.AUTH_SUCCESSFUL

    def get_allowed_auths(self, username):
        return "password"

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED


@pytest.fixture
def sshd():
    """Start an SSH server with SFTP; yield ``(port, host_key)``."""
    host_key = paramiko.RSAKey.generate(2048)
    _Server.attempts.clear()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    transports = []

    def serve():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            transport = paramiko.Transport(conn)
            transport.add_server_key(host_key)
            transport.set_subsystem_handler(
                "sftp", paramiko.SFTPServer, paramiko.SFTPServerInterface
            )
            transports.append(transport)
            try:
                transport.start_server(server=_Server())
            except (paramiko.SSHException, EOFError, OSError):
                pass  # the client left during the handshake

    threading.Thread(target=serve, daemon=True).start()
    yield listener.getsockname()[1], host_key
    listener.close()
    for transport in transports:
        transport.close()


def _write_known_hosts(path, port, key, host="127.0.0.1"):
    keys = paramiko.HostKeys()
    keys.add(f"[{host}]:{port}", key.get_name(), key)
    keys.save(str(path))


_CLIENT_KEY = paramiko.RSAKey.generate(2048)


def _connect(port, **kwargs):
    return fsspec.filesystem(
        "sftp",
        **{"host": "127.0.0.1", **kwargs},
        port=port,
        username="user",
        password="password",
        # offered to the server before the password, if it gets that far
        pkey=_CLIENT_KEY,
        allow_agent=False,
        look_for_keys=False,
        skip_instance_cache=True,
    )


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A home directory with an empty ``.ssh``."""
    for name in ("HOME", "USERPROFILE"):
        monkeypatch.setenv(name, str(tmp_path))
    (tmp_path / ".ssh").mkdir()
    return tmp_path


@pytest.mark.parametrize("policy", ["auto_add", "reject"])
def test_known_host_with_changed_key_is_refused(sshd, home, policy):
    port, _ = sshd
    _write_known_hosts(
        home / ".ssh" / "known_hosts", port, paramiko.RSAKey.generate(2048)
    )

    with pytest.raises(paramiko.BadHostKeyException):
        _connect(port, host_key_policy=policy)


def test_known_host_with_matching_key_is_accepted(sshd, home):
    port, host_key = sshd
    _write_known_hosts(home / ".ssh" / "known_hosts", port, host_key)

    fs = _connect(port, host_key_policy="reject")

    assert fs.ftp is not None


def test_unknown_host_is_rejected(sshd, home):
    port, _ = sshd

    with pytest.raises(paramiko.SSHException, match="not found in known_hosts"):
        _connect(port, host_key_policy="reject")


def test_known_hosts_file_with_changed_key_is_refused(sshd, home):
    port, _ = sshd
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, paramiko.RSAKey.generate(2048))

    with pytest.raises(paramiko.BadHostKeyException):
        _connect(port, known_hosts=str(known_hosts))


def test_known_host_leaves_the_file_alone(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, host_key)
    before = known_hosts.read_bytes()

    _connect(port, known_hosts=str(known_hosts))

    assert known_hosts.read_bytes() == before


def test_unknown_host_is_accepted_with_a_warning(sshd, home, caplog):
    port, host_key = sshd
    fingerprint = base64.b64encode(hashlib.sha256(host_key.asbytes()).digest())

    with caplog.at_level(logging.WARNING, logger="fsspec.sftp"):
        fs = _connect(port)

    assert fs.ftp is not None
    assert f"SHA256:{fingerprint.decode().rstrip('=')}" in caplog.text
    assert "without verification" in caplog.text


def test_known_host_is_accepted_without_a_warning(sshd, home, caplog):
    port, host_key = sshd
    _write_known_hosts(home / ".ssh" / "known_hosts", port, host_key)

    with caplog.at_level(logging.WARNING, logger="fsspec.sftp"):
        _connect(port)

    assert "without verification" not in caplog.text


def test_explicit_paramiko_auto_add_policy_stays_silent(sshd, home, caplog):
    port, _ = sshd

    with caplog.at_level(logging.WARNING, logger="fsspec.sftp"):
        _connect(port, host_key_policy=paramiko.AutoAddPolicy())

    assert "without verification" not in caplog.text


def _sees_no_authentication():
    return _Server.attempts == []


@pytest.mark.parametrize("policy", [None, "auto_add"])
def test_without_known_hosts_the_key_is_accepted_but_not_remembered(
    sshd, home, caplog, policy
):
    """The default and ``"auto_add"`` are one policy."""
    kwargs = {} if policy is None else {"host_key_policy": policy}
    with caplog.at_level(logging.WARNING, logger="fsspec.sftp"):
        fs = _connect(sshd[0], **kwargs)

    assert fs.ftp is not None
    assert "NOT remembered" in caplog.text


@pytest.mark.parametrize("policy", [None, "auto_add"])
def test_auto_add_adds_the_key_to_known_hosts(sshd, home, caplog, policy):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    known_hosts.write_text("")

    with caplog.at_level(logging.WARNING, logger="fsspec.sftp"):
        kwargs = {} if policy is None else {"host_key_policy": policy}
        _connect(port, known_hosts=str(known_hosts), **kwargs)

    assert "added to" in caplog.text
    assert "SHA256:" in caplog.text
    keys = paramiko.HostKeys(str(known_hosts))
    assert keys.lookup(f"[127.0.0.1]:{port}")[host_key.get_name()] == host_key
    # the host is known from now on
    caplog.clear()
    _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")
    assert "added to" not in caplog.text


def test_auto_add_creates_the_file(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"

    _connect(port, known_hosts=str(known_hosts), host_key_policy="auto_add")

    keys = paramiko.HostKeys(str(known_hosts))
    assert keys.lookup(f"[127.0.0.1]:{port}")[host_key.get_name()] == host_key


def test_auto_add_keeps_the_other_lines(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    # no newline at the end
    known_hosts.write_text("# kept\nother.example ssh-rsa AAAA")

    _connect(port, known_hosts=str(known_hosts), host_key_policy="auto_add")

    lines = known_hosts.read_text().splitlines()
    assert lines[:2] == ["# kept", "other.example ssh-rsa AAAA"]
    assert len(lines) == 3
    assert paramiko.HostKeys(str(known_hosts)).lookup(f"[127.0.0.1]:{port}")


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="needs a file that the user cannot write",
)
@pytest.mark.parametrize("policy", [None, "auto_add"])
def test_read_only_known_hosts_refuses_an_unknown_host(sshd, home, policy):
    known_hosts = home / "extra_known_hosts"
    known_hosts.write_text("")
    known_hosts.chmod(0o444)

    kwargs = {} if policy is None else {"host_key_policy": policy}
    with pytest.raises(paramiko.SSHException, match="cannot be added"):
        _connect(sshd[0], known_hosts=str(known_hosts), **kwargs)

    assert _sees_no_authentication()
    assert known_hosts.read_text() == ""


# The safest setup: a known_hosts file that is filled beforehand, and "reject".


def test_prefilled_known_hosts_with_reject_connects(sshd, home, caplog):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, host_key)
    before = known_hosts.read_bytes()

    with caplog.at_level(logging.WARNING, logger="fsspec.sftp"):
        fs = _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")

    assert fs.ftp is not None
    assert caplog.text == ""
    assert known_hosts.read_bytes() == before


def test_prefilled_known_hosts_with_reject_refuses_a_different_key(sshd, home):
    port, _ = sshd
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, paramiko.RSAKey.generate(2048))
    before = known_hosts.read_bytes()

    with pytest.raises(paramiko.BadHostKeyException):
        _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")

    assert _sees_no_authentication()
    assert known_hosts.read_bytes() == before


def test_prefilled_known_hosts_with_reject_refuses_an_unknown_host(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    keys = paramiko.HostKeys()
    keys.add("other.example", host_key.get_name(), host_key)
    keys.save(str(known_hosts))
    before = known_hosts.read_bytes()

    with pytest.raises(paramiko.SSHException, match="not found in known_hosts"):
        _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")

    assert _sees_no_authentication()
    assert known_hosts.read_bytes() == before


def test_prefilled_known_hosts_with_hashed_name(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    name = paramiko.HostKeys.hash_host(f"[127.0.0.1]:{port}")
    known_hosts.write_text(f"{name} {host_key.get_name()} {host_key.get_base64()}\n")

    fs = _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")

    assert fs.ftp is not None


def test_missing_known_hosts_with_reject_is_an_error(sshd, home):
    with pytest.raises(OSError, match="known_hosts"):
        _connect(sshd[0], known_hosts=str(home / "nope"), host_key_policy="reject")

    assert _sees_no_authentication()


def test_known_hosts_path_is_expanded(sshd, home):
    port, host_key = sshd
    _write_known_hosts(home / "extra_known_hosts", port, host_key)

    fs = _connect(port, known_hosts="~/extra_known_hosts", host_key_policy="reject")

    assert fs.ftp is not None
    # expanduser keeps the "/" of the argument, so on Windows the separators mix
    assert Path(fs.known_hosts) == home / "extra_known_hosts"


def _paramiko_stops_at_marker():
    """Does paramiko give up on a ``@revoked`` line, and with it on the rest?"""
    try:
        paramiko.hostkeys.HostKeyEntry.from_line("@revoked host ssh-ed25519 AAAA")
    except paramiko.hostkeys.InvalidHostKey:
        return True
    return False


#: Older paramiko (up to 3.1) skips such a line and reads on, a fixed one will too.
paramiko_stops_at_marker = pytest.mark.skipif(
    not _paramiko_stops_at_marker(),
    reason="this paramiko reads on after a line it does not understand",
)


@paramiko_stops_at_marker
@pytest.mark.parametrize(
    "marker", ["@cert-authority *.example.com", "@revoked other.example"]
)
@pytest.mark.parametrize("host_line_first", [True, False])
@pytest.mark.parametrize("policy", [None, "reject"])
def test_unsupported_line_in_known_hosts_refuses_every_connection(
    sshd, home, marker, host_line_first, policy
):
    """paramiko stops reading at such a line: refuse, wherever the host is."""
    port, host_key = sshd
    line = f"{host_key.get_name()} {host_key.get_base64()}"
    lines = [f"[127.0.0.1]:{port} {line}\n", f"{marker} {line}\n"]
    if not host_line_first:
        lines.reverse()
    (home / ".ssh" / "known_hosts").write_text("".join(lines))

    kwargs = {} if policy is None else {"host_key_policy": policy}
    with pytest.raises(paramiko.hostkeys.InvalidHostKey):
        _connect(port, **kwargs)

    assert _sees_no_authentication()


@paramiko_stops_at_marker
def test_unsupported_line_in_known_hosts_file_refuses_the_connection(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    known_hosts.write_text(f"@revoked x.example {host_key.get_name()} AAAA\n")

    with pytest.raises(paramiko.hostkeys.InvalidHostKey):
        _connect(port, known_hosts=str(known_hosts))

    assert _sees_no_authentication()


def test_changed_host_key_is_logged_as_an_error(sshd, home, caplog):
    port, _ = sshd
    _write_known_hosts(
        home / ".ssh" / "known_hosts", port, paramiko.RSAKey.generate(2048)
    )

    with caplog.at_level(logging.ERROR, logger="fsspec.sftp"):
        with pytest.raises(paramiko.BadHostKeyException):
            _connect(port)

    assert "CHANGED" in caplog.text
    assert "man-in-the-middle" in caplog.text


def test_known_hosts_that_cannot_be_read_refuse_the_connection(sshd, home):
    # a directory cannot be read as a file, on every system
    (home / ".ssh" / "known_hosts").mkdir()

    with pytest.raises(OSError):
        _connect(sshd[0])

    assert _sees_no_authentication()


@pytest.mark.parametrize("target", ["home", "file"])
def test_non_text_known_hosts_refuse_the_connection(sshd, home, target):
    port, _ = sshd
    known_hosts = home / (".ssh/known_hosts" if target == "home" else "extra")
    known_hosts.write_bytes(b"host \xff\xfe ssh-ed25519 AAAA\n")
    kwargs = {} if target == "home" else {"known_hosts": str(known_hosts)}

    # Windows reads with its code page, which decodes any byte: a bad line instead
    with pytest.raises((UnicodeDecodeError, paramiko.hostkeys.InvalidHostKey)):
        _connect(port, **kwargs)

    assert _sees_no_authentication()


def test_home_that_is_not_a_directory_has_no_known_hosts(sshd, tmp_path, monkeypatch):
    """HOME=/dev/null is common in services: there is no file to read."""
    not_a_directory = tmp_path / "file"
    not_a_directory.write_text("")
    for name in ("HOME", "USERPROFILE"):
        monkeypatch.setenv(name, str(not_a_directory))

    fs = _connect(sshd[0])

    assert fs.ftp is not None


def test_known_hosts_file_replaces_the_default(sshd, home):
    """Like UserKnownHostsFile: ~/.ssh/known_hosts is not read at all."""
    port, host_key = sshd
    _write_known_hosts(
        home / ".ssh" / "known_hosts", port, paramiko.RSAKey.generate(2048)
    )
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, host_key)

    fs = _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")

    assert fs.ftp is not None


def test_known_hosts_file_does_not_add_the_default(sshd, home):
    port, host_key = sshd
    _write_known_hosts(home / ".ssh" / "known_hosts", port, host_key)
    known_hosts = home / "extra_known_hosts"
    known_hosts.write_text("")

    with pytest.raises(paramiko.SSHException, match="not found in known_hosts"):
        _connect(port, known_hosts=str(known_hosts), host_key_policy="reject")

    assert _sees_no_authentication()


def _append_many(path, count, key_bytes):
    from fsspec.implementations.sftp import _append_known_host

    key = paramiko.RSAKey(data=key_bytes)
    for i in range(count):
        _append_known_host(path, f"host-{os.getpid()}-{i}", key)


def test_processes_adding_keys_at_the_same_time_lose_none(tmp_path):
    known_hosts = tmp_path / "known_hosts"
    key = paramiko.RSAKey.generate(2048)

    # spawn: forking a process that runs threads is not safe
    spawn = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(4, mp_context=spawn) as pool:
        futures = [
            pool.submit(_append_many, str(known_hosts), 25, key.asbytes())
            for _ in range(4)
        ]
        for future in futures:
            future.result()

    assert len(paramiko.HostKeys(str(known_hosts))) == 100


def test_host_name_with_non_ascii_characters_is_found_again(tmp_path):
    """paramiko reads the file with the default encoding: so must we write it."""
    known_hosts = tmp_path / "known_hosts"
    key = paramiko.RSAKey.generate(2048)

    fsspec.implementations.sftp._append_known_host(
        str(known_hosts), "b\u00fccher.example", key
    )

    keys = paramiko.HostKeys(str(known_hosts))
    assert keys.lookup("b\u00fccher.example")[key.get_name()] == key


def test_known_hosts_and_policy_survive_pickling(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, host_key)
    # not _connect(): its private key object cannot be pickled
    fs = fsspec.filesystem(
        "sftp",
        host="127.0.0.1",
        port=port,
        username="user",
        password="password",
        allow_agent=False,
        look_for_keys=False,
        known_hosts=str(known_hosts),
        host_key_policy="reject",
        skip_instance_cache=True,
    )

    restored = pickle.loads(pickle.dumps(fs))

    assert restored.known_hosts == str(known_hosts)
    assert isinstance(restored.host_key_policy, paramiko.RejectPolicy)
    assert restored.ftp is not None


def test_known_hosts_and_policy_are_part_of_the_instance_cache(sshd, home):
    port, host_key = sshd
    known_hosts = home / "extra_known_hosts"
    _write_known_hosts(known_hosts, port, host_key)
    args = {"host": "127.0.0.1", "port": port, "username": "user", "password": "x"}

    first = fsspec.filesystem("sftp", known_hosts=str(known_hosts), **args)
    same = fsspec.filesystem("sftp", known_hosts=str(known_hosts), **args)
    other = fsspec.filesystem(
        "sftp", known_hosts=str(known_hosts), host_key_policy="reject", **args
    )

    assert first is same
    assert first is not other
