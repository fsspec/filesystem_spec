import shlex
import subprocess
import time
from tarfile import TarFile

import pytest

import fsspec

pytest.importorskip("paramiko")


def stop_docker(name):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % name)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", cid])


@pytest.fixture(scope="module")
def ssh():
    try:
        subprocess.check_call(["docker", "run", "hello-world"])
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
    cmd = "docker run -d -p 9200:22 --name {} ubuntu:16.04 sleep 9000".format(name)
    cid = subprocess.check_output(shlex.split(cmd)).strip().decode()
    for cmd in cmds:
        subprocess.call(["docker", "exec", cid] + shlex.split(cmd))
    try:
        time.sleep(1)
        yield dict(host="localhost", port=9200, username="root", password="pass")
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
        protocol + "://{username}:{password}@{host}:{port}"
        "/home/someuserout".format(**ssh),
        "wb",
    )
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open(
        protocol + "://{username}:{password}@{host}:{port}"
        "/home/someuserout".format(**ssh),
        "rb",
    )
    with fo as f:
        assert f.read() == b"hello"


@pytest.fixture(scope="module")
def netloc(ssh):
    username = ssh.get("username")
    password = ssh.get("password")
    host = ssh.get("host")
    port = ssh.get("port")
    userpass = (
        username + ((":" + password) if password is not None else "") + "@"
        if username is not None
        else ""
    )
    netloc = host + ((":" + str(port)) if port is not None else "")
    return userpass + netloc


def test_simple_with_tar(ssh, netloc, tmp_path, root_path):

    files_to_pack = ["a.txt", "b.txt"]

    tar_filename = make_tarfile(files_to_pack, tmp_path)

    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.mkdirs(root_path + "deeper", exist_ok=True)
    try:
        remote_tar_filename = root_path + "deeper/somefile.tar"
        with f.open(remote_tar_filename, mode="wb") as wfd:
            with open(tar_filename, mode="rb") as rfd:
                wfd.write(rfd.read())
        fs = fsspec.open("tar::ssh://" + netloc + remote_tar_filename).fs
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


def test_mkdir_create_parent(ssh):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    with pytest.raises(FileNotFoundError):
        f.mkdir("/a/b/c")

    f.mkdir("/a/b/c", create_parents=True)
    assert f.exists("/a/b/c")

    with pytest.raises(FileExistsError, match="/a/b/c"):
        f.mkdir("/a/b/c")

    f.rm("/a/b/c", recursive=True)


def test_makedirs_exist_ok(ssh):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    f.makedirs("/a/b/c")

    with pytest.raises(FileExistsError, match="/a/b/c"):
        f.makedirs("/a/b/c", exist_ok=False)

    f.makedirs("/a/b/c", exist_ok=True)
