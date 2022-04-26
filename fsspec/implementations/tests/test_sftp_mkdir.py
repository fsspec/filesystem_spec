import shlex
import subprocess
import time

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


def test_mkdir_create_parent(ssh):
    f = fsspec.get_filesystem_class("sftp")(**ssh)

    with pytest.raises(FileNotFoundError):
        f.mkdir("/a/b/c")

    f.mkdir("/a/b/c", create_parents=True)
    assert f.exists("/a/b/c")

    with pytest.raises(FileExistsError, match="/a/b/c"):
        f.mkdir("/a/b/c")
