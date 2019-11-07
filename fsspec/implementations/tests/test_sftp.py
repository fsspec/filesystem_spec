import pytest
import shlex
import subprocess
import time
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
    except subprocess.CalledProcessError:
        pytest.skip("docker run not available")
        return

    # requires docker
    cmds = r"""apt-get update
apt-get install -y openssh-server
mkdir /var/run/sshd
bash -c "echo 'root:pass' | chpasswd"
sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
bash -c "echo \"export VISIBLE=now\" >> /etc/profile"
/usr/sbin/sshd
""".split(
        "\n"
    )
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


def test_simple(ssh):
    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.mkdirs("/home/someuser/deeper")
    f.touch("/home/someuser/deeper/afile")
    assert f.find("/home/someuser") == ["/home/someuser/deeper/afile"]
    assert f.ls("/home/someuser/deeper/") == ["/home/someuser/deeper/afile"]
    assert f.info("/home/someuser/deeper/afile")["type"] == "file"
    assert f.info("/home/someuser/deeper/afile")["size"] == 0
    assert f.exists("/home/someuser")
    f.rm("/home/someuser", recursive=True)
    assert not f.exists("/home/someuser")


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


def test_transaction(ssh):
    f = fsspec.get_filesystem_class("sftp")(**ssh)
    f.mkdirs("/home/someuser/deeper")
    f.start_transaction()
    f.touch("/home/someuser/deeper/afile")
    assert f.find("/home/someuser") == []
    f.end_transaction()
    f.find("/home/someuser") == ["/home/someuser/deeper/afile"]

    with f.transaction:
        assert f._intrans
        f.touch("/home/someuser/deeper/afile2")
        assert f.find("/home/someuser") == ["/home/someuser/deeper/afile"]
    assert f.find("/home/someuser") == [
        "/home/someuser/deeper/afile",
        "/home/someuser/deeper/afile2",
    ]
