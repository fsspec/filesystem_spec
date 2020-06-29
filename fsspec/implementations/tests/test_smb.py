import pytest
import shlex
import subprocess
import time
import fsspec

pytest.importorskip("smbprotocol")


def stop_docker(name):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % name)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="module")
def smb():
    try:
        pchk = ["docker", "run", "--name", "fsspec_test_smb", "hello-world"]
        subprocess.check_call(pchk)
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("docker run not available")
        return
    stop_docker("fsspec_test_smb")

    # requires docker
    name = "fsspec_smb"
    stop_docker(name)
    img = "docker run --name {} --detach -p 139:139 -p 445:445 dperson/samba"
    cfg = " -p -u 'testuser;testpass' -s 'home;/share;no;no;no;testuser'"
    cmd = img.format(name) + cfg
    cid = subprocess.check_output(shlex.split(cmd)).strip().decode()
    print(cid)
    try:
        time.sleep(1)
        yield dict(host="localhost", port=445, username="testuser", password="testpass")
    finally:
        import smbclient
        smbclient.reset_connection_cache()
        stop_docker(name)


def test_simple(smb):
    f = fsspec.get_filesystem_class("smb")(**smb)
    f.mkdirs("/home/someuser/deeper")
    f.touch("/home/someuser/deeper/afile")
    assert f.find("/home/someuser") == ["/home/someuser/deeper/afile"]
    assert f.ls("/home/someuser/deeper/") == ["/home/someuser/deeper/afile"]
    assert f.info("/home/someuser/deeper/afile")["type"] == "file"
    assert f.info("/home/someuser/deeper/afile")["size"] == 0
    assert f.exists("/home/someuser")
    f.rm("/home/someuser", recursive=True)
    assert not f.exists("/home/someuser")


def test_with_url(smb):
    fo = fsspec.open(
        "smb://{username}:{password}@{host}:{port}"
        "/home/someuser.txt".format(**smb),
        "wb",
    )
    with fo as f:
        f.write(b"hello")
    fo = fsspec.open(
        "smb://{username}:{password}@{host}:{port}"
        "/home/someuser.txt".format(**smb),
        "rb",
    )
    with fo as f:
        read_result = f.read()
        assert read_result == b"hello"


def test_transaction(smb):
    f = fsspec.get_filesystem_class("smb")(**smb)
    f.mkdirs("/home/sometran/deeper")
    f.start_transaction()
    f.touch("/home/sometran/deeper/afile")
    assert f.find("/home/sometran") == []
    f.end_transaction()
    f.find("/home/sometran") == ["/home/sometran/deeper/afile"]

    with f.transaction:
        assert f._intrans
        f.touch("/home/sometran/deeper/afile2")
        assert f.find("/home/sometran") == ["/home/sometran/deeper/afile"]
    assert f.find("/home/sometran") == [
        "/home/sometran/deeper/afile",
        "/home/sometran/deeper/afile2",
    ]


def test_makedirs_exist_ok(smb):
    f = fsspec.get_filesystem_class("smb")(**smb)

    f.makedirs("/home/a/b/c")
    f.makedirs("/home/a/b/c", exist_ok=True)
