"""
Test SMBFileSystem class using a docker container
"""

import logging
import os
import shlex
import subprocess
import time

import pytest

import fsspec

pytest.importorskip("smbprotocol")

# ruff: noqa: F821

if os.environ.get("WSL_INTEROP"):
    # Running on WSL (Windows)
    port_test = [9999]

else:
    # ! pylint: disable=redefined-outer-name,missing-function-docstring

    # Test standard and non-standard ports
    default_port = 445
    port_test = [None, default_port, 9999]


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="module", params=port_test)
def smb_params(request):
    try:
        pchk = ["docker", "run", "--name", "fsspec_test_smb", "hello-world"]
        subprocess.check_call(pchk)
        stop_docker("fsspec_test_smb")
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("docker run not available")

    # requires docker
    container = "fsspec_smb"
    stop_docker(container)
    cfg = "-p -u 'testuser;testpass' -s 'home;/share;no;no;no;testuser'"
    port = request.param if request.param is not None else default_port
    img = (
        f"docker run --name {container} --detach -p 139:139 -p {port}:445 dperson/samba"  # noqa: E231 E501
    )
    cmd = f"{img} {cfg}"
    try:
        cid = subprocess.check_output(shlex.split(cmd)).strip().decode()
        logger = logging.getLogger("fsspec")
        logger.debug("Container: %s", cid)
        time.sleep(1)
        yield {
            "host": "localhost",
            "port": request.param,
            "username": "testuser",
            "password": "testpass",
            "register_session_retries": 100,  # max ~= 10 seconds
        }
    finally:
        import smbclient  # pylint: disable=import-outside-toplevel

        smbclient.reset_connection_cache()
        stop_docker(container)


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_simple(smb_params):
    adir = "/home/adir"
    adir2 = "/home/adir/otherdir/"
    afile = "/home/adir/otherdir/afile"
    fsmb = fsspec.get_filesystem_class("smb")(**smb_params)
    fsmb.mkdirs(adir2)
    fsmb.touch(afile)
    assert fsmb.find(adir) == [afile]
    assert fsmb.ls(adir2, detail=False) == [afile]
    assert fsmb.info(afile)["type"] == "file"
    assert fsmb.info(afile)["size"] == 0
    assert fsmb.exists(adir)
    fsmb.rm(adir, recursive=True)
    assert not fsmb.exists(adir)


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_auto_mkdir(smb_params):
    adir = "/home/adir"
    adir2 = "/home/adir/otherdir/"
    afile = "/home/adir/otherdir/afile"
    fsmb = fsspec.get_filesystem_class("smb")(**smb_params, auto_mkdir=True)
    fsmb.touch(afile)
    assert fsmb.exists(adir)
    assert fsmb.exists(adir2)
    assert fsmb.exists(afile)
    assert fsmb.info(afile)["type"] == "file"

    another_dir = "/home/another_dir"
    another_dir2 = "/home/another_dir/another_nested_dir/"
    another_file = "/home/another_dir/another_nested_dir/another_file"
    fsmb.copy(afile, another_file)
    assert fsmb.exists(another_dir)
    assert fsmb.exists(another_dir2)
    assert fsmb.exists(another_file)
    assert fsmb.info(another_file)["type"] == "file"

    fsmb.rm(adir, recursive=True)
    fsmb.rm(another_dir, recursive=True)
    assert not fsmb.exists(adir)
    assert not fsmb.exists(another_dir)


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_with_url(smb_params):
    if smb_params["port"] is None:
        smb_url = "smb://{username}:{password}@{host}/home/someuser.txt"
    else:
        smb_url = "smb://{username}:{password}@{host}:{port}/home/someuser.txt"
    fwo = fsspec.open(smb_url.format(**smb_params), "wb")
    with fwo as fwr:
        fwr.write(b"hello")
    fro = fsspec.open(smb_url.format(**smb_params), "rb")
    with fro as frd:
        read_result = frd.read()
        assert read_result == b"hello"


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_transaction(smb_params):
    afile = "/home/afolder/otherdir/afile"
    afile2 = "/home/afolder/otherdir/afile2"
    adir = "/home/afolder"
    adir2 = "/home/afolder/otherdir"
    fsmb = fsspec.get_filesystem_class("smb")(**smb_params)
    fsmb.mkdirs(adir2)
    fsmb.start_transaction()
    fsmb.touch(afile)
    assert fsmb.find(adir) == []
    fsmb.end_transaction()
    assert fsmb.find(adir) == [afile]

    with fsmb.transaction:
        assert fsmb._intrans
        fsmb.touch(afile2)
        assert fsmb.find(adir) == [afile]
    assert fsmb.find(adir) == [afile, afile2]


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_makedirs_exist_ok(smb_params):
    fsmb = fsspec.get_filesystem_class("smb")(**smb_params)
    fsmb.makedirs("/home/a/b/c")
    fsmb.makedirs("/home/a/b/c", exist_ok=True)


@pytest.mark.flaky(reruns=2, reruns_delay=2)
def test_rename_from_upath(smb_params):
    fsmb = fsspec.get_filesystem_class("smb")(**smb_params)
    fsmb.makedirs("/home/a/b/c", exist_ok=True)
    fsmb.mv("/home/a/b/c", "/home/a/b/d", recursive=False, maxdepth=None)
