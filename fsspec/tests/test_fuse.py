import os
import subprocess
import time
from multiprocessing import Process

import pytest

try:
    pytest.importorskip("fuse")
except OSError:
    # can succeed in importing fuse, but fail to load so
    pytest.importorskip("nonexistent")

from fsspec.fuse import main, run
from fsspec.implementations.memory import MemoryFileSystem


@pytest.mark.parametrize(
    "flags, foreground", [([], False), (["-f"], True), (["--foreground"], True)]
)
def test_cli_foreground(flags, foreground, tmp_path, mocker):
    mount = mocker.patch("fsspec.fuse.run")

    main(["memory://", "/source", str(tmp_path), *flags])

    mount.assert_called_once()
    assert isinstance(mount.call_args.args[0], MemoryFileSystem)
    assert mount.call_args.args[1:] == ("/source", str(tmp_path))
    assert mount.call_args.kwargs["foreground"] is foreground


def test_run_defaults_to_foreground(tmp_path, mocker):
    mount = mocker.patch("fsspec.fuse.FUSE")

    run(MemoryFileSystem(), "/source", str(tmp_path))

    mount.assert_called_once()
    assert mount.call_args.kwargs["foreground"] is True


def host_fuse(mountdir):
    fs = MemoryFileSystem()
    fs.touch("/mounted/testfile")
    run(fs, "/mounted/", mountdir)


def test_basic(tmpdir, capfd):
    mountdir = str(tmpdir.mkdir("mount"))

    fuse_process = Process(target=host_fuse, args=(str(mountdir),))
    fuse_process.start()

    try:
        timeout = 10
        while True:
            try:
                # can fail with device not ready while waiting for fuse
                if "testfile" in os.listdir(mountdir):
                    break
            except Exception:
                pass
            timeout -= 1
            time.sleep(1)
            if not timeout > 0:
                import pdb

                pdb.set_trace()
                pytest.skip(msg="fuse didn't come live")

        fn = os.path.join(mountdir, "test")
        with open(fn, "wb") as f:
            f.write(b"data")

        with open(fn) as f:
            assert f.read() == "data"

        os.remove(fn)

        os.mkdir(fn)
        assert os.listdir(fn) == []

        os.mkdir(fn + "/inner")

        with pytest.raises(OSError):
            os.rmdir(fn)

        captured = capfd.readouterr()
        assert "Traceback" not in captured.out
        assert "Traceback" not in captured.err

        os.rmdir(fn + "/inner")
        os.rmdir(fn)
    finally:
        fuse_process.terminate()
        fuse_process.join(timeout=10)
        if fuse_process.is_alive():
            fuse_process.kill()
            fuse_process.join()


def host_mount_local(source_dir, mount_dir, debug_log):
    main(["local", source_dir, mount_dir, "-f", "-l", debug_log, "--ready-file"])


@pytest.fixture()
def mount_local(tmpdir):
    source_dir = tmpdir.mkdir("source")
    mount_dir = tmpdir.mkdir("local")
    debug_log = tmpdir / "debug.log"
    fuse_process = Process(
        target=host_mount_local, args=(str(source_dir), str(mount_dir), str(debug_log))
    )
    fuse_process.start()
    ready_file = mount_dir / ".fuse_ready"
    for _ in range(20):
        if ready_file.exists() and open(ready_file).read() == b"ready":
            break
        time.sleep(0.1)
    try:
        yield (source_dir, mount_dir)
    finally:
        fuse_process.terminate()
        fuse_process.join(timeout=10)
        if fuse_process.is_alive():
            fuse_process.kill()
            fuse_process.join()


def test_mount(mount_local):
    source_dir, mount_dir = mount_local
    assert os.listdir(mount_dir) == []
    assert os.listdir(source_dir) == []

    mount_dir.mkdir("a")

    assert os.listdir(mount_dir) == ["a"]
    assert os.listdir(source_dir) == ["a"]


def test_chmod(mount_local):
    source_dir, mount_dir = mount_local
    open(mount_dir / "text", "w").write("test")
    assert os.listdir(source_dir) == ["text"]

    cp = subprocess.run(
        ["cp", str(mount_dir / "text"), str(mount_dir / "new")],
        capture_output=True,
        check=False,
    )

    assert cp.stderr == b""
    assert cp.stdout == b""
    assert set(os.listdir(source_dir)) == {"text", "new"}
    assert open(mount_dir / "new").read() == "test"


def test_seek_rw(mount_local):
    source_dir, mount_dir = mount_local
    fh = open(mount_dir / "text", "w")
    fh.write("teST")
    fh.seek(2)
    fh.write("st")
    fh.close()

    fh = open(mount_dir / "text", "r")
    assert fh.read() == "test"
    fh.seek(2)
    assert fh.read() == "st"
    fh.close()
