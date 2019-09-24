import os
import signal
import time
from multiprocessing import Process

import pytest

from fsspec.fuse import run
from fsspec.implementations.memory import MemoryFileSystem

pytest.importorskip("fuse")


def host_fuse(mountdir):
    fs = MemoryFileSystem()
    fs.touch("/mounted/testfile")
    run(fs, "/mounted/", mountdir)


def test_basic(tmpdir):
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
            assert timeout > 0, "Timeout"

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

        os.rmdir(fn + "/inner")
        os.rmdir(fn)
    finally:
        os.kill(fuse_process.pid, signal.SIGTERM)
        fuse_process.join()
