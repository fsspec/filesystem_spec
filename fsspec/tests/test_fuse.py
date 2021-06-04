import os
import signal
import time
from multiprocessing import Process

import pytest

try:
    pytest.importorskip("fuse")  # noqa: E402
except OSError:
    # can succeed in importing fuse, but fail to load so
    pytest.importorskip("nonexistent")  # noqa: E402

from fsspec.fuse import run
from fsspec.implementations.memory import MemoryFileSystem


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
        os.kill(fuse_process.pid, signal.SIGTERM)
        fuse_process.join()
