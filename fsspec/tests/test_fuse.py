import logging
import os
import signal
import time
from multiprocessing import Process

import pytest

from fsspec.fuse import run
from fsspec.implementations.memory import MemoryFileSystem

pytest.importorskip("fuse")


logger = logging.getLogger(__name__)


def host_fuse(mountdir):
    fs = MemoryFileSystem()
    fs.touch("/mounted/testfile")
    run(fs, "/mounted/", mountdir)


def test_basic(tmpdir):
    mountdir = str(tmpdir.mkdir("mount"))

    fuse_process = Process(target=host_fuse, args=(str(mountdir),))
    fuse_process.start()
    try:

        logger.debug("run %s", mountdir)

        timeout = 10
        while True:
            try:
                logger.debug("listdir")
                # can fail with device not ready while waiting for fuse
                if "testfile" in os.listdir(mountdir):
                    logger.debug("break")
                    break
            except:
                pass
            timeout -= 1
            time.sleep(1)
            assert timeout > 0, "Timeout"

        logger.debug("write")
        fn = os.path.join(mountdir, "test")
        with open(fn, "wb") as f:
            f.write(b"data")
        logger.debug("info")
        # assert fs.info("/mounted/test")['size'] == 4

        logger.debug("open")
        f = open(fn)

        logger.debug("read")
        assert f.read() == "data"

        logger.debug("remove")
        os.remove(fn)

        logger.debug("mkdir")
        os.mkdir(fn)

        logger.debug("listdir")
        assert os.listdir(fn) == []

        logger.debug("mkdir")
        os.mkdir(fn + "/inner")

        logger.debug("rmdir")
        with pytest.raises(OSError):
            os.rmdir(fn)

        os.rmdir(fn + "/inner")
        os.rmdir(fn)
        # assert not fs.pseudo_dirs
    finally:
        os.kill(fuse_process.pid, signal.SIGTERM)
        fuse_process.join()
