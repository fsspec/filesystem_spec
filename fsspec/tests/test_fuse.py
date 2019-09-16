import os
import pytest
from fsspec.implementations.memory import MemoryFileSystem

pytest.importorskip("fuse")
from fsspec.fuse import run
import time

import logging
logger = logging.getLogger(__name__)


def test_basic(tmpdir):
    tmpdir = str(tmpdir)
    fs = MemoryFileSystem()
    fs.touch('/mounted/testfile')
    logger.debug(fs)

    th = run(fs, '/mounted/', tmpdir, False)
    logger.debug("run %s", tmpdir)

    timeout = 10
    while True:
        try:
            logger.debug("listdir")
            # can fail with device not ready while waiting for fuse
            if 'testfile' in os.listdir(tmpdir):
                logger.debug("break")
                break
        except:
            pass
        timeout -= 1
        time.sleep(1)
        assert timeout > 0, "Timeout"

    logger.debug("write")
    fn = os.path.join(tmpdir, 'test')
    with open(fn, 'wb') as f:
        f.write(b'data')
    logger.debug("info")
    assert fs.info("/mounted/test")['size'] == 4

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
    os.mkdir(fn + '/inner')

    logger.debug("rmdir")
    with pytest.raises(OSError):
        os.rmdir(fn)

    os.rmdir(fn + "/inner")
    os.rmdir(fn)
    assert not fs.pseudo_dirs

    # should not normally kill a thread like this, but FUSE blocks, so we
    # cannot have thread listen for event. Alternative may be to .join() but
    # send a SIGINT
    logger.debug("release")
    th._tstate_lock.release()
    logger.debug("_stop")
    th._stop()
    logger.debug("join")
    th.join()
    logger.debug("clear")
    fs.store.clear()
