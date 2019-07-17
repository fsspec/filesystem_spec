import os
import pytest
from fsspec.implementations.memory import MemoryFileSystem
pytest.importorskip("fuse")
from fsspec.fuse import run
import time


def test_basic(tmpdir):
    tmpdir = str(tmpdir)
    fs = MemoryFileSystem()
    fs.touch('/mounted/testfile')
    run(fs, '/mounted/', tmpdir, False)
    timeout = 10
    while True:
        try:
            # can fail with device not ready while waiting for fuse
            if 'testfile' in os.listdir(tmpdir):
                break
        except:
            pass
        timeout -= 1
        time.sleep(1)
        assert timeout > 0, "Timeout"
    fn = os.path.join(tmpdir, 'test')
    with open(fn, 'wb') as f:
        f.write(b'data')
    assert fs.info("/mounted/test")['size'] == 4

    assert open(fn).read() == "data"
    os.remove(fn)

    os.mkdir(fn)
    assert os.listdir(fn) == []

    os.mkdir(fn + '/inner')

    with pytest.raises(OSError):
        os.rmdir(fn)

    os.rmdir(fn + '/inner')
    os.rmdir(fn)
    assert not fs.pseudo_dirs
