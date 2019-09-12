import os
import pickle
import pytest

import fsspec
from fsspec.implementations.cached import CachingFileSystem
from .test_ftp import ftp_writable, FTPFileSystem


def test_idempotent():
    fs = CachingFileSystem('file')
    fs2 = CachingFileSystem('file')
    assert fs2 is fs
    fs3 = pickle.loads(pickle.dumps(fs))
    assert fs3.storage == fs.storage


def test_worflow(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open('/out', 'wb') as f:
        f.write(b'test')
    fs = fsspec.filesystem('cached', target_protocol='ftp',
                           storage_options={'host': host, 'port': port,
                                            'username': user, 'password': pw})
    assert os.listdir(fs.storage) == []
    with fs.open('/out') as f:
        assert os.listdir(fs.storage)
        assert f.read() == b'test'
        assert fs.cached_files['ftp:///out']['blocks']
    assert fs.cat('/out') == b'test'
    assert fs.cached_files['ftp:///out']['blocks'] is True

    with fs.open('/out', 'wb') as f:
        f.write(b'changed')

    assert fs.cat('/out') == b'test'  # old value


def test_blocksize(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open('/out', 'wb') as f:
        f.write(b'test')

    fs = fsspec.filesystem('cached', target_protocol='ftp',
                           storage_options={'host': host, 'port': port,
                                            'username': user, 'password': pw})

    assert fs.cat('/out') == b'test'
    with pytest.raises(ValueError):
        fs.open('/out', block_size=1)


def test_local_filecache_basic():
    import tempfile
    d1 = tempfile.mkdtemp()
    d2 = tempfile.mkdtemp()
    f1 = os.path.join(d1, 'afile')
    data = b'test data'
    with open(f1, 'wb') as f:
        f.write(data)
    fs = fsspec.filesystem('filecache', target_protocol='file',
                           cache_storage=d2)
    with fs.open(f1, 'rb') as f:
        assert f.read() == data
    assert 'cache' in os.listdir(d2)
    fn = list(fs.cached_files.values())[0]['fn']
    assert fn in os.listdir(d2)
    with open(os.path.join(d2, fn), 'rb') as f:
        assert f.read() == data
