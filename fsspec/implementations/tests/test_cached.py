import os
import pickle

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
        assert fs.cached_files['/out']['blocks']
    assert fs.cat('/out') == b'test'
    assert fs.cached_files['/out']['blocks'] is True

    with fs.open('/out', 'wb') as f:
        f.write(b'changed')

    assert fs.cat('/out') == b'test'  # old value
