import datetime
import time
from inspect import isfunction, signature

import pytest

from fsspec import AbstractFileSystem
from fsspec.implementations.tests.conftest import READ_ONLY_FILESYSTEMS


@pytest.mark.parametrize("fs", ["local"], indirect=["fs"])
def test_created(fs: AbstractFileSystem, temp_file):
    try:
        fs.touch(temp_file)
        created = fs.created(path=temp_file)
        assert isinstance(created, datetime.datetime)
    finally:
        if not isinstance(fs, tuple(READ_ONLY_FILESYSTEMS)):
            fs.rm(temp_file)


@pytest.mark.parametrize("fs", ["local", "memory", "arrow"], indirect=["fs"])
def test_modified(fs: AbstractFileSystem, temp_file):
    try:
        fs.touch(temp_file)
        # created = fs.created(path=temp_file)
        created = datetime.datetime.utcnow()  # pyarrow only have modified
        time.sleep(0.05)
        fs.touch(temp_file)
        modified = fs.modified(path=temp_file).replace(tzinfo=None)
        assert isinstance(modified, datetime.datetime)
        assert modified > created
    finally:
        fs.rm(temp_file)


@pytest.mark.parametrize(
    "method",
    [
 'cat',
 'cat_file',
 'cat_ranges',
 'checksum',
 'copy',
 'cp',
 'cp_file',
 'created',
 'delete',
 'disk_usage',
 'download',
 'du',
 'end_transaction',
 'exists',
 'expand_path',
 'find',
 'from_json',
 'get',
 'get_file',
 'get_mapper',
 'glob',
 'head',
 'info',
 'invalidate_cache',
 'isdir',
 'isfile',
 'lexists',
 'listdir',
 'ls',
 'makedir',
 'makedirs',
 'mkdir',
 'mkdirs',
 'modified',
 'move',
 'mv',
 'open',
 'pipe',
 'pipe_file',
 'put',
 'put_file',
 'read_block',
 'read_bytes',
 'read_text',
 'rename',
 'rm',
 'rm_file',
 'rmdir',
 'sign',
 'size',
 'sizes',
 'start_transaction',
 'stat',
 'tail',
 'to_json',
 'touch',
 'ukey',
 'unstrip_protocol',
 'upload',
 'walk',
 'write_bytes',
 'write_text']
)
@pytest.mark.parametrize(
    "fscls",
    [
        "file",
        "memory",
        "http",
        "zip",
        "tar",
        "sftp",
        "ssh",
        "ftp",
        "webhdfs",
        "cached",
        "blockcache",
        "filecache",
        "simplecache",
        "dask",
        "dbfs",
        "github",
        "git",
        "smb",
        "jupyter",
        "libarchive",
        "reference",
        "hdfs",
    ],
    indirect=["fscls"],
)
def test_signature(fscls, method):
    abstract_fs = AbstractFileSystem
    abs_signature = signature(getattr(abstract_fs, method))
    fs_signature = signature(getattr(fscls, method))
    # We only compare parameters shown in the abstract class
    # We allow extra parameters in the subclass
    for k1, k2 in zip(abs_signature.parameters, fs_signature.parameters):
        if k1 in ["self", "args", "kwargs"]:
            continue
        assert (
            abs_signature.parameters[k1].kind
            == fs_signature.parameters[k2].kind
        ), (
            f"{abs_signature} != {fs_signature}"
        )
        assert (
            abs_signature.parameters[k1].default
            == fs_signature.parameters[k2].default
        ), (
            f"{abs_signature} != {fs_signature}"
        )
