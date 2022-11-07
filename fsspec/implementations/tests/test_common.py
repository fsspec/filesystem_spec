import datetime
import time

import pytest

from fsspec import AbstractFileSystem
from fsspec.implementations.tests.conftest import READ_ONLY_FILESYSTEMS
from inspect import signature, isfunction


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

# TODO: add more filesystems
@pytest.mark.parametrize("fscls", ["file", "memory", "hdfs"], indirect=["fscls"])
def test_signature(fscls):
    abstract_fs = AbstractFileSystem
    for method in dir(abstract_fs):
        if isfunction(getattr(abstract_fs, method)):
            print(method)
            if method.startswith('_'): continue
            abs_signature = signature(getattr(abstract_fs, method))
            fs_signature = signature(getattr(fscls, method))
            # assert abs_signature == fs_signature
            for k in abs_signature.parameters:
                if k in ['self', 'args', 'kwargs']: continue
                assert abs_signature.parameters[k] == fs_signature.parameters[k]
    assert False