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
def test_signature(fscls):
    abstract_fs = AbstractFileSystem
    for method in dir(abstract_fs):
        if isfunction(getattr(abstract_fs, method)):
            if method.startswith("_"):
                continue
            abs_signature = signature(getattr(abstract_fs, method))
            fs_signature = signature(getattr(fscls, method))
            # assert abs_signature == fs_signature
            for k1, k2 in zip(abs_signature.parameters, fs_signature.parameters):
                if k1 in ["self", "args", "kwargs"]:
                    continue
                assert (
                    abs_signature.parameters[k1].kind
                    == fs_signature.parameters[k2].kind
                ), (
                    f"Paramete {k2} of {method} in {fscls.__name__} "
                    "don't have the same kind with the one in base class"
                )
                assert (
                    abs_signature.parameters[k1].default
                    == fs_signature.parameters[k2].default
                ), (
                    f"Paramete {k2} of {method} in {fscls.__name__} don't"
                    " have the same default value with the one in base class"
                )
