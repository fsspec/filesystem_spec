import datetime
import time

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
        created = datetime.datetime.now(
            tz=datetime.timezone.utc
        )  # pyarrow only have modified
        time.sleep(0.05)
        fs.touch(temp_file)
        modified = fs.modified(path=temp_file)
        assert isinstance(modified, datetime.datetime)
        assert modified > created
    finally:
        fs.rm(temp_file)
