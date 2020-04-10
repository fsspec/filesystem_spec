import datetime
import time
import pytest

from fsspec import AbstractFileSystem
from fsspec.implementations.tests.conftest import READ_ONLY_FILESYSTEMS

TEST_FILE = "file"


@pytest.mark.parametrize("fs", ["local"], indirect=["fs"])
def test_created(fs: AbstractFileSystem):
    try:
        fs.touch(TEST_FILE)
        created = fs.created(path=TEST_FILE)
        assert isinstance(created, datetime.datetime)
    finally:
        if not isinstance(fs, tuple(READ_ONLY_FILESYSTEMS)):
            fs.rm(TEST_FILE)


@pytest.mark.parametrize("fs", ["local"], indirect=["fs"])
def test_modified(fs: AbstractFileSystem):
    try:
        fs.touch(TEST_FILE)
        created = fs.created(path=TEST_FILE)
        time.sleep(0.05)
        fs.touch(TEST_FILE)
        modified = fs.modified(path=TEST_FILE)
        assert isinstance(modified, datetime.datetime)
        assert modified > created
    finally:
        fs.rm(TEST_FILE)
