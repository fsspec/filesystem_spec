import datetime

import pytest

from fsspec import AbstractFileSystem
from fsspec.implementations.tests.conftest import READ_ONLY_FILESYSTEMS

TEST_FILE = 'file'


@pytest.mark.parametrize("fs", ['local'], indirect=["fs"])
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
        fs.touch(TEST_FILE)
        modified = fs.modified(path=TEST_FILE)
        assert modified > created
        assert isinstance(created, datetime.datetime)
    finally:
        fs.rm(TEST_FILE)
