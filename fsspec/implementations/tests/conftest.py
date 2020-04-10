import tempfile

import pytest

from fsspec.implementations.local import LocalFileSystem

FILESYSTEMS = {"local": LocalFileSystem}

READ_ONLY_FILESYSTEMS = []


@pytest.fixture(scope="function")
def fs(request):
    cls = FILESYSTEMS[request.param]
    return cls()


@pytest.fixture(scope="function")
def temp_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        return temp_dir + "test-file"
