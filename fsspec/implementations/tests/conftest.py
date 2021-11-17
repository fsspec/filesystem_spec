import tempfile

import pytest

import fsspec
from fsspec.implementations.local import LocalFileSystem


# A dummy filesystem that has a list of protocols
class MultiProtocolFileSystem(LocalFileSystem):
    protocol = ["file", "other"]


FILESYSTEMS = {"local": LocalFileSystem, "multi": MultiProtocolFileSystem}

READ_ONLY_FILESYSTEMS = []


@pytest.fixture(scope="function")
def fs(request):
    cls = FILESYSTEMS[request.param]
    return cls()


@pytest.fixture(scope="function")
def temp_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        return temp_dir + "test-file"


@pytest.fixture()
def m():
    # memory filesystem with clean state
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    yield fs
    fs.store.clear()
