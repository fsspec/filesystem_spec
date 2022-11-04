import tempfile

import pytest

from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

# A dummy filesystem that has a list of protocols


class MultiProtocolFileSystem(LocalFileSystem):
    protocol = ["file", "other"]


FILESYSTEMS = {
    "local": LocalFileSystem,
    "multi": MultiProtocolFileSystem,
    "memory": MemoryFileSystem,
}

READ_ONLY_FILESYSTEMS = []


@pytest.fixture(scope="function")
def fs(request):
    pyarrow_fs = pytest.importorskip("pyarrow.fs")
    FileSystem = pyarrow_fs.FileSystem
    if request.param == "arrow":
        fs = ArrowFSWrapper(FileSystem.from_uri("file:///")[0])
        return fs
    cls = FILESYSTEMS[request.param]
    return cls()


@pytest.fixture(scope="function")
def temp_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        return temp_dir + "test-file"
