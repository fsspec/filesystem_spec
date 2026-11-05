import tempfile

import pytest

import fsspec
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
    if request.param == "arrow":
        pyarrow_fs = pytest.importorskip("pyarrow.fs")
        FileSystem = pyarrow_fs.FileSystem
        fs = ArrowFSWrapper(FileSystem.from_uri("file:///")[0])
        return fs
    cls = FILESYSTEMS[request.param]
    return cls()


@pytest.fixture(scope="function")
def fscls(request):
    try:
        return fsspec.get_filesystem_class(request.param)
    except ImportError:
        pytest.skip(f"filesystem {request.param} not installed")


@pytest.fixture(scope="function")
def temp_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        return temp_dir + "test-file"
