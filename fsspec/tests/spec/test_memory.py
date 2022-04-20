import pytest

from fsspec.implementations.memory import MemoryFile, MemoryFileSystem
from fsspec.tests.base import BaseFSTests, BaseReadTests


@pytest.fixture
def prefix():
    return "/root/"


@pytest.fixture
def fs(prefix):
    memfs = MemoryFileSystem()
    memfs.store[f"{prefix}/exists"] = MemoryFile(
        fs=memfs, path=f"{prefix}/exists", data=b"data from /exists"
    )
    return memfs


class TestFS(BaseFSTests):
    pass


class TestRead(BaseReadTests):
    @pytest.mark.xfail(
        strict=True, reason="https://github.com/intake/filesystem_spec/issues/652"
    )
    def test_ls_raises_filenotfound(self, fs, prefix):
        return super().test_ls_raises_filenotfound(fs, prefix)
