import pytest

from fsspec import AbstractFileSystem, filesystem, register_implementation, url_to_fs
from fsspec.implementations.cached import ChainedFileSystem


class MyChainedFS(ChainedFileSystem):
    protocol = "mychain"

    def __init__(self, target_protocol="", target_options=None, **kwargs):
        super().__init__(**kwargs)
        self.fs = filesystem(target_protocol, **target_options)


class MyNonChainedFS(AbstractFileSystem):
    protocol = "mynonchain"


@pytest.fixture(scope="module")
def register_fs():
    register_implementation(MyChainedFS.protocol, MyChainedFS)
    register_implementation(MyNonChainedFS.protocol, MyNonChainedFS)
    yield


def test_token_passthrough_to_chained(register_fs):
    # First, run a sanity check
    fs, rest = url_to_fs("mynonchain://path/to/file")
    assert isinstance(fs, MyNonChainedFS)
    assert fs.protocol == "mynonchain"
    assert rest == "path/to/file"

    # Now test that the chained FS works
    fs, rest = url_to_fs("mychain::mynonchain://path/to/file")
    assert isinstance(fs, MyChainedFS)
    assert fs.protocol == "mychain"
    assert rest == "path/to/file"
    assert isinstance(fs.fs, MyNonChainedFS)
    assert fs.fs.protocol == "mynonchain"
