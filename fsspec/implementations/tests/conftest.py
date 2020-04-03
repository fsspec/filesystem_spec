import pytest

from fsspec.implementations.cached import CachingFileSystem
from fsspec.implementations.dask import DaskWorkerFileSystem
from fsspec.implementations.ftp import FTPFileSystem
from fsspec.implementations.github import GithubFileSystem
from fsspec.implementations.hdfs import PyArrowHDFS
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.implementations.sftp import SFTPFileSystem
from fsspec.implementations.zip import ZipFileSystem

FILESYSTEMS = {
    # 'cached': CachingFileSystem,
    # 'ftp': FTPFileSystem,
    'dask': DaskWorkerFileSystem,
    'github': GithubFileSystem,
    'hdfs': PyArrowHDFS,
    # 'http': HTTPFileSystem,
    'local': LocalFileSystem,
    'memory': MemoryFileSystem,
    'sftp': SFTPFileSystem,
    'zip': ZipFileSystem,
}

READ_ONLY_FILESYSTEMS = [
    HTTPFileSystem,
]


@pytest.fixture(scope="function")
def fs(request):
    cls = FILESYSTEMS[request.param]
    return cls()
