import pytest

from fsspec.implementations.local import LocalFileSystem
from fsspec.tests.abstract import AbstractFixtures


class LocalFixtures(AbstractFixtures):
    @staticmethod
    @pytest.fixture
    def fs():
        return LocalFileSystem(auto_mkdir=True)

    @staticmethod
    @pytest.fixture
    def fs_path(tmpdir):
        return str(tmpdir)
