import pytest

from fsspec.implementations.local import LocalFileSystem
from fsspec.tests.abstract import AbstractFixtures


class LocalFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return LocalFileSystem(auto_mkdir=True)

    @pytest.fixture
    def fs_path(self, tmpdir):
        return str(tmpdir)
