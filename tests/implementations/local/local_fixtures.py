import pytest

from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.tests.abstract import AbstractFixtures


class LocalFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return LocalFileSystem(auto_mkdir=True)

    @pytest.fixture
    def fs_path(self, tmpdir):
        return str(tmpdir)

    @pytest.fixture
    def fs_sanitize_path(self):
        return make_path_posix
