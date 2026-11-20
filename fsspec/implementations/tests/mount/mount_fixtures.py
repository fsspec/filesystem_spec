import pytest

from fsspec import filesystem
from fsspec.implementations.mount import MountFileSystem
from fsspec.tests.abstract import AbstractFixtures


class MountFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    @classmethod
    def fs(cls):
        m = filesystem("memory")
        m.store.clear()
        m.pseudo_dirs.clear()
        m.pseudo_dirs.append("")
        try:
            yield MountFileSystem({"/mnt": (m, "/")})
        finally:
            m.store.clear()
            m.pseudo_dirs.clear()
            m.pseudo_dirs.append("")

    @pytest.fixture
    def fs_join(self):
        return lambda *args: "/".join(args)

    @pytest.fixture
    def fs_path(self):
        return "/mnt"
