import pytest

from fsspec import filesystem
from fsspec.tests.abstract import AbstractFixtures


class MemoryFixtures(AbstractFixtures):
    @staticmethod
    @pytest.fixture
    def fs():
        m = filesystem("memory")
        m.store.clear()
        m.pseudo_dirs.clear()
        m.pseudo_dirs.append("")
        try:
            yield m
        finally:
            m.store.clear()
            m.pseudo_dirs.clear()
            m.pseudo_dirs.append("")

    @staticmethod
    @pytest.fixture
    def fs_join():
        return lambda *args: "/".join(args)

    @staticmethod
    @pytest.fixture
    def fs_path():
        return ""
