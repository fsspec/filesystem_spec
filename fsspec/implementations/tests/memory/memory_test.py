import pytest

import fsspec.tests.abstract as abstract
from fsspec.implementations.tests.memory.memory_fixtures import MemoryFixtures


class TestMemoryCopy(abstract.AbstractCopyTests, MemoryFixtures):
    pass


class TestMemoryGet(abstract.AbstractGetTests, MemoryFixtures):
    @pytest.mark.skip(reason="Bug: does not auto-create new directory")
    def test_get_file_to_new_directory(self):
        pass

    @pytest.mark.skip(reason="Bug: does not auto-create new directory")
    def test_get_file_to_file_in_new_directory(self):
        pass

    @pytest.mark.skip(reason="Bug: does not auto-create new directory")
    def test_get_glob_to_new_directory(self):
        pass

    @pytest.mark.skip(reason="Bug: does not auto-create new directory")
    def test_get_list_of_files_to_new_directory(self):
        pass


class TestMemoryPut(abstract.AbstractPutTests, MemoryFixtures):
    pass
