from fsspec.implementations.tests.memory.memory_fixtures import MemoryFixtures
from fsspec.tests.abstract.copy import AbstractCopyTests
from fsspec.tests.abstract.get import AbstractGetTests
from fsspec.tests.abstract.put import AbstractPutTests


class TestMemoryCopy(AbstractCopyTests, MemoryFixtures):
    pass


class TestMemoryGet(AbstractGetTests, MemoryFixtures):
    pass


class TestMemoryPut(AbstractPutTests, MemoryFixtures):
    pass
