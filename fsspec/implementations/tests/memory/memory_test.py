import fsspec.tests.abstract as abstract
from fsspec.implementations.tests.memory.memory_fixtures import MemoryFixtures


class TestMemoryCopy(abstract.AbstractCopyTests, MemoryFixtures):
    pass


class TestMemoryGet(abstract.AbstractGetTests, MemoryFixtures):
    pass


class TestMemoryPut(abstract.AbstractPutTests, MemoryFixtures):
    pass
