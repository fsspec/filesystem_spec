from fsspec.implementations.tests.local.local_fixtures import LocalFixtures
from fsspec.tests.abstract.copy import AbstractCopyTests
from fsspec.tests.abstract.get import AbstractGetTests
from fsspec.tests.abstract.put import AbstractPutTests


class TestLocalCopy(AbstractCopyTests, LocalFixtures):
    pass


class TestLocalGet(AbstractGetTests, LocalFixtures):
    pass


class TestLocalPut(AbstractPutTests, LocalFixtures):
    pass
