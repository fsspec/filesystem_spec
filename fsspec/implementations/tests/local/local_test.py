import fsspec.tests.abstract as abstract
from fsspec.implementations.tests.local.local_fixtures import LocalFixtures


class TestLocalCopy(abstract.AbstractCopyTests, LocalFixtures):
    pass


class TestLocalGet(abstract.AbstractGetTests, LocalFixtures):
    pass


class TestLocalPut(abstract.AbstractPutTests, LocalFixtures):
    pass
