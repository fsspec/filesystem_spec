import fsspec.tests.abstract as abstract
from fsspec.implementations.tests.mount.mount_fixtures import MountFixtures


class TestMountCopy(abstract.AbstractCopyTests, MountFixtures):
    pass


class TestMountGet(abstract.AbstractGetTests, MountFixtures):
    pass


class TestMountPut(abstract.AbstractPutTests, MountFixtures):
    pass


class TestMountPipe(abstract.AbstractPipeTests, MountFixtures):
    pass


class TestMountOpen(abstract.AbstractOpenTests, MountFixtures):
    pass
