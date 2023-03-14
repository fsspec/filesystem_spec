import os

import pytest

from fsspec.implementations.local import LocalFileSystem
from fsspec.tests.abstract.copy import AbstractCopyTests  # noqa
from fsspec.tests.abstract.get import AbstractGetTests  # noqa
from fsspec.tests.abstract.put import AbstractPutTests  # noqa


class AbstractFixtures:
    def fs_join(self, *args):
        # Most fsspec implementations join paths in a platform-dependent way,
        # but some will override this to always use a forward slash.
        return os.path.join(*args)

    @staticmethod
    @pytest.fixture
    def local_fs():
        # Maybe need an option for auto_mkdir=False?  This is only relevant
        # for certain implementations.
        return LocalFileSystem(auto_mkdir=True)

    def local_join(self, *args):
        return os.path.join(*args)

    @staticmethod
    @pytest.fixture
    def local_path(tmpdir):
        return tmpdir
