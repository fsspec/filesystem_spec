import os

import pytest

from fsspec.implementations.local import LocalFileSystem
from fsspec.tests.abstract.copy import AbstractCopyTests  # noqa
from fsspec.tests.abstract.get import AbstractGetTests  # noqa
from fsspec.tests.abstract.put import AbstractPutTests  # noqa


class AbstractFixtures:
    @staticmethod
    @pytest.fixture
    def fs_join():
        """
        Return a function that joins its arguments together into a path.

        Most fsspec implementations join paths in a platform-dependent way,
        but some will override this to always use a forward slash.
        """
        return os.path.join

    @staticmethod
    @pytest.fixture
    def local_fs():
        # Maybe need an option for auto_mkdir=False?  This is only relevant
        # for certain implementations.
        return LocalFileSystem(auto_mkdir=True)

    @staticmethod
    @pytest.fixture
    def local_join():
        """
        Return a function that joins its arguments together into a path, on
        the local filesystem.
        """
        return os.path.join

    @staticmethod
    @pytest.fixture
    def local_path(tmpdir):
        return tmpdir
