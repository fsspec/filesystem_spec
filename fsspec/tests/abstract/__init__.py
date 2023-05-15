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

    @pytest.fixture
    def fs_scenario_cp(self, fs, fs_join, fs_path):
        """Scenario on remote filesystem that is used for many cp/get/put tests."""
        return self.scenario_cp(fs, fs_join, fs_path)

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

    def supports_empty_directories(self):
        """
        Return whether this implementation supports empty directories.
        """
        return True

    @pytest.fixture
    def local_scenario_cp(self, local_fs, local_join, local_path):
        """Scenario on local filesystem that is used for many cp/get/put tests."""
        return self.scenario_cp(local_fs, local_join, local_path)

    def scenario_cp(self, some_fs, some_join, some_path):
        """
        Scenario that is used for many cp/get/put tests. Creates the following
        directory and file structure:

        📁 source
        ├── 📄 file1
        ├── 📄 file2
        └── 📁 subdir
            ├── 📄 subfile1
            ├── 📄 subfile2
            └── 📁 nesteddir
                └── 📄 nestedfile
        """
        source = some_join(some_path, "source")
        subdir = some_join(source, "subdir")
        nesteddir = some_join(subdir, "nesteddir")
        some_fs.makedirs(nesteddir)
        some_fs.touch(some_join(source, "file1"))
        some_fs.touch(some_join(source, "file2"))
        some_fs.touch(some_join(subdir, "subfile1"))
        some_fs.touch(some_join(subdir, "subfile2"))
        some_fs.touch(some_join(nesteddir, "nestedfile"))
        return source
