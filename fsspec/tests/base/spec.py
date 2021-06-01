import pytest


class BaseSpecTests:
    """Base class for all specification tests."""


class BaseFSTests(BaseSpecTests):
    """
    Tests that the fixture object provided meets expectations. Validate this first.
    """

    def test_files_exist(self, fs, prefix):
        assert fs.exists(f"{prefix}/exists")


class BaseReadTests(BaseSpecTests):
    """
    Tests that apply to read-only or read-write filesystems.
    """

    def test_ls_raises_filenotfound(self, fs, prefix):
        with pytest.raises(FileNotFoundError):
            fs.ls(f"{prefix}/not-a-key")

        with pytest.raises(FileNotFoundError):
            fs.ls(f"{prefix}/not/a/key")
