import os

import pytest


class AbstractOpenTests:
    def test_open_exclusive(self, fs, fs_target):
        with fs.open(fs_target, "wb") as f:
            f.write(b"data")
        with fs.open(fs_target, "rb") as f:
            assert f.read() == b"data"
        with pytest.raises(FileExistsError):
            fs.open(fs_target, "xb")

    def test_open_not_pathlike(self, fs, fs_target):
        with fs.open(fs_target, "wb") as f:
            assert not isinstance(f, os.PathLike)
