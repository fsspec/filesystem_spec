import os

import pytest

import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.tests.test_archive import archive_data, temptar


def test_info():
    with temptar(archive_data) as t:
        fs = fsspec.filesystem("tar", fo=t)

        # Iterate over all directories.
        # Probe specific fields of Tar archives.
        for d in fs._all_dirnames(archive_data.keys()):
            lhs = fs.info(d)
            del lhs["chksum"]
            expected = {
                "name": f"{d}/",
                "size": 0,
                "type": "directory",
                "devmajor": 0,
                "devminor": 0,
                "gname": "",
                "linkname": "",
                "uid": 0,
                "gid": 0,
                "mode": 420,
                "mtime": 0,
                "uname": "",
            }
            assert lhs == expected

        # Iterate over all files.
        for f, v in archive_data.items():
            lhs = fs.info(f)

            # Probe some specific fields of Tar archives.
            assert "mode" in lhs
            assert "uid" in lhs
            assert "gid" in lhs
            assert "mtime" in lhs
            assert "chksum" in lhs


@pytest.mark.parametrize(
    "recipe",
    [
        {"mode": "w", "suffix": ".tar"},
        {"mode": "w:gz", "suffix": ".tar.gz"},
        {"mode": "w:bz2", "suffix": ".tar.bz2"},
        {"mode": "w:xz", "suffix": ".tar.xz"},
    ],
    ids=["tar", "tar-gz", "tar-bz2", "tar-xz"],
)
def test_compressions(recipe):
    """
    Run tests on all available tar file compression variants.
    """
    with temptar(archive_data, mode=recipe["mode"], suffix=recipe["suffix"]) as t:
        fs = fsspec.filesystem("tar", fo=t)
        assert fs.cat("b") == b"hello"


@pytest.mark.parametrize(
    "recipe",
    [
        {"mode": "w", "suffix": ".tar"},
        {"mode": "w:gz", "suffix": ".tar.gz"},
        {"mode": "w:bz2", "suffix": ".tar.bz2"},
        {"mode": "w:xz", "suffix": ".tar.xz"},
    ],
    ids=["tar", "tar-gz", "tar-bz2", "tar-xz"],
)
def test_filesystem(recipe, tmpdir):
    """
    Run tests through a real fsspec filesystem implementation.
    Here: `LocalFileSystem`.
    """

    filename = os.path.join(tmpdir, f'temp{recipe["suffix"]}')

    fs = fsspec.filesystem("file")
    f = OpenFile(fs, filename, mode="wb")

    with temptar(archive_data, mode=recipe["mode"], suffix=recipe["suffix"]) as tf:
        with f as fo:
            fo.write(open(tf, "rb").read())

    with fs.open(filename) as resource:
        tarfs = fsspec.filesystem("tar", fo=resource)
        assert tarfs.cat("b") == b"hello"
