from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
from io import BytesIO
from pathlib import Path

import pytest

import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.cached import WholeFileCacheFileSystem
from fsspec.implementations.tar import TarFileSystem
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
                "name": f"{d}",
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
        {"mode": "w", "suffix": ".tar", "magic": b"a\x00\x00\x00\x00"},
        {"mode": "w:gz", "suffix": ".tar.gz", "magic": b"\x1f\x8b\x08\x08"},
        {"mode": "w:bz2", "suffix": ".tar.bz2", "magic": b"BZh91AY"},
        {"mode": "w:xz", "suffix": ".tar.xz", "magic": b"\xfd7zXZ\x00\x00"},
    ],
    ids=["tar", "tar-gz", "tar-bz2", "tar-xz"],
)
def test_compressions(recipe):
    """
    Run tests on all available tar file compression variants.
    """
    with temptar(archive_data, mode=recipe["mode"], suffix=recipe["suffix"]) as t:
        fs = fsspec.filesystem("tar", fo=t)

        # Verify that the tar archive has the correct compression.
        with open(t, "rb") as raw:
            assert raw.read()[:10].startswith(recipe["magic"])

        # Verify content of a sample file.
        assert fs.cat("b") == b"hello"


@pytest.mark.parametrize(
    "recipe",
    [
        {"mode": "w", "suffix": ".tar", "magic": b"a\x00\x00\x00\x00"},
        {"mode": "w:gz", "suffix": ".tar.gz", "magic": b"\x1f\x8b\x08\x08"},
        {"mode": "w:bz2", "suffix": ".tar.bz2", "magic": b"BZh91AY"},
        {"mode": "w:xz", "suffix": ".tar.xz", "magic": b"\xfd7zXZ\x00\x00"},
    ],
    ids=["tar", "tar-gz", "tar-bz2", "tar-xz"],
)
def test_filesystem_direct(recipe, tmpdir):
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

    # Verify that the tar archive has the correct compression.
    with open(filename, "rb") as raw:
        assert raw.read()[:10].startswith(recipe["magic"])

    # Verify content of a sample file.
    with fs.open(filename) as resource:
        tarfs = fsspec.filesystem("tar", fo=resource)
        assert tarfs.cat("b") == b"hello"


@pytest.mark.parametrize(
    "recipe",
    [
        {"mode": "w", "suffix": ".tar", "magic": b"a\x00\x00\x00\x00"},
        {"mode": "w:gz", "suffix": ".tar.gz", "magic": b"\x1f\x8b\x08\x08"},
        {"mode": "w:bz2", "suffix": ".tar.bz2", "magic": b"BZh91AY"},
        {"mode": "w:xz", "suffix": ".tar.xz", "magic": b"\xfd7zXZ\x00\x00"},
    ],
    ids=["tar", "tar-gz", "tar-bz2", "tar-xz"],
)
def test_filesystem_cached(recipe, tmpdir):
    """
    Run tests through a real, cached, fsspec filesystem implementation.
    Here: `TarFileSystem` over `WholeFileCacheFileSystem` over `LocalFileSystem`.
    """

    filename = os.path.join(tmpdir, f'temp{recipe["suffix"]}')

    # Create a filesystem from test fixture.
    fs = fsspec.filesystem("file")
    f = OpenFile(fs, filename, mode="wb")

    with temptar(archive_data, mode=recipe["mode"], suffix=recipe["suffix"]) as tf:
        with f as fo:
            fo.write(open(tf, "rb").read())

    # Verify that the tar archive has the correct compression.
    with open(filename, "rb") as raw:
        assert raw.read()[:10].startswith(recipe["magic"])

    # Access cached filesystem.
    cachedir = tempfile.mkdtemp()
    filesystem = WholeFileCacheFileSystem(fs=fs, cache_storage=cachedir)

    # Verify the cache is empty beforehand.
    assert os.listdir(cachedir) == []

    # Verify content of a sample file.
    with filesystem.open(filename) as resource:
        tarfs = fsspec.filesystem("tar", fo=resource)
        assert tarfs.cat("b") == b"hello"

    # Verify the cache is populated afterwards.
    assert len(os.listdir(cachedir)) == 2

    # Verify that the cache is empty after clearing it.
    filesystem.clear_cache()
    assert os.listdir(cachedir) == []

    filesystem.clear_cache()
    shutil.rmtree(cachedir)


@pytest.mark.parametrize(
    "recipe",
    [
        {"mode": "w", "suffix": ".tar", "magic": b"a\x00\x00\x00\x00"},
        {"mode": "w:gz", "suffix": ".tar.gz", "magic": b"\x1f\x8b\x08\x08"},
        {"mode": "w:bz2", "suffix": ".tar.bz2", "magic": b"BZh91AY"},
        {"mode": "w:xz", "suffix": ".tar.xz", "magic": b"\xfd7zXZ\x00\x00"},
    ],
    ids=["tar", "tar-gz", "tar-bz2", "tar-xz"],
)
def test_url_to_fs_direct(recipe, tmpdir):
    with temptar(archive_data, mode=recipe["mode"], suffix=recipe["suffix"]) as tf:
        url = f"tar://inner::file://{tf}"
        fs, url = fsspec.core.url_to_fs(url=url)
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
def test_url_to_fs_cached(recipe, tmpdir):
    with temptar(archive_data, mode=recipe["mode"], suffix=recipe["suffix"]) as tf:
        url = f"tar://inner::simplecache::file://{tf}"
        # requires same_names in order to be able to guess compression from
        # filename
        fs, url = fsspec.core.url_to_fs(url=url, simplecache={"same_names": True})
        assert fs.cat("b") == b"hello"


@pytest.mark.parametrize(
    "compression", ["", "gz", "bz2", "xz"], ids=["tar", "tar-gz", "tar-bz2", "tar-xz"]
)
def test_ls_with_folders(compression: str, tmp_path: Path):
    """
    Create a tar file that doesn't include the intermediate folder structure,
    but make sure that the reading filesystem is still able to resolve the
    intermediate folders, like the ZipFileSystem.
    """
    tar_data: dict[str, bytes] = {
        "a.pdf": b"Hello A!",
        "b/c.pdf": b"Hello C!",
        "d/e/f.pdf": b"Hello F!",
        "d/g.pdf": b"Hello G!",
    }
    if compression:
        temp_archive_file = tmp_path / f"test_tar_file.tar.{compression}"
    else:
        temp_archive_file = tmp_path / "test_tar_file.tar"
    with open(temp_archive_file, "wb") as fd:
        # We need to manually write the tarfile here, because temptar
        # creates intermediate directories which is not how tars are always created
        with tarfile.open(fileobj=fd, mode=f"w:{compression}") as tf:
            for tar_file_path, data in tar_data.items():
                content = data
                info = tarfile.TarInfo(name=tar_file_path)
                info.size = len(content)
                tf.addfile(info, BytesIO(content))
    with open(temp_archive_file, "rb") as fd:
        fs = TarFileSystem(fd)
        assert fs.find("/", withdirs=True) == [
            "a.pdf",
            "b",
            "b/c.pdf",
            "d",
            "d/e",
            "d/e/f.pdf",
            "d/g.pdf",
        ]
