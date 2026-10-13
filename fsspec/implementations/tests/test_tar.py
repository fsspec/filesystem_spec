from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
from io import BytesIO
from typing import TYPE_CHECKING

import pytest

import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.cached import WholeFileCacheFileSystem
from fsspec.implementations.tar import TarFileSystem
from fsspec.implementations.tests.test_archive import archive_data, temptar

if TYPE_CHECKING:
    from pathlib import Path


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
        for f in archive_data:
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

    filename = os.path.join(tmpdir, f"temp{recipe['suffix']}")

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

    filename = os.path.join(tmpdir, f"temp{recipe['suffix']}")

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


@pytest.mark.parametrize(
    "compression", ["", "gz", "bz2", "xz"], ids=["tar", "tar-gz", "tar-bz2", "tar-xz"]
)
def test_ls_with_duplicate_slashes(compression: str, tmp_path: Path):
    """
    Members whose names contain redundant duplicate slashes (e.g.
    ``"a/b//c.txt"``) must still be reachable through the directory listing
    and openable, rather than becoming silently invisible to
    ``find``/``glob``/``ls``/``walk``. Regression test for
    https://github.com/fsspec/filesystem_spec/issues/1947.
    """
    tar_data: dict[str, bytes] = {
        "path/with/extra/slash//test.txt": b"Hello slash!",
        "regular/file.txt": b"Hello regular!",
    }
    if compression:
        temp_archive_file = tmp_path / f"test_tar_file.tar.{compression}"
    else:
        temp_archive_file = tmp_path / "test_tar_file.tar"
    with open(temp_archive_file, "wb") as fd:
        with tarfile.open(fileobj=fd, mode=f"w:{compression}") as tf:
            for tar_file_path, data in tar_data.items():
                info = tarfile.TarInfo(name=tar_file_path)
                info.size = len(data)
                tf.addfile(info, BytesIO(data))

    with open(temp_archive_file, "rb") as fd:
        fs = TarFileSystem(fd)

        # The duplicate-slash member is discoverable with its slashes collapsed.
        assert fs.find("/") == [
            "path/with/extra/slash/test.txt",
            "regular/file.txt",
        ]
        assert fs.glob("path/**/*.txt") == ["path/with/extra/slash/test.txt"]
        assert fs.ls("path/with/extra/slash", detail=False) == [
            "path/with/extra/slash/test.txt"
        ]

        # The intermediate directory is inferred without a trailing slash.
        assert fs.isdir("path/with/extra/slash")

        # It can be opened both by its normalised name and its original name.
        assert fs.cat("path/with/extra/slash/test.txt") == b"Hello slash!"
        assert fs.cat("path/with/extra/slash//test.txt") == b"Hello slash!"


@pytest.fixture
def tar_with_one_member(tmp_path):
    path = tmp_path / "archive.tar"
    data = b"data"
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("present.txt")
        info.size = len(data)
        tar.addfile(info, BytesIO(data))
    return path


@pytest.mark.parametrize(
    "read",
    [
        lambda fs: fs.open("missing.txt").read(),
        lambda fs: fs.cat("missing.txt"),
    ],
    ids=["open", "cat"],
)
def test_reading_missing_member_raises_file_not_found(tar_with_one_member, read):
    fs = TarFileSystem(str(tar_with_one_member))
    assert fs.cat("present.txt") == b"data"
    with pytest.raises(FileNotFoundError):
        read(fs)


@pytest.mark.parametrize(
    "start, end, expected",
    [(-2, None, b"ta"), (1, -1, b"at"), (None, -3, b"d")],
)
def test_cat_file_negative_offsets(tar_with_one_member, start, end, expected):
    fs = TarFileSystem(str(tar_with_one_member))
    assert fs.cat_file("present.txt", start=start, end=end) == expected


def test_links_report_target_size(tmp_path: Path):
    path = tmp_path / "links.tar"
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("d/f")
        info.size = 5
        tar.addfile(info, BytesIO(b"hello"))
        for name, kind, target in [
            ("d/sym", tarfile.SYMTYPE, "f"),
            ("hard", tarfile.LNKTYPE, "d/f"),
            ("dangling", tarfile.SYMTYPE, "missing"),
        ]:
            info = tarfile.TarInfo(name)
            info.type = kind
            info.linkname = target
            tar.addfile(info)

    fs = TarFileSystem(str(path))
    for name in ["d/sym", "hard"]:
        assert fs.size(name) == 5
        assert fs.cat_file(name, start=-2) == b"lo"
        assert fs.read_block(name, 1, 3) == b"ell"
    assert fs.size("dangling") == 0


@pytest.mark.parametrize("prefix", ["./", "././"])
@pytest.mark.parametrize("explicit_directories", [False, True])
def test_dot_path_components(tmp_path, prefix, explicit_directories):
    path = tmp_path / "dots.tar"
    members = [
        (prefix + "dir/./nested.txt", "dir/nested.txt", b"nested"),
        (prefix + ".hidden", ".hidden", b"hidden"),
        ("plain.txt", "plain.txt", b"plain"),
    ]
    with tarfile.open(path, "w") as tar:
        if explicit_directories:
            for name in [prefix, prefix + "dir/./"]:
                info = tarfile.TarInfo(name)
                info.type = tarfile.DIRTYPE
                tar.addfile(info)
        for name, _, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

    fs = TarFileSystem(str(path))
    assert fs.ls("", detail=False) == [".hidden", "dir", "plain.txt"]
    assert fs.ls(prefix, detail=False) == fs.ls("", detail=False)
    assert fs.find("") == [".hidden", "dir/nested.txt", "plain.txt"]
    assert fs.glob("./dir/*.txt") == ["dir/nested.txt"]
    for directory in ["dir", prefix + "dir/./", "tar://./dir"]:
        assert fs.isdir(directory)
        assert fs.info(directory)["name"] == "dir"
        assert fs.ls(directory, detail=False) == ["dir/nested.txt"]
        assert [entry["name"] for entry in fs.ls(directory)] == ["dir/nested.txt"]
    for raw_name, name, data in members:
        assert fs.info(name)["name"] == name
        assert fs.info(raw_name)["name"] == name
        assert fs.cat(name) == data
        with fs.open(raw_name, "rb") as f:
            assert f.read() == data


def test_dot_root_directory(tmp_path):
    path = tmp_path / "root.tar"
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("./")
        info.type = tarfile.DIRTYPE
        tar.addfile(info)

    fs = TarFileSystem(str(path))
    assert fs.info(".")["type"] == "directory"
    assert fs.info("")["type"] == "directory"
    assert fs.ls(".") == []
    assert fs.ls("", detail=False) == []
    assert fs.find("") == []


def test_dot_normalization_preserves_parent_components(tmp_path):
    path = tmp_path / "parents.tar"
    members = {"../file.txt": b"parent", "dir/../file.txt": b"literal"}
    with tarfile.open(path, "w") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

    fs = TarFileSystem(str(path))
    for name, data in members.items():
        assert fs.info(name)["name"] == name
        assert fs.cat_file("./" + name) == data
    assert not fs.exists("file.txt")


@pytest.mark.parametrize(
    "names", [["./file.txt", "file.txt"], ["file.txt", "./file.txt"]]
)
def test_dot_normalization_duplicate_members(tmp_path, names):
    path = tmp_path / "duplicates.tar"
    with tarfile.open(path, "w") as tar:
        for name, data in zip(names, [b"first", b"last"]):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

    fs = TarFileSystem(str(path))
    assert fs.ls("", detail=False) == ["file.txt"]
    assert fs.size("file.txt") == len(b"last")
    assert fs.cat("file.txt") == b"last"
    assert fs.cat("./file.txt") == b"last"


def test_dot_normalization_preserves_links(tmp_path):
    path = tmp_path / "dot-links.tar"
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("./dir/target")
        info.size = 5
        tar.addfile(info, BytesIO(b"hello"))
        for name, kind, target in [
            ("./dir/sym", tarfile.SYMTYPE, "target"),
            ("./hard", tarfile.LNKTYPE, "./dir/target"),
        ]:
            info = tarfile.TarInfo(name)
            info.type = kind
            info.linkname = target
            tar.addfile(info)

    fs = TarFileSystem(str(path))
    for name in ["dir/target", "dir/sym", "hard"]:
        assert fs.size(name) == 5
        assert fs.cat(name) == b"hello"
        assert fs.cat_file(name, start=-2) == b"lo"
