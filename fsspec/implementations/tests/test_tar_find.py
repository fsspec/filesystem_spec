from io import BytesIO
from tarfile import DIRTYPE, LNKTYPE, SYMTYPE, TarInfo

import pytest

from fsspec.implementations.tar import TarFileSystem
from fsspec.spec import AbstractFileSystem


@pytest.fixture
def tar_find_fs():
    import tarfile

    stream = BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for name, kind, target in [
            ("./", DIRTYPE, ""),
            ("empty", DIRTYPE, ""),
            ("project/empty", DIRTYPE, ""),
            ("project/src/main.py", None, ""),
            ("project/src/data.json", None, ""),
            ("project-old/notes.txt", None, ""),
            ("project//./README.md", None, ""),
            ("project/README.md", None, ""),
            ("../literal.txt", None, ""),
            ("project/../literal.txt", None, ""),
            (r"project/back\slash.txt", None, ""),
            ("project/link", SYMTYPE, "README.md"),
            ("hardlink", LNKTYPE, "project/README.md"),
        ]:
            member = TarInfo(name)
            if kind is not None:
                member.type = kind
                member.linkname = target
                archive.addfile(member)
            else:
                member.size = 4
                archive.addfile(member, BytesIO(b"data"))
    stream.seek(0)
    fs = TarFileSystem(stream)
    try:
        yield fs
    finally:
        fs.close()


@pytest.mark.parametrize(
    "path",
    ["", "./", "project", "tar://./project/", "empty", "missing", "project/README.md"],
)
@pytest.mark.parametrize("maxdepth", [None, 1, 2, 3])
@pytest.mark.parametrize("withdirs", [False, True])
@pytest.mark.parametrize("detail", [False, True])
def test_find_matches_walk(tar_find_fs, path, maxdepth, withdirs, detail):
    fs = tar_find_fs
    expected = AbstractFileSystem.find(fs, path, maxdepth, withdirs, detail)
    fs.dir_cache = None
    assert fs.find(path, maxdepth, withdirs, detail) == expected


@pytest.mark.parametrize("path", ["", "a", "a/nested"])
@pytest.mark.parametrize("maxdepth", [None, 1, 2])
@pytest.mark.parametrize("withdirs", [False, True])
@pytest.mark.parametrize("detail", [False, True])
def test_find_file_masking_directory(path, maxdepth, withdirs, detail):
    import tarfile

    stream = BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for name in ["a", "a/file.txt", "a/nested/data.json", "other/data.json"]:
            archive.addfile(TarInfo(name), BytesIO())
    stream.seek(0)
    fs = TarFileSystem(stream)
    try:
        expected = AbstractFileSystem.find(fs, path, maxdepth, withdirs, detail)
        assert fs.find(path, maxdepth, withdirs, detail) == expected
    finally:
        fs.close()


@pytest.mark.parametrize("maxdepth", [0, -1, "2", 1.5, True])
def test_find_unusual_depth_matches_walk(tar_find_fs, maxdepth):
    fs = tar_find_fs
    try:
        expected = AbstractFileSystem.find(fs, "project", maxdepth)
    except (TypeError, ValueError) as error:
        with pytest.raises(type(error), match=str(error)):
            fs.find("project", maxdepth)
    else:
        assert fs.find("project", maxdepth) == expected


def test_find_custom_walk_options(tar_find_fs):
    assert tar_find_fs.find("project", topdown=False) == AbstractFileSystem.find(
        tar_find_fs, "project", topdown=False
    )
    with pytest.raises(FileNotFoundError):
        tar_find_fs.find("missing", on_error="raise")


def test_find_avoids_directory_rescans(tar_find_fs, monkeypatch):
    expected = AbstractFileSystem.find(tar_find_fs, "project", detail=True)

    def fail(*args, **kwargs):
        pytest.fail("find should not list each directory separately")

    monkeypatch.setattr(tar_find_fs, "ls", fail)
    assert tar_find_fs.find("project", detail=True) == expected


def test_find_respects_subclass_walk(tar_find_fs):
    class CustomTarFileSystem(TarFileSystem):
        def walk(self, *args, **kwargs):
            return iter([])

    tar_find_fs.fo.seek(0)
    fs = CustomTarFileSystem(tar_find_fs.fo)
    try:
        assert fs.find("project") == []
    finally:
        fs.close()
