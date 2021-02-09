import os
import pickle
import sys
import tempfile
import zipfile
from contextlib import contextmanager

import pytest

import fsspec


@contextmanager
def tempzip(data={}):
    f = tempfile.mkstemp(suffix="zip")[1]
    with zipfile.ZipFile(f, mode="w") as z:
        for k, v in data.items():
            z.writestr(k, v)
    try:
        yield f
    finally:
        try:
            os.remove(f)
        except (IOError, OSError):
            pass


data = {"a": b"", "b": b"hello", "deeply/nested/path": b"stuff"}


def test_empty():
    with tempzip() as z:
        fs = fsspec.filesystem("zip", fo=z)
        assert fs.find("") == []
        assert fs.find("", withdirs=True) == []
        with pytest.raises(FileNotFoundError):
            fs.info("")
        assert fs.ls("") == []


def test_glob():
    with tempzip(data) as z:
        fs = fsspec.filesystem("zip", fo=z)
        assert fs.glob("*/*/*th") == ["deeply/nested/path"]


@pytest.mark.xfail(sys.version_info < (3, 6), reason="zip-info odd on py35")
def test_mapping():
    with tempzip(data) as z:
        fs = fsspec.filesystem("zip", fo=z)
        m = fs.get_mapper("")
        assert list(m) == ["a", "b", "deeply/nested/path"]
        assert m["b"] == data["b"]


@pytest.mark.xfail(sys.version_info < (3, 6), reason="zip not supported on py35")
def test_pickle():
    with tempzip(data) as z:
        fs = fsspec.filesystem("zip", fo=z)
        fs2 = pickle.loads(pickle.dumps(fs))
        assert fs2.cat("b") == b"hello"


def test_all_dirnames():
    with tempzip() as z:
        fs = fsspec.filesystem("zip", fo=z)

        # fx are files, dx are a directories
        assert fs._all_dirnames([]) == set()
        assert fs._all_dirnames(["f1"]) == set()
        assert fs._all_dirnames(["f1", "f2"]) == set()
        assert fs._all_dirnames(["f1", "f2", "d1/f1"]) == {"d1"}
        assert fs._all_dirnames(["f1", "d1/f1", "d1/f2"]) == {"d1"}
        assert fs._all_dirnames(["f1", "d1/f1", "d2/f1"]) == {"d1", "d2"}
        assert fs._all_dirnames(["d1/d1/d1/f1"]) == {"d1", "d1/d1", "d1/d1/d1"}


def test_ls():
    with tempzip(data) as z:
        lhs = fsspec.filesystem("zip", fo=z)

        assert lhs.ls("") == ["a", "b", "deeply/"]
        assert lhs.ls("/") == lhs.ls("")

        assert lhs.ls("deeply") == ["deeply/nested/"]
        assert lhs.ls("deeply/") == lhs.ls("deeply")

        assert lhs.ls("deeply/nested") == ["deeply/nested/path"]
        assert lhs.ls("deeply/nested/") == lhs.ls("deeply/nested")


def test_find():
    with tempzip(data) as z:
        lhs = fsspec.filesystem("zip", fo=z)

        assert lhs.find("") == ["a", "b", "deeply/nested/path"]
        assert lhs.find("", withdirs=True) == [
            "a",
            "b",
            "deeply/",
            "deeply/nested/",
            "deeply/nested/path",
        ]

        assert lhs.find("deeply") == ["deeply/nested/path"]
        assert lhs.find("deeply/") == lhs.find("deeply")


def test_walk():
    with tempzip(data) as z:
        fs = fsspec.filesystem("zip", fo=z)
        expected = [
            # (dirname, list of subdirs, list of files)
            ("", ["deeply"], ["a", "b"]),
            ("deeply", ["nested"], []),
            ("deeply/nested", [], ["path"]),
        ]
        assert list(fs.walk("")) == expected


def test_info():
    with tempzip(data) as z:
        fs_cache = fsspec.filesystem("zip", fo=z)

        with pytest.raises(FileNotFoundError):
            fs_cache.info("i-do-not-exist")

        # Iterate over all directories
        # The ZipFile does not include additional information about the directories,
        for d in fs_cache._all_dirnames(data.keys()):
            lhs = fs_cache.info(d)
            expected = {"name": f"{d}/", "size": 0, "type": "directory"}
            assert lhs == expected

        # Iterate over all files
        for f, v in data.items():
            lhs = fs_cache.info(f)
            assert lhs["name"] == f
            assert lhs["size"] == len(v)
            assert lhs["type"] == "file"

            # There are many flags specific to Zip Files.
            # These are two we can use to check we are getting some of them
            assert "CRC" in lhs
            assert "compress_size" in lhs


@pytest.mark.parametrize("scale", [128, 512, 4096])
def test_isdir_isfile(scale):
    def make_nested_dir(i):
        x = f"{i}"
        table = x.maketrans("0123456789", "ABCDEFGHIJ")
        return "/".join(x.translate(table))

    scaled_data = {f"{make_nested_dir(i)}/{i}": b"" for i in range(1, scale + 1)}
    with tempzip(scaled_data) as z:
        fs = fsspec.filesystem("zip", fo=z)

        lhs_dirs, lhs_files = fs._all_dirnames(scaled_data.keys()), scaled_data.keys()

        # Warm-up the Cache, this is done in both cases anyways...
        fs._get_dirs()

        entries = lhs_files | lhs_dirs

        assert lhs_dirs == {e for e in entries if fs.isdir(e)}
        assert lhs_files == {e for e in entries if fs.isfile(e)}
