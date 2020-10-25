from contextlib import contextmanager

import os
import pickle
import pytest
import tempfile
import fsspec

libarchive = pytest.importorskip("libarchive")


@contextmanager
def temparchive(data={}):
    f = tempfile.mkstemp(suffix="7z")[1]
    with libarchive.file_writer(f, "7zip") as archive:
        for k, v in data.items():
            archive.add_file_from_memory(entry_path=k, entry_size=len(v), entry_data=v)
    try:
        yield f
    finally:
        try:
            os.remove(f)
        except (IOError, OSError):
            pass


data = {"a": b"", "b": b"hello", "deeply/nested/path": b"stuff"}


def test_empty():
    with temparchive() as archive_file:
        fs = fsspec.filesystem("libarchive", fo=archive_file)
        assert fs.find("") == []
        assert fs.find("", withdirs=True) == []
        with pytest.raises(FileNotFoundError):
            fs.info("")
        assert fs.ls("") == []


def test_mapping():
    with temparchive(data) as archive_file:
        fs = fsspec.filesystem("libarchive", fo=archive_file)
        m = fs.get_mapper("")

        fs._get_dirs()
        print(fs.dir_cache)

        assert list(m) == ["a", "b", "deeply/nested/path"]
        assert m["b"] == data["b"]


def test_pickle():
    with temparchive(data) as archive_file:
        fs = fsspec.filesystem("libarchive", fo=archive_file)
        fs2 = pickle.loads(pickle.dumps(fs))
        assert fs2.cat("b") == b"hello"


def test_all_dirnames():
    with temparchive() as archive_file:
        fs = fsspec.filesystem("libarchive", fo=archive_file)

        # fx are files, dx are a directories
        assert fs._all_dirnames([]) == set()
        assert fs._all_dirnames(["f1"]) == set()
        assert fs._all_dirnames(["f1", "f2"]) == set()
        assert fs._all_dirnames(["f1", "f2", "d1/f1"]) == {"d1"}
        assert fs._all_dirnames(["f1", "d1/f1", "d1/f2"]) == {"d1"}
        assert fs._all_dirnames(["f1", "d1/f1", "d2/f1"]) == {"d1", "d2"}
        assert fs._all_dirnames(["d1/d1/d1/f1"]) == {"d1", "d1/d1", "d1/d1/d1"}


def test_ls():
    with temparchive(data) as archive_file:
        lhs = fsspec.filesystem("libarchive", fo=archive_file)

        assert lhs.ls("") == ["a", "b", "deeply/"]
        assert lhs.ls("/") == lhs.ls("")

        assert lhs.ls("deeply") == ["deeply/nested/"]
        assert lhs.ls("deeply/") == lhs.ls("deeply")

        assert lhs.ls("deeply/nested") == ["deeply/nested/path"]
        assert lhs.ls("deeply/nested/") == lhs.ls("deeply/nested")


def test_find():
    with temparchive(data) as archive_file:
        lhs = fsspec.filesystem("libarchive", fo=archive_file)

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
    with temparchive(data) as archive_file:
        fs = fsspec.filesystem("libarchive", fo=archive_file)
        expected = [
            # (dirname, list of subdirs, list of files)
            ("", ["deeply"], ["a", "b"]),
            ("deeply", ["nested"], []),
            ("deeply/nested", [], ["path"]),
        ]
        for lhs, rhs in zip(fs.walk(""), expected):
            assert lhs[0] == rhs[0]
            assert sorted(lhs[1]) == sorted(rhs[1])
            assert sorted(lhs[2]) == sorted(rhs[2])


def test_info():
    with temparchive(data) as archive_file:
        fs_cache = fsspec.filesystem("libarchive", fo=archive_file)

        with pytest.raises(FileNotFoundError):
            fs_cache.info("i-do-not-exist")

        # Iterate over all directories
        # The 7zip archive does not include additional information about the
        # directories
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

            # These are the specific flags retrieved from the archived files
            assert "created" in lhs
            assert "mode" in lhs
            assert "uid" in lhs
            assert "gid" in lhs
            assert "mtime" in lhs


@pytest.mark.parametrize("scale", [128, 512, 4096])
def test_isdir_isfile(scale):
    def make_nested_dir(i):
        x = f"{i}"
        table = x.maketrans("0123456789", "ABCDEFGHIJ")
        return "/".join(x.translate(table))

    scaled_data = {f"{make_nested_dir(i)}/{i}": b"" for i in range(1, scale + 1)}
    with temparchive(scaled_data) as archive_file:
        fs = fsspec.filesystem("libarchive", fo=archive_file)

        lhs_dirs, lhs_files = fs._all_dirnames(scaled_data.keys()), scaled_data.keys()

        # Warm-up the Cache, this is done in both cases anyways...
        fs._get_dirs()

        entries = lhs_files | lhs_dirs

        assert lhs_dirs == {e for e in entries if fs.isdir(e)}
        assert lhs_files == {e for e in entries if fs.isfile(e)}
