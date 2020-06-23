import zipfile
from contextlib import contextmanager
import os
import pickle
import pytest
import sys
import tempfile
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
        fs = fsspec.get_filesystem_class("zip")(fo=z)
        assert fs.find("") == []
        with pytest.raises(FileNotFoundError):
            fs.info("")


@pytest.mark.xfail(sys.version_info < (3, 6), reason="zip-info odd on py35")
def test_mapping():
    with tempzip(data) as z:
        fs = fsspec.get_filesystem_class("zip")(fo=z)
        m = fs.get_mapper("")
        assert list(m) == ["a", "b", "deeply/nested/path"]
        assert m["b"] == data["b"]


@pytest.mark.xfail(sys.version_info < (3, 6), reason="zip not supported on py35")
def test_pickle():
    with tempzip(data) as z:
        fs = fsspec.get_filesystem_class("zip")(fo=z)
        fs2 = pickle.loads(pickle.dumps(fs))
        assert fs2.cat("b") == b"hello"


def test_all_dirnames():
    with tempzip() as z:
        fs = fsspec.get_filesystem_class("zip")(fo=z)

        # fx are files, dx are a directories
        assert fs._all_dirnames([]) == set()
        assert fs._all_dirnames(["f1"]) == set()
        assert fs._all_dirnames(["f1", "f2"]) == set()
        assert fs._all_dirnames(["f1", "f2", "d1/f1"]) == {"d1"}
        assert fs._all_dirnames(["f1", "d1/f1", "d1/f2"]) == {"d1"}
        assert fs._all_dirnames(["f1", "d1/f1", "d2/f1"]) == {"d1", "d2"}
        assert fs._all_dirnames(["d1/d1/d1/f1"]) == {"d1", "d1/d1", "d1/d1/d1"}


def test_ls(monkeypatch):
    with tempzip(data) as z:
        lhs = fsspec.get_filesystem_class("zip")(fo=z)

        assert lhs.ls("") == ["a", "b", "deeply/"]
        assert lhs.ls("/") == lhs.ls("")

        assert lhs.ls("deeply") == ["deeply/nested/"]
        assert lhs.ls("deeply/") == lhs.ls("deeply")

        assert lhs.ls("deeply/nested") == ["deeply/nested/path"]
        assert lhs.ls("deeply/nested/") == lhs.ls("deeply/nested")


def test_walk():
    with tempzip(data) as z:
        fs_base = fsspec.get_filesystem_class("zip")(fo=z, _info_implementation="base")
        fs_cache = fsspec.get_filesystem_class("zip")(fo=z)

        lhs = list(fs_base.walk(""))
        rhs = list(fs_cache.walk(""))
        assert lhs == rhs


def test_info():
    with tempzip(data) as z:
        fs_base = fsspec.get_filesystem_class("zip")(fo=z, _info_implementation="base")
        fs_cache = fsspec.get_filesystem_class("zip")(fo=z)

        with pytest.raises(FileNotFoundError):
            fs_base.info("i-do-not-exist")
        with pytest.raises(FileNotFoundError):
            fs_cache.info("i-do-not-exist")

        # Iterate over all directories
        for d in fs_cache._all_dirnames(data.keys()):
            lhs = fs_base.info(d)
            rhs = fs_cache.info(d)
            assert lhs == rhs

        # Iterate over all files
        for f, v in data.items():
            lhs = fs_base.info(f)
            rhs = fs_cache.info(f)
            assert lhs == rhs


@pytest.mark.parametrize("implementation", ["base", "cache"])
@pytest.mark.parametrize("scale", [128, 256, 512, 1024, 2048, 4096])
def test_isdir_isfile(benchmark, implementation, scale):
    if implementation == "base" and scale > 1_000:
        pytest.skip("test takes too long...")

    def make_nested_dir(i):
        x = f"{i}"
        table = x.maketrans("0123456789", "ABCDEFGHIJ")
        return os.path.join(*x.translate(table))

    scaled_data = {f"{make_nested_dir(i)}/{i}": b"" for i in range(1,scale+1)}
    with tempzip(scaled_data) as z:
        fs = fsspec.get_filesystem_class("zip")(fo=z, _info_implementation=implementation)

        lhs_dirs, lhs_files = fs._all_dirnames(scaled_data.keys()), scaled_data.keys()

        # Warm-up the Cache, this is done in both cases anyways...
        fs._get_dirs()

        entries = lhs_files | lhs_dirs
        @benchmark
        def split_into_dirs_files():
            rhs_dirs = {e for e in entries if fs.isdir(e)}
            rhs_files = {e for e in entries if fs.isfile(e)}
            return rhs_dirs, rhs_files

        rhs = split_into_dirs_files

        assert lhs_dirs == rhs[0]
        assert lhs_files == rhs[1]
