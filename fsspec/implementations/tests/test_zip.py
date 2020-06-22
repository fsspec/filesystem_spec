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


def test_info():
    with tempzip(data) as z:
        fs = fsspec.get_filesystem_class("zip")(fo=z)

        with pytest.raises(FileNotFoundError):
            fs.info("i-do-not-exist")

        # Iterate over all directories
        for d in {os.path.dirname(f) for f in data.keys()}:
            d_info = fs.info(d)
            assert d_info["type"] == "directory"
            assert d_info["size"] == 0
            assert d_info["name"] == (f"{d}/" if d != fs.root_marker else fs.root_marker)

        # Iterate over all files
        for f, v in data.items():
            f_info = fs.info(f)
            assert f_info["type"] == "file"
            assert f_info["size"] == len(v)
            assert f_info["name"] == f
