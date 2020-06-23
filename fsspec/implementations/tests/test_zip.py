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


@pytest.mark.parametrize("implementation", ["super", "cache"])
def test_info(implementation):
    with tempzip(data) as z:
        fs = fsspec.get_filesystem_class("zip")(fo=z)

        with pytest.raises(FileNotFoundError):
            fs.info("i-do-not-exist")

        def all_dirnames(keys):
            dirnames = {os.path.dirname(k) for k in keys}
            if len(dirnames) == 1 and "" in dirnames:
                return dirnames
            return dirnames | all_dirnames(dirnames)

        # Iterate over all directories
        for d in all_dirnames(data.keys()):
            d_info = fs.info(d, _info_implementation=implementation)
            assert (d_info["type"], d_info["size"], d_info["name"]) == ("directory", 0, (f"{d}/" if d != fs.root_marker else fs.root_marker))

        # Iterate over all files
        for f, v in data.items():
            f_info = fs.info(f, _info_implementation=implementation)
            assert (f_info["type"], f_info["size"], f_info["name"]) == ("file", len(v), f)
