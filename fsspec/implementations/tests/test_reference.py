import fsspec
from .test_http import data, realfile, server  # noqa: F401


def test_simple(server):  # noqa: F811

    refs = {"a": b"data", "b": (realfile, 0, 5), "c": (realfile, 1, 6)}
    h = fsspec.filesystem("http")
    fs = fsspec.filesystem("reference", references=refs, fs=h)

    assert fs.cat("a") == b"data"
    assert fs.cat("b") == data[:5]
    assert fs.cat("c") == data[1:6]
