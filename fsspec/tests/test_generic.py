import fsspec
from fsspec.tests.conftest import data, server  # noqa: F401


def test_remote_async_ops(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    out = fs.info(server + "/index/realfile")
    assert out["size"] == len(data)
    assert out["type"] == "file"
    assert fs.isfile(server + "/index/realfile")  # this method from superclass


def test_cp_async_to_sync(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    m = fsspec.filesystem("memory")
    fs = fsspec.filesystem("generic", default_method="current")
    fs.cp(server + "/index/realfile", "memory://realfile")
    assert m.cat("realfile") == data
