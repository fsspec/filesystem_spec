import fsspec
from fsspec.tests.conftest import data, server  # noqa: F401


def test_remote_async_ops(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    out = fs.info(server + "/index/realfile")
    assert out["size"] == len(data)
    assert out["type"] == "file"
    assert fs.isfile(server + "/index/realfile")  # this method from superclass


def test_touch_rm(m):
    m.touch("afile")
    m.touch("dir/afile")

    fs = fsspec.filesystem("generic", default_method="current")
    fs.rm("memory://afile")
    assert not m.exists("afile")

    fs.rm("memory://dir", recursive=True)
    assert not m.exists("dir/afile")
    assert not m.exists("dir")


def test_cp_async_to_sync(server, m):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    fs.cp(server + "/index/realfile", "memory://realfile")
    assert m.cat("realfile") == data

    fs.rm("memory://realfile")
    assert not m.exists("realfile")


def test_pipe_cat_sync(m):
    fs = fsspec.filesystem("generic", default_method="current")
    fs.pipe("memory://afile", b"data")
    assert fs.cat("memory://afile") == b"data"
