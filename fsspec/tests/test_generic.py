import pytest

import fsspec
from fsspec.tests.conftest import data, server  # noqa: F401


def test_remote_async_ops(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    out = fs.info(server.realfile)
    assert out["size"] == len(data)
    assert out["type"] == "file"
    assert fs.isfile(server.realfile)  # this method from superclass


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
    fs.cp([server.realfile], ["memory://realfile"])
    assert m.cat("realfile") == data

    fs.rm("memory://realfile")
    assert not m.exists("realfile")


def test_pipe_cat_sync(m):
    fs = fsspec.filesystem("generic", default_method="current")
    fs.pipe("memory://afile", b"data")
    assert fs.cat("memory://afile") == b"data"


def test_cat_async(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    assert fs.cat(server.realfile) == data


def test_cp_one(server, tmpdir):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    local = fsspec.filesystem("file")
    fn = f"file://{tmpdir}/afile"

    fs = fsspec.filesystem("generic", default_method="current")

    fs.copy([server.realfile], [fn])
    assert local.cat(fn) == data
    fs.rm(fn)
    assert not fs.exists(fn)

    fs.copy(server.realfile, fn)
    assert local.cat(fn) == data
    fs.rm(fn)
    assert not fs.exists(fn)

    fs.cp([server.realfile], [fn])
    assert local.cat(fn) == data
    fs.rm(fn)
    assert not fs.exists(fn)

    fs.cp_file(server.realfile, fn)
    assert local.cat(fn) == data
    fs.rm(fn)
    assert not fs.exists(fn)


def test_rsync(tmpdir, m):
    from fsspec.generic import GenericFileSystem, rsync

    fs = GenericFileSystem()
    fs.pipe("memory:///deep/path/afile", b"data1")
    fs.pipe("memory:///deep/afile", b"data2")

    with pytest.raises(ValueError):
        rsync("memory:///deep/afile", f"file://{tmpdir}")
    rsync("memory://", f"file://{tmpdir}")

    allfiles = fs.find(f"file://{tmpdir}", withdirs=True, detail=True)
    pos_tmpdir = fsspec.implementations.local.make_path_posix(str(tmpdir))  # for WIN
    assert set(allfiles) == {
        f"file://{pos_tmpdir}{_}"
        for _ in [
            "",
            "/deep",
            "/deep/path",
            "/deep/path/afile",
            "/deep/afile",
        ]
    }
    fs.rm("memory:///deep/afile")
    rsync("memory://", f"file://{tmpdir}", delete_missing=True)
    allfiles2 = fs.find(f"file://{tmpdir}", withdirs=True, detail=True)
    assert set(allfiles2) == {
        f"file://{pos_tmpdir}{_}"
        for _ in [
            "",
            "/deep",
            "/deep/path",
            "/deep/path/afile",
        ]
    }
    # the file was not updated, since size was correct
    assert (
        allfiles[f"file://{pos_tmpdir}/deep/path/afile"]
        == allfiles2[f"file://{pos_tmpdir}/deep/path/afile"]
    )
