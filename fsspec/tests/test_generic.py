import pytest

import fsspec
from fsspec.generic import rsync
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.registry import _registry, register_implementation
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


class _OptionsRequiredFS(MemoryFileSystem):
    """Backend that raises unless its storage_options reached the constructor."""

    protocol = "optsrequired"

    def __init__(self, *args, token=None, **kwargs):
        if token is None:
            raise ValueError("storage_options were not propagated")
        super().__init__(*args, **kwargs)

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, str):
            path = path.removeprefix(f"{cls.protocol}://")
        return MemoryFileSystem._strip_protocol(path)


@pytest.fixture()
def opts_fs(m):
    register_implementation(
        _OptionsRequiredFS.protocol, _OptionsRequiredFS, clobber=True
    )
    m.pipe_file("/afile", b"hello")
    m.makedirs("/adir", exist_ok=True)
    m.pipe_file("/adir/nested", b"world")
    yield fsspec.filesystem(
        "generic",
        default_method="options",
        storage_options={_OptionsRequiredFS.protocol: {"token": "set"}},
    )
    _registry.pop(_OptionsRequiredFS.protocol, None)
    _OptionsRequiredFS.clear_instance_cache()


def test_storage_options_propagated_to_backend(opts_fs):
    opts_fs.info("optsrequired:///afile")
    opts_fs.ls("optsrequired:///adir")
    opts_fs.cat_file("optsrequired:///afile")
    opts_fs.isdir("optsrequired:///adir")
    opts_fs.find("optsrequired:///adir")


def test_storage_options_propagated_cross_protocol(opts_fs, tmpdir):
    rsync(
        "optsrequired:///adir",
        f"file://{tmpdir}",
        inst_kwargs={
            "default_method": "options",
            "storage_options": {_OptionsRequiredFS.protocol: {"token": "set"}},
        },
    )
    assert (tmpdir / "nested").read_binary() == b"world"
