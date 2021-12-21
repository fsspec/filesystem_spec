import json

import pytest

import fsspec
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.reference import _unmodel_hdf5

from .test_http import data, realfile, server  # noqa: F401


def test_simple(server):  # noqa: F811

    refs = {
        "a": b"data",
        "b": (realfile, 0, 5),
        "c": (realfile, 1, 5),
        "d": b"base64:aGVsbG8=",
    }
    h = fsspec.filesystem("http")
    fs = fsspec.filesystem("reference", fo=refs, fs=h)

    assert fs.cat("a") == b"data"
    assert fs.cat("b") == data[:5]
    assert fs.cat("c") == data[1 : 1 + 5]
    assert fs.cat("d") == b"hello"


def test_ls(server):  # noqa: F811
    refs = {"a": b"data", "b": (realfile, 0, 5), "c/d": (realfile, 1, 6)}
    h = fsspec.filesystem("http")
    fs = fsspec.filesystem("reference", fo=refs, fs=h)

    assert fs.ls("", detail=False) == ["a", "b", "c"]
    assert {"name": "c", "type": "directory", "size": 0} in fs.ls("", detail=True)
    assert fs.find("") == ["a", "b", "c/d"]
    assert fs.find("", withdirs=True) == ["a", "b", "c", "c/d"]


def test_defaults(server):  # noqa: F811
    refs = {"a": b"data", "b": (None, 0, 5)}
    fs = fsspec.filesystem(
        "reference", fo=refs, target_protocol="http", target=realfile
    )

    assert fs.cat("a") == b"data"
    assert fs.cat("b") == data[:5]


def test_inputs():  # noqa: F811
    import io

    refs = io.StringIO("""{"a": "data", "b": [null, 0, 5]}""")
    fs = fsspec.filesystem(
        "reference", fo=refs, target_protocol="http", target=realfile
    )
    assert fs.cat("a") == b"data"

    refs = io.BytesIO(b"""{"a": "data", "b": [null, 0, 5]}""")
    fs = fsspec.filesystem(
        "reference", fo=refs, target_protocol="http", target=realfile
    )
    assert fs.cat("a") == b"data"


jdata = """{
    "metadata": {
        ".zattrs": {
            "Conventions": "UGRID-0.9.0"
        },
        ".zgroup": {
            "zarr_format": 2
        },
        "adcirc_mesh/.zarray": {
            "chunks": [
                1
            ],
            "dtype": "<i4",
            "shape": [
                1
            ],
            "zarr_format": 2
        },
        "adcirc_mesh/.zattrs": {
            "_ARRAY_DIMENSIONS": [
                "mesh"
            ],
            "cf_role": "mesh_topology"
        },
        "adcirc_mesh/.zchunkstore": {
            "adcirc_mesh/0": {
                "offset": 8928,
                "size": 4
            },
            "source": {
                "array_name": "/adcirc_mesh",
                "uri": "https://url"
            }
        }
    },
    "zarr_consolidated_format": 1
}
"""


def test_unmodel():
    refs = _unmodel_hdf5(json.loads(jdata))
    # apparently the output may or may not contain a space after ":"
    assert b'"Conventions":"UGRID-0.9.0"' in refs[".zattrs"].replace(b" ", b"")
    assert refs["adcirc_mesh/0"] == ("https://url", 8928, 8932)


def test_spec1_expand():
    pytest.importorskip("jinja2")
    in_data = {
        "version": 1,
        "templates": {"u": "server.domain/path", "f": "{{c}}"},
        "gen": [
            {
                "key": "gen_key{{i}}",
                "url": "http://{{u}}_{{i}}",
                "offset": "{{(i + 1) * 1000}}",
                "length": "1000",
                "dimensions": {"i": {"stop": 5}},
            },
            {
                "key": "gen_key{{i}}",
                "url": "http://{{u}}_{{i}}",
                "dimensions": {"i": {"start": 5, "stop": 7}},
            },
        ],
        "refs": {
            "key0": "data",
            "key1": ["http://target_url", 10000, 100],
            "key2": ["http://{{u}}", 10000, 100],
            "key3": ["http://{{f(c='text')}}", 10000, 100],
            "key4": ["http://target_url"],
        },
    }
    fs = fsspec.filesystem(
        "reference", fo=in_data, target_protocol="http", simple_templates=False
    )
    assert fs.references == {
        "key0": "data",
        "key1": ["http://target_url", 10000, 100],
        "key2": ["http://server.domain/path", 10000, 100],
        "key3": ["http://text", 10000, 100],
        "key4": ["http://target_url"],
        "gen_key0": ["http://server.domain/path_0", 1000, 1000],
        "gen_key1": ["http://server.domain/path_1", 2000, 1000],
        "gen_key2": ["http://server.domain/path_2", 3000, 1000],
        "gen_key3": ["http://server.domain/path_3", 4000, 1000],
        "gen_key4": ["http://server.domain/path_4", 5000, 1000],
        "gen_key5": ["http://server.domain/path_5"],
        "gen_key6": ["http://server.domain/path_6"],
    }


def test_spec1_gen_variants():
    pytest.importorskip("jinja2")
    with pytest.raises(ValueError):
        missing_length_spec = {
            "version": 1,
            "templates": {"u": "server.domain/path"},
            "gen": [
                {
                    "key": "gen_key{{i}}",
                    "url": "http://{{u}}_{{i}}",
                    "offset": "{{(i + 1) * 1000}}",
                    "dimensions": {"i": {"stop": 2}},
                },
            ],
        }
        fsspec.filesystem("reference", fo=missing_length_spec, target_protocol="http")

    with pytest.raises(ValueError):
        missing_offset_spec = {
            "version": 1,
            "templates": {"u": "server.domain/path"},
            "gen": [
                {
                    "key": "gen_key{{i}}",
                    "url": "http://{{u}}_{{i}}",
                    "length": "1000",
                    "dimensions": {"i": {"stop": 2}},
                },
            ],
        }
        fsspec.filesystem("reference", fo=missing_offset_spec, target_protocol="http")

    url_only_gen_spec = {
        "version": 1,
        "templates": {"u": "server.domain/path"},
        "gen": [
            {
                "key": "gen_key{{i}}",
                "url": "http://{{u}}_{{i}}",
                "dimensions": {"i": {"stop": 2}},
            },
        ],
    }

    fs = fsspec.filesystem("reference", fo=url_only_gen_spec, target_protocol="http")
    assert fs.references == {
        "gen_key0": ["http://server.domain/path_0"],
        "gen_key1": ["http://server.domain/path_1"],
    }


def test_empty():
    pytest.importorskip("jinja2")
    fs = fsspec.filesystem("reference", fo={"version": 1}, target_protocol="http")
    assert fs.references == {}


def test_get_sync(tmpdir):
    localfs = LocalFileSystem()

    real = tmpdir / "file"
    real.write_binary(b"0123456789")

    refs = {"a": b"data", "b": (str(real), 0, 5), "c/d": (str(real), 1, 6)}
    fs = fsspec.filesystem("reference", fo=refs, fs=localfs)

    fs.get("a", str(tmpdir / "a"))
    assert (tmpdir / "a").read_binary() == b"data"
    fs.get("b", str(tmpdir / "b"))
    assert (tmpdir / "b").read_binary() == b"01234"
    fs.get("c/d", str(tmpdir / "d"))
    assert (tmpdir / "d").read_binary() == b"123456"
    fs.get("c", str(tmpdir / "c"), recursive=True)
    assert (tmpdir / "c").isdir()
    assert (tmpdir / "c" / "d").read_binary() == b"123456"


def test_multi_fs_provided(m, tmpdir):
    localfs = LocalFileSystem()

    real = tmpdir / "file"
    real.write_binary(b"0123456789")

    m.pipe("afile", b"hello")

    # local URLs are file:// by default
    refs = {
        "a": b"data",
        "b": ("file://" + str(real), 0, 5),
        "c/d": ("file://" + str(real), 1, 6),
        "c/e": ["memory://afile"],
    }

    fs = fsspec.filesystem("reference", fo=refs, fs={"file": localfs, "memory": m})
    assert fs.cat("c/e") == b"hello"
    assert fs.cat(["c/e", "a", "b"]) == {
        "a": b"data",
        "b": b"01234",
        "c/e": b"hello",
    }
