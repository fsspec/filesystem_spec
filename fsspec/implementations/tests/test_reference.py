import json

import pytest

import fsspec
from fsspec.implementations.reference import _unmodel_hdf5

from .test_http import data, realfile, server  # noqa: F401


def test_simple(server):  # noqa: F811

    refs = {"a": b"data", "b": (realfile, 0, 5), "c": (realfile, 1, 5)}
    h = fsspec.filesystem("http")
    fs = fsspec.filesystem("reference", references=refs, fs=h)

    assert fs.cat("a") == b"data"
    assert fs.cat("b") == data[:5]
    assert fs.cat("c") == data[1 : 1 + 5]


def test_ls(server):  # noqa: F811
    refs = {"a": b"data", "b": (realfile, 0, 5), "c/d": (realfile, 1, 6)}
    h = fsspec.filesystem("http")
    fs = fsspec.filesystem("reference", references=refs, fs=h)

    assert fs.ls("", detail=False) == ["a", "b", "c"]
    assert {"name": "c", "type": "directory", "size": 0} in fs.ls("", detail=True)
    assert fs.find("") == ["a", "b", "c/d"]
    assert fs.find("", withdirs=True) == ["a", "b", "c", "c/d"]


def test_err(m):
    with pytest.raises(NotImplementedError):
        fsspec.filesystem("reference", references={}, fs=m)
    with pytest.raises(NotImplementedError):
        fsspec.filesystem("reference", references={}, target_protocol="memory")


def test_defaults(server):  # noqa: F811
    refs = {"a": b"data", "b": (None, 0, 5)}
    fs = fsspec.filesystem(
        "reference", references=refs, target_protocol="http", target=realfile
    )

    assert fs.cat("a") == b"data"
    assert fs.cat("b") == data[:5]


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
    assert b'"Conventions": "UGRID-0.9.0"' in refs[".zattrs"]
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
            }
        ],
        "refs": {
            "key0": "data",
            "key1": ["http://target_url", 10000, 100],
            "key2": ["http://{{u}}", 10000, 100],
            "key3": ["http://{{f(c='text')}}", 10000, 100],
        },
    }
    fs = fsspec.filesystem("reference", references=in_data, target_protocol="http")
    assert fs.references == {
        "key0": "data",
        "key1": ["http://target_url", 10000, 100],
        "key2": ["http://server.domain/path", 10000, 100],
        "key3": ["http://text", 10000, 100],
        "gen_key0": ["http://server.domain/path_0", 1000, 1000],
        "gen_key1": ["http://server.domain/path_1", 2000, 1000],
        "gen_key2": ["http://server.domain/path_2", 3000, 1000],
        "gen_key3": ["http://server.domain/path_3", 4000, 1000],
        "gen_key4": ["http://server.domain/path_4", 5000, 1000],
    }
