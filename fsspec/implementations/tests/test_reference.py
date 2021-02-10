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
