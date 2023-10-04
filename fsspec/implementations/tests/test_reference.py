import json
import os

import pytest

import fsspec
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.reference import ReferenceFileSystem, ReferenceNotReachable
from fsspec.tests.conftest import data, realfile, reset_files, server, win  # noqa: F401


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
    with fs.open("d", "rt") as f:
        assert f.read(2) == "he"


def test_target_options(m):
    m.pipe("data/0", b"hello")
    refs = {"a": ["memory://data/0"]}
    fn = "memory://refs.json.gz"
    with fsspec.open(fn, "wt", compression="gzip") as f:
        json.dump(refs, f)

    fs = fsspec.filesystem("reference", fo=fn, target_options={"compression": "gzip"})
    assert fs.cat("a") == b"hello"


def test_ls(server):  # noqa: F811
    refs = {"a": b"data", "b": (realfile, 0, 5), "c/d": (realfile, 1, 6)}
    h = fsspec.filesystem("http")
    fs = fsspec.filesystem("reference", fo=refs, fs=h)

    assert fs.ls("", detail=False) == ["a", "b", "c"]
    assert {"name": "c", "type": "directory", "size": 0} in fs.ls("", detail=True)
    assert fs.find("") == ["a", "b", "c/d"]
    assert fs.find("", withdirs=True) == ["a", "b", "c", "c/d"]
    assert fs.find("c", detail=True) == {
        "c/d": {"name": "c/d", "size": 6, "type": "file"}
    }


def test_info(server):  # noqa: F811
    refs = {
        "a": b"data",
        "b": (realfile, 0, 5),
        "c/d": (realfile, 1, 6),
        "e": (realfile,),
    }
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("reference", fo=refs, fs=h)
    assert fs.size("a") == 4
    assert fs.size("b") == 5
    assert fs.size("c/d") == 6
    assert fs.info("e")["size"] == len(data)


def test_mutable(server, m):
    refs = {
        "a": b"data",
        "b": (realfile, 0, 5),
        "c/d": (realfile, 1, 6),
        "e": (realfile,),
    }
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("reference", fo=refs, fs=h)
    fs.rm("a")
    assert not fs.exists("a")

    bin_data = b"bin data"
    fs.pipe("aa", bin_data)
    assert fs.cat("aa") == bin_data

    fs.save_json("memory://refs.json")
    assert m.exists("refs.json")

    fs = fsspec.filesystem("reference", fo="memory://refs.json", remote_protocol="http")
    assert not fs.exists("a")
    assert fs.cat("aa") == bin_data


def test_put_get(tmpdir):
    d1 = f"{tmpdir}/d1"
    os.mkdir(d1)
    with open(f"{d1}/a", "wb") as f:
        f.write(b"1")
    with open(f"{d1}/b", "wb") as f:
        f.write(b"2")
    d2 = f"{tmpdir}/d2"

    fs = fsspec.filesystem("reference", fo={}, remote_protocol="file")
    fs.put(d1, "out", recursive=True)

    fs.get("out", d2, recursive=True)
    assert open(f"{d2}/a", "rb").read() == b"1"
    assert open(f"{d2}/b", "rb").read() == b"2"


def test_put_get_single(tmpdir):
    d1 = f"{tmpdir}/f1"
    d2 = f"{tmpdir}/f2"
    with open(d1, "wb") as f:
        f.write(b"1")

    # skip instance cache since this is the same kwargs as previous test
    fs = fsspec.filesystem(
        "reference", fo={}, remote_protocol="file", skip_instance_cache=True
    )
    fs.put_file(d1, "out")

    fs.get_file("out", d2)
    assert open(d2, "rb").read() == b"1"
    fs.pipe({"hi": b"data"})
    assert fs.cat("hi") == b"data"


def test_defaults(server):  # noqa: F811
    refs = {"a": b"data", "b": (None, 0, 5)}
    fs = fsspec.filesystem(
        "reference",
        fo=refs,
        target_protocol="http",
        target=realfile,
        remote_protocol="http",
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


def test_spec1_expand_simple():
    pytest.importorskip("jinja2")
    in_data = {
        "version": 1,
        "templates": {"u": "server.domain/path"},
        "refs": {
            "key0": "base64:ZGF0YQ==",
            "key2": ["http://{{u}}", 10000, 100],
            "key4": ["http://target_url"],
        },
    }
    fs = fsspec.filesystem("reference", fo=in_data, target_protocol="http")
    assert fs.references["key2"] == ["http://server.domain/path", 10000, 100]
    fs = fsspec.filesystem(
        "reference",
        fo=in_data,
        target_protocol="http",
        template_overrides={"u": "not.org/p"},
    )
    assert fs.references["key2"] == ["http://not.org/p", 10000, 100]
    assert fs.cat("key0") == b"data"


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
        "b": (f"file://{real}", 0, 5),
        "c/d": (f"file://{real}", 1, 6),
        "c/e": ["memory://afile"],
    }

    fs = fsspec.filesystem("reference", fo=refs, fs={"file": localfs, "memory": m})
    assert fs.cat("c/e") == b"hello"
    assert fs.cat(["c/e", "a", "b"]) == {
        "a": b"data",
        "b": b"01234",
        "c/e": b"hello",
    }


def test_multi_fs_created(m, tmpdir):
    real = tmpdir / "file"
    real.write_binary(b"0123456789")

    m.pipe("afile", b"hello")

    # local URLs are file:// by default
    refs = {
        "a": b"data",
        "b": (f"file://{real}", 0, 5),
        "c/d": (f"file://{real}", 1, 6),
        "c/e": ["memory://afile"],
    }

    fs = fsspec.filesystem("reference", fo=refs, fs={"file": {}, "memory": {}})
    assert fs.cat("c/e") == b"hello"
    assert fs.cat(["c/e", "a", "b"]) == {
        "a": b"data",
        "b": b"01234",
        "c/e": b"hello",
    }


def test_missing_nonasync(m):
    zarr = pytest.importorskip("zarr")
    zarray = {
        "chunks": [1],
        "compressor": None,
        "dtype": "<f8",
        "fill_value": "NaN",
        "filters": [],
        "order": "C",
        "shape": [10],
        "zarr_format": 2,
    }
    refs = {".zarray": json.dumps(zarray)}

    m = fsspec.get_mapper("reference://", fo=refs, remote_protocol="memory")

    a = zarr.open_array(m)
    assert str(a[0]) == "nan"


def test_fss_has_defaults(m):
    fs = fsspec.filesystem("reference", fo={})
    assert None in fs.fss

    fs = fsspec.filesystem("reference", fo={}, remote_protocol="memory")
    assert fs.fss[None].protocol == "memory"
    assert fs.fss["memory"].protocol == "memory"

    fs = fsspec.filesystem("reference", fs=m, fo={})
    assert fs.fss[None] is m

    fs = fsspec.filesystem("reference", fs={"memory": m}, fo={})
    assert fs.fss["memory"] is m
    assert fs.fss[None].protocol == ("file", "local")

    fs = fsspec.filesystem("reference", fs={None: m}, fo={})
    assert fs.fss[None] is m

    fs = fsspec.filesystem("reference", fo={"key": ["memory://a"]})
    assert fs.fss[None] is fs.fss["memory"]

    fs = fsspec.filesystem("reference", fo={"key": ["memory://a"], "blah": ["path"]})
    assert fs.fss[None] is fs.fss["memory"]


def test_merging(m):
    m.pipe("/a", b"test data")
    other = b"other test data"
    m.pipe("/b", other)
    fs = fsspec.filesystem(
        "reference",
        fo={
            "a": ["memory://a", 1, 1],
            "b": ["memory://a", 2, 1],
            "c": ["memory://b"],
            "d": ["memory://b", 4, 6],
        },
    )
    out = fs.cat(["a", "b", "c", "d"])
    assert out == {"a": b"e", "b": b"s", "c": other, "d": other[4:10]}


def test_cat_file_ranges(m):
    other = b"other test data"
    m.pipe("/b", other)
    fs = fsspec.filesystem(
        "reference",
        fo={
            "c": ["memory://b"],
            "d": ["memory://b", 4, 6],
        },
    )
    assert fs.cat_file("c") == other
    assert fs.cat_file("c", start=1) == other[1:]
    assert fs.cat_file("c", start=-5) == other[-5:]
    assert fs.cat_file("c", 1, -5) == other[1:-5]

    assert fs.cat_file("d") == other[4:10]
    assert fs.cat_file("d", start=1) == other[4:10][1:]
    assert fs.cat_file("d", start=-5) == other[4:10][-5:]
    assert fs.cat_file("d", 1, -3) == other[4:10][1:-3]


def test_cat_missing(m):
    other = b"other test data"
    m.pipe("/b", other)
    fs = fsspec.filesystem(
        "reference",
        fo={
            "c": ["memory://b"],
            "d": ["memory://unknown", 4, 6],
        },
    )
    with pytest.raises(FileNotFoundError):
        fs.cat("notafile")

    with pytest.raises(FileNotFoundError):
        fs.cat(["notone", "nottwo"])

    mapper = fs.get_mapper("")

    with pytest.raises(KeyError):
        mapper["notakey"]

    with pytest.raises(KeyError):
        mapper.getitems(["notone", "nottwo"])

    with pytest.raises(ReferenceNotReachable) as ex:
        fs.cat("d")
    assert ex.value.__cause__
    out = fs.cat("d", on_error="return")
    assert isinstance(out, ReferenceNotReachable)

    with pytest.raises(ReferenceNotReachable) as e:
        mapper["d"]
    assert '"d"' in str(e.value)
    assert "//unknown" in str(e.value)

    with pytest.raises(ReferenceNotReachable):
        mapper.getitems(["c", "d"])

    out = mapper.getitems(["c", "d"], on_error="return")
    assert isinstance(out["d"], ReferenceNotReachable)

    out = mapper.getitems(["c", "d"], on_error="omit")
    assert list(out) == ["c"]


def test_df_single(m):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    data = b"data0data1data2"
    m.pipe({"data": data})
    df = pd.DataFrame(
        {
            "path": [None, "memory://data", "memory://data"],
            "offset": [0, 0, 4],
            "size": [0, 0, 4],
            "raw": [b"raw", None, None],
        }
    )
    df.to_parquet("memory://stuff/refs.0.parq")
    m.pipe(
        ".zmetadata",
        b"""{
    "metadata": {
        ".zgroup": {
            "zarr_format": 2
        },
        "stuff/.zarray": {
            "chunks": [1],
            "compressor": null,
            "dtype": "i8",
            "filters": null,
            "shape": [3],
            "zarr_format": 2
        }
    },
    "zarr_consolidated_format": 1,
    "record_size": 10
    }
    """,
    )
    fs = ReferenceFileSystem(fo="memory:///", remote_protocol="memory")
    allfiles = fs.find("")
    assert ".zmetadata" in allfiles
    assert ".zgroup" in allfiles
    assert "stuff/2" in allfiles

    assert fs.cat("stuff/0") == b"raw"
    assert fs.cat("stuff/1") == data
    assert fs.cat("stuff/2") == data[4:8]


def test_df_multi(m):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    data = b"data0data1data2"
    m.pipe({"data": data})
    df0 = pd.DataFrame(
        {
            "path": [None, "memory://data", "memory://data"],
            "offset": [0, 0, 4],
            "size": [0, 0, 4],
            "raw": [b"raw1", None, None],
        }
    )
    df0.to_parquet("memory://stuff/refs.0.parq")
    df1 = pd.DataFrame(
        {
            "path": [None, "memory://data", "memory://data"],
            "offset": [0, 0, 2],
            "size": [0, 0, 2],
            "raw": [b"raw2", None, None],
        }
    )
    df1.to_parquet("memory://stuff/refs.1.parq")
    m.pipe(
        ".zmetadata",
        b"""{
    "metadata": {
        ".zgroup": {
            "zarr_format": 2
        },
        "stuff/.zarray": {
            "chunks": [1],
            "compressor": null,
            "dtype": "i8",
            "filters": null,
            "shape": [6],
            "zarr_format": 2
        }
    },
    "zarr_consolidated_format": 1,
    "record_size": 3
    }
    """,
    )
    fs = ReferenceFileSystem(
        fo="memory:///", remote_protocol="memory", skip_instance_cache=True
    )
    allfiles = fs.find("")
    assert ".zmetadata" in allfiles
    assert ".zgroup" in allfiles
    assert "stuff/2" in allfiles
    assert "stuff/4" in allfiles

    assert fs.cat("stuff/0") == b"raw1"
    assert fs.cat("stuff/1") == data
    assert fs.cat("stuff/2") == data[4:8]
    assert fs.cat("stuff/3") == b"raw2"
    assert fs.cat("stuff/4") == data
    assert fs.cat("stuff/5") == data[2:4]


def test_mapping_getitems(m):
    m.pipe({"a": b"A", "b": b"B"})

    refs = {
        "a": ["a"],
        "b": ["b"],
    }
    h = fsspec.filesystem("memory")
    fs = fsspec.filesystem("reference", fo=refs, fs=h)
    mapping = fs.get_mapper("")
    assert mapping.getitems(["b", "a"]) == {"a": b"A", "b": b"B"}
