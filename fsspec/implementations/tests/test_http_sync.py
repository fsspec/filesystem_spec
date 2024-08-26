import io
import json
import os
import time

import pytest

import fsspec.utils
from fsspec.tests.conftest import data, reset_files, server, win  # noqa: F401


@pytest.fixture()
def sync():
    from fsspec.implementations.http_sync import register, unregister

    register()
    yield
    unregister()


def test_list(server, sync):
    h = fsspec.filesystem("http")
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_invalid_args(server, sync):
    with pytest.raises(TypeError):
        h = fsspec.filesystem("http", use_foobar=True)
        h.glob(server + "/index/*")


def test_list_cache(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True)
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_cache_with_expiry_time_cached(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=30)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]

    # Verify cache content.
    assert len(h.dircache) == 1

    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_cache_with_expiry_time_purged(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=0.3)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]
    assert len(h.dircache) == 1

    # Verify cache content.
    assert server + "/index/" in h.dircache
    assert len(h.dircache.get(server + "/index/")) == 1

    # Wait beyond the TTL / cache expiry time.
    time.sleep(0.31)

    # Verify that the cache item should have been purged.
    cached_items = h.dircache.get(server + "/index/")
    assert cached_items is None

    # Verify that after clearing the item from the cache,
    # it can get populated again.
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]
    cached_items = h.dircache.get(server + "/index/")
    assert len(cached_items) == 1


def test_list_cache_reuse(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]

    # Verify cache content.
    assert len(h.dircache) == 1

    # Verify another instance without caching enabled does not have cache content.
    h = fsspec.filesystem("http", use_listings_cache=False)
    assert not h.dircache

    # Verify that yet another new instance, with caching enabled,
    # will see the same cache content again.
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)
    assert len(h.dircache) == 1

    # However, yet another instance with a different expiry time will also not have
    # any valid cache content.
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=666)
    assert len(h.dircache) == 0


def test_ls_raises_filenotfound(server, sync):
    h = fsspec.filesystem("http")

    with pytest.raises(FileNotFoundError):
        h.ls(server + "/not-a-key")


def test_list_cache_with_max_paths(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, max_paths=5)
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_cache_with_skip_instance_cache(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, skip_instance_cache=True)
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_isdir(server, sync):
    h = fsspec.filesystem("http")
    assert h.isdir(server + "/index/")
    assert not h.isdir(server + "/index/realfile")
    assert not h.isdir(server + "doesnotevenexist")


def test_exists(server, sync):
    h = fsspec.filesystem("http")
    assert not h.exists(server + "/notafile")
    with pytest.raises(FileNotFoundError):
        h.cat(server + "/notafile")


def test_read(server, sync):
    h = fsspec.filesystem("http")
    out = server + "/index/realfile"
    # with h.open(out, "rb") as f:
    #    assert f.read() == data
    # with h.open(out, "rb", block_size=0) as f:
    #    assert f.read() == data
    with h.open(out, "rb") as f:
        o1 = f.read(100)
        o2 = f.read()
        assert o1 + o2 == data


def test_methods(server, sync):
    h = fsspec.filesystem("http")
    url = server + "/index/realfile"
    assert h.exists(url)
    assert h.cat(url) == data


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"give_length": "true"},
        {"give_length": "true", "head_ok": "true"},
        {"give_range": "true"},
        {"give_length": "true", "head_not_auth": "true"},
        {"give_range": "true", "head_not_auth": "true"},
        {"use_206": "true", "head_ok": "true", "head_give_length": "true"},
        {"use_206": "true", "give_length": "true"},
        {"use_206": "true", "give_range": "true"},
    ],
)
def test_random_access(server, headers, sync):
    h = fsspec.filesystem("http", headers=headers)
    url = server + "/index/realfile"
    with h.open(url, "rb") as f:
        if headers:
            assert f.size == len(data)
        assert f.read(5) == data[:5]

        if headers:
            f.seek(5, 1)
            assert f.read(5) == data[10:15]
        else:
            with pytest.raises(ValueError):
                f.seek(5, 1)
    assert f.closed


@pytest.mark.parametrize(
    "headers",
    [
        {"ignore_range": "true", "head_ok": "true", "give_length": "true"},
        {"ignore_range": "true", "give_length": "true"},
        {"ignore_range": "true", "give_range": "true"},
    ],
)
def test_no_range_support(server, headers, sync):
    h = fsspec.filesystem("http", headers=headers)
    url = server + "/index/realfile"
    with h.open(url, "rb") as f:
        # Random access is not possible if the server doesn't respect Range
        f.seek(5)
        with pytest.raises(ValueError):
            f.read(10)

        # Reading from the beginning should still work
        f.seek(0)
        assert f.read(10) == data[:10]


def test_mapper_url(server, sync):
    h = fsspec.filesystem("http")
    mapper = h.get_mapper(server + "/index/")
    assert mapper.root.startswith("http:")
    assert list(mapper)

    mapper2 = fsspec.get_mapper(server + "/index/")
    assert mapper2.root.startswith("http:")
    assert list(mapper) == list(mapper2)


def test_content_length_zero(server, sync):
    h = fsspec.filesystem(
        "http", headers={"give_length": "true", "zero_length": "true"}
    )
    url = server + "/index/realfile"

    with h.open(url, "rb") as f:
        assert f.read() == data


def test_download(server, tmpdir, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    url = server + "/index/realfile"
    fn = os.path.join(tmpdir, "afile")
    h.get(url, fn)
    assert open(fn, "rb").read() == data


def test_multi_download(server, tmpdir, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    fna = os.path.join(tmpdir, "afile")
    fnb = os.path.join(tmpdir, "bfile")
    h.get([urla, urlb], [fna, fnb])
    assert open(fna, "rb").read() == data
    assert open(fnb, "rb").read() == data


def test_ls(server, sync):
    h = fsspec.filesystem("http")
    l = h.ls(server + "/data/20020401/", detail=False)
    nc = server + "/data/20020401/GRACEDADM_CLSM0125US_7D.A20020401.030.nc4"
    assert nc in l
    assert len(l) == 11
    assert all(u["type"] == "file" for u in h.ls(server + "/data/20020401/"))
    assert h.glob(server + "/data/20020401/*.nc4") == [nc]


def test_mcat(server, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    out = h.cat([urla, urlb])
    assert out == {urla: data, urlb: data}


def test_cat_file_range(server, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    assert h.cat(urla, start=1, end=10) == data[1:10]
    assert h.cat(urla, start=1) == data[1:]

    assert h.cat(urla, start=-10) == data[-10:]
    assert h.cat(urla, start=-10, end=-2) == data[-10:-2]

    assert h.cat(urla, end=-10) == data[:-10]


def test_mcat_cache(server, sync):
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    fs = fsspec.filesystem("simplecache", target_protocol="http")
    assert fs.cat([urla, urlb]) == {urla: data, urlb: data}


def test_mcat_expand(server, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    out = h.cat(server + "/index/*")
    assert out == {server + "/index/realfile": data}


def test_info(server, sync):
    fs = fsspec.filesystem("http", headers={"give_etag": "true", "head_ok": "true"})
    info = fs.info(server + "/index/realfile")
    assert info["ETag"] == "xxx"


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_put_file(server, tmp_path, method, reset_files, sync):
    src_file = tmp_path / "file_1"
    src_file.write_bytes(data)

    dwl_file = tmp_path / "down_1"

    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    with pytest.raises(FileNotFoundError):
        fs.info(server + "/hey")

    fs.put_file(src_file, server + "/hey", method=method)
    assert fs.info(server + "/hey")["size"] == len(data)

    fs.get_file(server + "/hey", dwl_file)
    assert dwl_file.read_bytes() == data

    src_file.write_bytes(b"xxx")
    with open(src_file, "rb") as stream:
        fs.put_file(stream, server + "/hey_2", method=method)
    assert fs.cat(server + "/hey_2") == b"xxx"

    fs.put_file(io.BytesIO(b"yyy"), server + "/hey_3", method=method)
    assert fs.cat(server + "/hey_3") == b"yyy"


def test_encoded(server, sync):
    fs = fsspec.filesystem("http", encoded=False)
    out = fs.cat(server + "/Hello: Günter", headers={"give_path": "true"})
    assert json.loads(out)["path"] == "/Hello:%20G%C3%BCnter"


def test_with_cache(server, sync):
    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    fn = server + "/index/realfile"
    fs1 = fsspec.filesystem("blockcache", fs=fs)
    with fs1.open(fn, "rb") as f:
        out = f.read()
    assert out == fs1.cat(fn)
