import asyncio
import io
import json
import os
import sys
import time

import aiohttp
import pytest

import fsspec.asyn
import fsspec.utils
from fsspec.tests.conftest import data, reset_files, server, win  # noqa: F401


def test_list(server):
    h = fsspec.filesystem("http")
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_invalid_args(server):
    with pytest.raises(TypeError):
        h = fsspec.filesystem("http", use_foobar=True)
        h.glob(server + "/index/*")


def test_list_cache(server):
    h = fsspec.filesystem("http", use_listings_cache=True)
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_cache_with_expiry_time_cached(server):
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


def test_list_cache_with_expiry_time_purged(server):
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


def test_list_cache_reuse(server):
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


def test_ls_raises_filenotfound(server):
    h = fsspec.filesystem("http")

    with pytest.raises(FileNotFoundError):
        h.ls(server + "/not-a-key")


def test_list_cache_with_max_paths(server):
    h = fsspec.filesystem("http", use_listings_cache=True, max_paths=5)
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_list_cache_with_skip_instance_cache(server):
    h = fsspec.filesystem("http", use_listings_cache=True, skip_instance_cache=True)
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_isdir(server):
    h = fsspec.filesystem("http")
    assert h.isdir(server + "/index/")
    assert not h.isdir(server + "/index/realfile")
    assert not h.isdir(server + "doesnotevenexist")


def test_policy_arg(server):
    h = fsspec.filesystem("http", size_policy="get")
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_exists(server):
    h = fsspec.filesystem("http")
    assert not h.exists(server + "/notafile")
    with pytest.raises(FileNotFoundError):
        h.cat(server + "/notafile")


def test_read(server):
    h = fsspec.filesystem("http")
    out = server + "/index/realfile"
    with h.open(out, "rb") as f:
        assert f.read() == data
    with h.open(out, "rb", block_size=0) as f:
        assert f.read() == data
    with h.open(out, "rb") as f:
        assert f.read(100) + f.read() == data


def test_file_pickle(server):
    import pickle

    # via HTTPFile
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    out = server + "/index/realfile"

    with fsspec.open(out, headers={"give_length": "true", "head_ok": "true"}) as f:
        pic = pickle.loads(pickle.dumps(f))
        assert pic.read() == data

    with h.open(out, "rb") as f:
        pic = pickle.dumps(f)
        assert f.read() == data
    with pickle.loads(pic) as f:
        assert f.read() == data

    # via HTTPStreamFile
    h = fsspec.filesystem("http")
    out = server + "/index/realfile"
    with h.open(out, "rb") as f:
        out = pickle.dumps(f)
        assert f.read() == data
    with pickle.loads(out) as f:
        assert f.read() == data


def test_methods(server):
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
def test_random_access(server, headers):
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
        {"ignore_range": "true", "head_ok": "true", "head_give_length": "true"},
        {"ignore_range": "true", "give_length": "true"},
        {"ignore_range": "true", "give_range": "true"},
    ],
)
def test_no_range_support(server, headers):
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


def test_mapper_url(server):
    h = fsspec.filesystem("http")
    mapper = h.get_mapper(server + "/index/")
    assert mapper.root.startswith("http:")
    assert list(mapper)

    mapper2 = fsspec.get_mapper(server + "/index/")
    assert mapper2.root.startswith("http:")
    assert list(mapper) == list(mapper2)


def test_content_length_zero(server):
    h = fsspec.filesystem(
        "http", headers={"give_length": "true", "zero_length": "true"}
    )
    url = server + "/index/realfile"

    with h.open(url, "rb") as f:
        assert f.read() == data


def test_download(server, tmpdir):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    url = server + "/index/realfile"
    fn = os.path.join(tmpdir, "afile")
    h.get(url, fn)
    assert open(fn, "rb").read() == data


def test_multi_download(server, tmpdir):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    fna = os.path.join(tmpdir, "afile")
    fnb = os.path.join(tmpdir, "bfile")
    h.get([urla, urlb], [fna, fnb])
    assert open(fna, "rb").read() == data
    assert open(fnb, "rb").read() == data


def test_ls(server):
    h = fsspec.filesystem("http")
    l = h.ls(server + "/data/20020401/", detail=False)
    nc = server + "/data/20020401/GRACEDADM_CLSM0125US_7D.A20020401.030.nc4"
    assert nc in l
    assert len(l) == 11
    assert all(u["type"] == "file" for u in h.ls(server + "/data/20020401/"))
    assert h.glob(server + "/data/20020401/*.nc4") == [nc]


def test_mcat(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    out = h.cat([urla, urlb])
    assert out == {urla: data, urlb: data}


def test_cat_file_range(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    assert h.cat(urla, start=1, end=10) == data[1:10]
    assert h.cat(urla, start=1) == data[1:]

    assert h.cat(urla, start=-10) == data[-10:]
    assert h.cat(urla, start=-10, end=-2) == data[-10:-2]

    assert h.cat(urla, end=-10) == data[:-10]


def test_mcat_cache(server):
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    fs = fsspec.filesystem("simplecache", target_protocol="http")
    assert fs.cat([urla, urlb]) == {urla: data, urlb: data}


def test_mcat_expand(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    out = h.cat(server + "/index/*")
    assert out == {server + "/index/realfile": data}


def test_info(server):
    fs = fsspec.filesystem("http", headers={"give_etag": "true", "head_ok": "true"})
    info = fs.info(server + "/index/realfile")
    assert info["ETag"] == "xxx"


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_put_file(server, tmp_path, method, reset_files):
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


async def get_aiohttp():
    from aiohttp import ClientSession

    return ClientSession()


async def get_proxy():
    class ProxyClient:
        pass

    return ProxyClient()


@pytest.mark.xfail(
    condition=sys.flags.optimize > 1, reason="no docstrings when optimised"
)
def test_docstring():
    h = fsspec.filesystem("http")
    # most methods have empty docstrings and draw from base class, but this one
    # is generated
    assert h.pipe.__doc__


def test_async_other_thread(server):
    import threading

    loop = asyncio.get_event_loop()
    th = threading.Thread(target=loop.run_forever)

    th.daemon = True
    th.start()
    fs = fsspec.filesystem("http", asynchronous=True, loop=loop)
    asyncio.run_coroutine_threadsafe(fs.set_session(), loop=loop).result()
    url = server + "/index/realfile"
    cor = fs._cat([url])
    fut = asyncio.run_coroutine_threadsafe(cor, loop=loop)
    assert fut.result() == {url: data}
    loop.call_soon_threadsafe(loop.stop)


def test_async_this_thread(server):
    async def _():
        fs = fsspec.filesystem("http", asynchronous=True)

        session = await fs.set_session()  # creates client

        url = server + "/index/realfile"
        with pytest.raises((NotImplementedError, RuntimeError)):
            fs.cat([url])
        out = await fs._cat([url])
        del fs
        assert out == {url: data}
        await session.close()

    asyncio.run(_())


def _inner_pass(fs, q, fn):
    # pass the FS instance, but don't use it; in new process, the instance
    # cache should be skipped to make a new instance
    import traceback

    try:
        fs = fsspec.filesystem("http")
        q.put(fs.cat(fn))
    except Exception:
        q.put(traceback.format_exc())


@pytest.mark.parametrize("method", ["spawn", "forkserver"])
def test_processes(server, method):
    import multiprocessing as mp

    if win and method != "spawn":
        pytest.skip("Windows can only spawn")
    ctx = mp.get_context(method)
    fn = server + "/index/realfile"
    fs = fsspec.filesystem("http")

    q = ctx.Queue()
    p = ctx.Process(target=_inner_pass, args=(fs, q, fn))
    p.start()
    out = q.get()
    assert out == fs.cat(fn)
    p.join()


@pytest.mark.parametrize("get_client", [get_aiohttp, get_proxy])
def test_close(get_client):
    fs = fsspec.filesystem("http", skip_instance_cache=True)
    fs.close_session(None, asyncio.run(get_client()))


@pytest.mark.asyncio
async def test_async_file(server):
    fs = fsspec.filesystem("http", asynchronous=True, skip_instance_cache=True)
    fn = server + "/index/realfile"
    of = await fs.open_async(fn)
    async with of as f:
        out1 = await f.read(10)
        assert data.startswith(out1)
        out2 = await f.read()
        assert data == out1 + out2
    await fs._session.close()


def test_encoded(server):
    fs = fsspec.filesystem("http", encoded=True)
    out = fs.cat(server + "/Hello%3A%20G%C3%BCnter", headers={"give_path": "true"})
    assert json.loads(out)["path"] == "/Hello%3A%20G%C3%BCnter"
    with pytest.raises(aiohttp.client_exceptions.ClientError):
        fs.cat(server + "/Hello: Günter", headers={"give_path": "true"})

    fs = fsspec.filesystem("http", encoded=False)
    out = fs.cat(server + "/Hello: Günter", headers={"give_path": "true"})
    assert json.loads(out)["path"] == "/Hello:%20G%C3%BCnter"


def test_with_cache(server):
    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    fn = server + "/index/realfile"
    fs1 = fsspec.filesystem("blockcache", fs=fs)
    with fs1.open(fn, "rb") as f:
        out = f.read()
    assert out == fs1.cat(fn)
