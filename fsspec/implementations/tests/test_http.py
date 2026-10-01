import asyncio
import gzip
import io
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import aiohttp
import pytest

import fsspec.asyn
import fsspec.utils
from fsspec.implementations.http import HTTPStreamFile
from fsspec.tests.conftest import data, reset_files, server, win  # noqa: F401


@pytest.fixture
def encoded_range_server(request):
    content = getattr(request, "param", bytes(range(256)) * 32)
    encoded = gzip.compress(content, mtime=0)
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def respond(self, head=False):
            encoding = self.headers.get("Accept-Encoding", "")
            compress = "gzip" in encoding or not encoding
            representation = encoded if compress else content
            total = len(representation)
            headers = {"Accept-Ranges": "bytes", "Vary": "Accept-Encoding"}
            if compress:
                headers["Content-Encoding"] = "gzip"
            requests.append((self.command, dict(self.headers)))
            status = 200
            if not head and "Range" in self.headers:
                start, end = self.headers["Range"].removeprefix("bytes=").split("-")
                start = int(start)
                end = min(int(end), total - 1)
                if start >= total:
                    status = 416
                    headers["Content-Range"] = f"bytes */{total}"
                    representation = b""
                else:
                    status = 206
                    headers["Content-Range"] = f"bytes {start}-{end}/{total}"
                    representation = representation[start : end + 1]
            headers["Content-Length"] = str(len(representation))
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            if not head:
                self.wfile.write(representation)

        def do_HEAD(self):
            if "head_unavailable" in self.headers:
                requests.append((self.command, dict(self.headers)))
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.respond(head=True)

        def do_GET(self):
            self.respond()

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever)
        thread.start()
        try:
            yield SimpleNamespace(
                url=f"http://127.0.0.1:{httpd.server_port}/data",
                content=content,
                encoded=encoded,
                requests=requests,
            )
        finally:
            httpd.shutdown()
            thread.join(timeout=5)
            assert not thread.is_alive()


@pytest.mark.parametrize("start", [0, 4096])
@pytest.mark.parametrize("metadata_method", ["HEAD", "GET"])
def test_range_uses_identity_representation(
    encoded_range_server, start, metadata_method
):
    server = encoded_range_server
    headers = {"X-Test": "preserved"}
    if metadata_method == "GET":
        headers["head_unavailable"] = "true"
    original = headers.copy()
    fs = fsspec.filesystem("http", headers=headers)
    with fs.open(server.url, block_size=16, cache_type="none") as f:
        assert f.size == len(server.content)
        f.seek(start)
        assert f.read(5) == server.content[start : start + 5]
    assert headers == original
    assert all(h["X-Test"] == "preserved" for _, h in server.requests)
    metadata = [(m, h) for m, h in server.requests if "Range" not in h]
    assert [m for m, _ in metadata] == (
        ["HEAD"] if metadata_method == "HEAD" else ["HEAD", "GET"]
    )
    assert all(h["Accept-Encoding"] == "identity" for _, h in metadata)


@pytest.mark.parametrize(
    "encoded_range_server",
    [
        b'def main():\n    print("Hallo, R\xc3\xa4uber Hotzenplotz.")  # noqa: T201\n'
        b"    return 42\n"
    ],
    indirect=True,
)
def test_small_file_read_all_with_encoding(encoded_range_server):
    server = encoded_range_server
    assert len(server.encoded) > len(server.content)
    fs = fsspec.filesystem("http")
    with fs.open(server.url) as f:
        assert f.size == len(server.content)
        assert f.size < f.blocksize
        assert f.read() == server.content
    assert server.requests[-1][1]["Range"] == f"bytes=0-{len(server.content) - 1}"


@pytest.mark.parametrize(
    "header_name", ["Accept-Encoding", "accept-encoding", "aCcEpT-EnCoDiNg"]
)
@pytest.mark.parametrize("location", ["request", "session", "request_over_session"])
def test_range_preserves_explicit_encoding(encoded_range_server, header_name, location):
    server = encoded_range_server
    headers = {header_name: "gzip", "X-Test": "preserved"}
    original = headers.copy()
    options = {"client_kwargs": {"auto_decompress": False}}
    if location != "session":
        options["headers"] = headers
        if location == "request_over_session":
            options["client_kwargs"]["headers"] = {"Accept-Encoding": "identity"}
    else:
        options["client_kwargs"]["headers"] = headers
    fs = fsspec.filesystem("http", **options)
    with fs.open(
        server.url, size=len(server.encoded), block_size=16, cache_type="none"
    ) as f:
        assert f.read(5) == server.encoded[:5]
        f.seek(100)
        assert f.read(5) == server.encoded[100:105]
    assert headers == original
    assert all(h["X-Test"] == "preserved" for _, h in server.requests)


@pytest.mark.parametrize("block_size", [0, 16])
def test_full_read_preserves_compression(encoded_range_server, block_size):
    server = encoded_range_server
    fs = fsspec.filesystem("http")
    with fs.open(server.url, block_size=block_size) as f:
        assert f.read() == server.content
    get_headers = [h for method, h in server.requests if method == "GET"]
    assert len(get_headers) == 1
    assert "gzip" in get_headers[0]["Accept-Encoding"]


def test_list(server):
    h = fsspec.filesystem("http")
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_invalid_args(server):
    with pytest.raises(TypeError):
        h = fsspec.filesystem("http", use_foobar=True)
        h.glob(server.address + "/index/*")


def test_list_cache(server):
    h = fsspec.filesystem("http", use_listings_cache=True)
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_cache_with_expiry_time_cached(server):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=30)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]

    # Verify cache content.
    assert len(h.dircache) == 1

    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_cache_with_expiry_time_purged(server):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=0.3)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]
    assert len(h.dircache) == 1

    # Verify cache content.
    assert server.address + "/index/" in h.dircache
    assert len(h.dircache.get(server.address + "/index/")) == 1

    # Wait beyond the TTL / cache expiry time.
    time.sleep(0.31)

    # Verify that the cache item should have been purged.
    cached_items = h.dircache.get(server.address + "/index/")
    assert cached_items is None

    # Verify that after clearing the item from the cache,
    # it can get populated again.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]
    cached_items = h.dircache.get(server.address + "/index/")
    assert len(cached_items) == 1


def test_list_cache_reuse(server):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]

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
        h.ls(server.address + "/not-a-key")


def test_list_cache_with_max_paths(server):
    h = fsspec.filesystem("http", use_listings_cache=True, max_paths=5)
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_list_cache_with_skip_instance_cache(server):
    h = fsspec.filesystem("http", use_listings_cache=True, skip_instance_cache=True)
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_glob_return_subfolders(server):
    h = fsspec.filesystem("http")
    out = h.glob(server.address + "/simple/*")
    assert set(out) == {
        server.address + "/simple/dir/",
        server.address + "/simple/file",
    }


def test_isdir(server):
    h = fsspec.filesystem("http", headers={"give_mimetype": "true"})
    assert h.isdir(server.address + "/index/")
    assert not h.isdir(server.realfile)
    assert not h.isdir(server.address + "doesnotevenexist")

    h = fsspec.filesystem("http")
    assert h.isdir(server.address + "/index/")
    assert not h.isdir(server.realfile)
    assert not h.isdir(server.address + "doesnotevenexist")


def test_policy_arg(server):
    h = fsspec.filesystem("http", size_policy="get")
    out = h.glob(server.address + "/index/*")
    assert out == [server.realfile]


def test_exists(server):
    h = fsspec.filesystem("http")
    assert not h.exists(server.address + "/notafile")
    with pytest.raises(FileNotFoundError):
        h.cat(server.address + "/notafile")


def test_exists_strict(server):
    h = fsspec.filesystem("http")
    assert not h.exists(server.address + "/notafile", strict=True)
    with pytest.raises(aiohttp.ClientResponseError) as e:
        h.exists(server.address + "/unauthorized", strict=True)
    assert e.value.status == 401


def test_read(server):
    h = fsspec.filesystem("http")
    out = server.realfile
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
    out = server.realfile

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
    out = server.realfile
    with h.open(out, "rb") as f:
        out = pickle.dumps(f)
        assert f.read() == data
    with pickle.loads(out) as f:
        assert f.read() == data


def test_methods(server):
    h = fsspec.filesystem("http")
    url = server.realfile
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
    url = server.realfile
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
        # HTTPFile seeks, response headers lack size, assumed no range support
        {"head_ok": "true", "head_give_length": "true"},
        # HTTPFile seeks, response is not a range
        {"ignore_range": "true", "give_length": "true"},
        {"ignore_range": "true", "give_range": "true"},
        # HTTPStreamFile does not seek (past 0)
        {"accept_range": "none", "head_ok": "true", "give_length": "true"},
    ],
)
def test_no_range_support(server, headers):
    h = fsspec.filesystem("http", headers=headers)
    url = server.realfile
    with h.open(url, "rb") as f:
        # Random access is not possible if the server doesn't respect Range
        with pytest.raises(ValueError):
            f.seek(5)
            f.read(10)

        # Reading from the beginning should still work
        f.seek(0)
        assert f.read(10) == data[:10]


def test_stream_seek(server):
    h = fsspec.filesystem("http")
    url = server.realfile
    with h.open(url, "rb") as f:
        f.seek(0)  # is OK
        data1 = f.read(5)
        assert len(data1) == 5
        f.seek(5)
        f.seek(0, 1)
        data2 = f.read()
        assert data1 + data2 == data


def test_mapper_url(server):
    h = fsspec.filesystem("http")
    mapper = h.get_mapper(server.address + "/index/")
    assert mapper.root.startswith("http:")
    assert list(mapper)

    mapper2 = fsspec.get_mapper(server.address + "/index/")
    assert mapper2.root.startswith("http:")
    assert list(mapper) == list(mapper2)


def test_content_length_zero(server):
    h = fsspec.filesystem(
        "http", headers={"give_length": "true", "zero_length": "true"}
    )
    url = server.realfile

    with h.open(url, "rb") as f:
        assert f.read() == data


def test_content_encoding_gzip(server):
    h = fsspec.filesystem(
        "http", headers={"give_length": "true", "gzip_encoding": "true"}
    )
    url = server.realfile

    with h.open(url, "rb") as f:
        assert isinstance(f, HTTPStreamFile)
        assert f.size is None
        assert f.read() == data


def test_download(server, tmpdir):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    url = server.realfile
    fn = os.path.join(tmpdir, "afile")
    h.get(url, fn)
    assert open(fn, "rb").read() == data


def test_multi_download(server, tmpdir):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    urlb = server.address + "/index/otherfile"
    fna = os.path.join(tmpdir, "afile")
    fnb = os.path.join(tmpdir, "bfile")
    h.get([urla, urlb], [fna, fnb])
    assert open(fna, "rb").read() == data
    assert open(fnb, "rb").read() == data


def test_ls(server):
    h = fsspec.filesystem("http")
    l = h.ls(server.address + "/data/20020401/", detail=False)
    nc = server.address + "/data/20020401/GRACEDADM_CLSM0125US_7D.A20020401.030.nc4"
    assert nc in l
    assert len(l) == 11
    assert all(u["type"] == "file" for u in h.ls(server.address + "/data/20020401/"))
    assert h.glob(server.address + "/data/20020401/*.nc4") == [nc]


def test_mcat(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    urlb = server.address + "/index/otherfile"
    out = h.cat([urla, urlb])
    assert out == {urla: data, urlb: data}


def test_cat_file_range(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    assert h.cat(urla, start=1, end=10) == data[1:10]
    assert h.cat(urla, start=1) == data[1:]

    assert h.cat(urla, start=-10) == data[-10:]
    assert h.cat(urla, start=-10, end=-2) == data[-10:-2]

    assert h.cat(urla, end=-10) == data[:-10]


def test_cat_file_range_numpy(server):
    np = pytest.importorskip("numpy")
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.realfile
    assert h.cat(urla, start=np.int8(1), end=np.int8(10)) == data[1:10]
    out = h.cat_ranges([urla, urla], starts=np.array([1, 5]), ends=np.array([10, 15]))
    assert out == [data[1:10], data[5:15]]


def test_mcat_cache(server):
    urla = server.realfile
    urlb = server.address + "/index/otherfile"
    fs = fsspec.filesystem("simplecache", target_protocol="http")
    assert fs.cat([urla, urlb]) == {urla: data, urlb: data}


def test_mcat_expand(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    out = h.cat(server.address + "/index/*")
    assert out == {server.realfile: data}


def test_info(server):
    fs = fsspec.filesystem("http", headers={"give_etag": "true", "head_ok": "true"})
    info = fs.info(server.realfile)
    assert info["ETag"] == "xxx"

    fs = fsspec.filesystem("http", headers={"give_mimetype": "true"})
    info = fs.info(server.realfile)
    assert info["mimetype"] == "text/html"

    fs = fsspec.filesystem("http", headers={"redirect": "true"})
    info = fs.info(server.address + "/redirectme")
    assert info["url"] == server.realfile


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_put_file(server, tmp_path, method, reset_files):
    src_file = tmp_path / "file_1"
    src_file.write_bytes(data)

    dwl_file = tmp_path / "down_1"

    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    with pytest.raises(FileNotFoundError):
        fs.info(server.address + "/hey")

    fs.put_file(src_file, server.address + "/hey", method=method)
    assert fs.info(server.address + "/hey")["size"] == len(data)

    fs.get_file(server.address + "/hey", dwl_file)
    assert dwl_file.read_bytes() == data

    src_file.write_bytes(b"xxx")
    with open(src_file, "rb") as stream:
        fs.put_file(stream, server.address + "/hey_2", method=method)
    assert fs.cat(server.address + "/hey_2") == b"xxx"

    fs.put_file(io.BytesIO(b"yyy"), server.address + "/hey_3", method=method)
    assert fs.cat(server.address + "/hey_3") == b"yyy"


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

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:  # Python 3.14+ codepath
        loop = asyncio.new_event_loop()

    th = threading.Thread(target=loop.run_forever)

    th.daemon = True
    th.start()
    fs = fsspec.filesystem("http", asynchronous=True, loop=loop)
    asyncio.run_coroutine_threadsafe(fs.set_session(), loop=loop).result()
    url = server.realfile
    cor = fs._cat([url])
    fut = asyncio.run_coroutine_threadsafe(cor, loop=loop)
    assert fut.result() == {url: data}
    loop.call_soon_threadsafe(loop.stop)


def test_async_this_thread(server):
    async def _():
        fs = fsspec.filesystem("http", asynchronous=True)

        session = await fs.set_session()  # creates client

        url = server.realfile
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
    fn = server.realfile
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
    fn = server.realfile
    of = await fs.open_async(fn)
    async with of as f:
        out1 = await f.read(10)
        assert data.startswith(out1)
        out2 = await f.read()
        assert data == out1 + out2
    await fs._session.close()


def test_encoded(server):
    fs = fsspec.filesystem("http", encoded=True)
    out = fs.cat(
        server.address + "/Hello%3A%20G%C3%BCnter", headers={"give_path": "true"}
    )
    assert json.loads(out)["path"] == "/Hello%3A%20G%C3%BCnter"
    with pytest.raises(aiohttp.client_exceptions.ClientError):
        fs.cat(server.address + "/Hello: Günter", headers={"give_path": "true"})

    fs = fsspec.filesystem("http", encoded=False)
    out = fs.cat(server.address + "/Hello: Günter", headers={"give_path": "true"})
    assert json.loads(out)["path"] == "/Hello:%20G%C3%BCnter"


def test_with_cache(server):
    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    fn = server.realfile
    fs1 = fsspec.filesystem("blockcache", fs=fs)
    with fs1.open(fn, "rb") as f:
        out = f.read()
    assert out == fs1.cat(fn)


@pytest.mark.asyncio
async def test_async_expand_path(server):
    fs = fsspec.filesystem("http", asynchronous=True, skip_instance_cache=True)

    # maxdepth=1
    assert await fs._expand_path(
        server.address + "/index", recursive=True, maxdepth=1
    ) == [
        server.address + "/index",
        server.address + "/index/realfile",
    ]

    # maxdepth=0
    with pytest.raises(ValueError):
        await fs._expand_path(server.address + "/index", maxdepth=0)
    with pytest.raises(ValueError):
        await fs._expand_path(server.address + "/index", recursive=True, maxdepth=0)

    await fs._session.close()


@pytest.mark.asyncio
async def test_async_walk(server):
    fs = fsspec.filesystem("http", asynchronous=True, skip_instance_cache=True)

    # No maxdepth
    res = [a async for a in fs._walk(server.address + "/index")]
    assert res == [(server.address + "/index", [], ["realfile"])]

    # maxdepth=0
    with pytest.raises(ValueError):
        async for a in fs._walk(server.address + "/index", maxdepth=0):
            pass

    await fs._session.close()


def test_pipe_file(server, tmpdir, reset_files):
    """Test that the pipe_file method works correctly."""
    import io

    import fsspec

    # Create test data
    test_content = b"This is test data to pipe to a file"

    # Initialize filesystem
    fs = fsspec.filesystem("http", headers={"accept_put": "true"})

    # Test that the file doesn't exist yet
    with pytest.raises(FileNotFoundError):
        fs.info(server.address + "/piped_file")

    # Pipe data to the file
    fs.pipe_file(server.address + "/piped_file", test_content)

    # Verify the file exists now
    assert fs.exists(server.address + "/piped_file")

    # Verify content
    assert fs.cat(server.address + "/piped_file") == test_content

    # Test with different modes and headers
    fs.pipe_file(
        server.address + "/piped_file2",
        test_content,
        mode="overwrite",
        headers={"Content-Type": "text/plain"},
    )
    assert fs.cat(server.address + "/piped_file2") == test_content

    # Test with byte-like object
    bytesio = io.BytesIO(b"BytesIO content")
    fs.pipe_file(server.address + "/piped_bytes", bytesio.getvalue())
    assert fs.cat(server.address + "/piped_bytes") == b"BytesIO content"


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_protocol_independent_of_first_used_protocol(protocol):
    from fsspec import filesystem

    filesystem(protocol)
    fs0 = filesystem("http")
    p0 = fs0.protocol[0] if isinstance(fs0.protocol, tuple) else fs0.protocol
    fs1 = filesystem("https")
    p1 = fs1.protocol[0] if isinstance(fs1.protocol, tuple) else fs1.protocol
    assert p0 == p1 == "http"
