import asyncio
import contextlib
import io
import os
import sys
import threading
import time
from collections import ChainMap
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import fsspec.asyn
import fsspec.utils

fsspec.utils.setup_logging(logger_name="fsspec.http")

requests = pytest.importorskip("requests")
port = 9898
data = b"\n".join([b"some test data"] * 1000)
realfile = "http://localhost:%i/index/realfile" % port
index = b'<a href="%s">Link</a>' % realfile.encode()
listing = open(
    os.path.join(os.path.dirname(__file__), "data", "listing.html"), "rb"
).read()
win = os.name == "nt"


class HTTPTestHandler(BaseHTTPRequestHandler):
    static_files = {
        "/index/realfile": data,
        "/index/otherfile": data,
        "/index": index,
        "/data/20020401": listing,
    }
    dynamic_files = {}

    files = ChainMap(dynamic_files, static_files)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _respond(self, code=200, headers=None, data=b""):
        headers = headers or {}
        headers.update({"User-Agent": "test"})
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, str(v))
        self.end_headers()
        if data:
            self.wfile.write(data)

    def do_GET(self):
        file_path = self.path.rstrip("/")
        file_data = self.files.get(file_path)
        if file_data is None:
            return self._respond(404)
        if "Range" in self.headers:
            ran = self.headers["Range"]
            b, ran = ran.split("=")
            start, end = ran.split("-")
            if start:
                file_data = file_data[int(start) : (int(end) + 1) if end else None]
            else:
                # suffix only
                file_data = file_data[-int(end) :]
        if "give_length" in self.headers:
            response_headers = {"Content-Length": len(file_data)}
            self._respond(200, response_headers, file_data)
        elif "give_range" in self.headers:
            self._respond(
                200,
                {"Content-Range": "0-%i/%i" % (len(file_data) - 1, len(file_data))},
                file_data,
            )
        else:
            self._respond(200, data=file_data)

    def do_POST(self):
        length = self.headers.get("Content-Length")
        file_path = self.path.rstrip("/")
        if length is None:
            assert self.headers.get("Transfer-Encoding") == "chunked"
            self.files[file_path] = b"".join(self.read_chunks())
        else:
            self.files[file_path] = self.rfile.read(length)
        self._respond(200)

    do_PUT = do_POST

    def read_chunks(self):
        length = -1
        while length != 0:
            line = self.rfile.readline().strip()
            if len(line) == 0:
                length = 0
            else:
                length = int(line, 16)
            yield self.rfile.read(length)
            self.rfile.readline()

    def do_HEAD(self):
        if "head_not_auth" in self.headers:
            return self._respond(
                403, {"Content-Length": 123}, b"not authorized for HEAD request"
            )
        elif "head_ok" not in self.headers:
            return self._respond(405)

        file_path = self.path.rstrip("/")
        file_data = self.files.get(file_path)
        if file_data is None:
            return self._respond(404)

        if "give_length" in self.headers:
            response_headers = {"Content-Length": len(file_data)}
            if "zero_length" in self.headers:
                response_headers["Content-Length"] = 0

            self._respond(200, response_headers)
        elif "give_range" in self.headers:
            self._respond(
                200, {"Content-Range": "0-%i/%i" % (len(file_data) - 1, len(file_data))}
            )
        elif "give_etag" in self.headers:
            self._respond(200, {"ETag": "xxx"})
        else:
            self._respond(200)  # OK response, but no useful info


@contextlib.contextmanager
def serve():
    server_address = ("", port)
    httpd = HTTPServer(server_address, HTTPTestHandler)
    th = threading.Thread(target=httpd.serve_forever)
    th.daemon = True
    th.start()
    try:
        yield "http://localhost:%i" % port
    finally:
        httpd.socket.close()
        httpd.shutdown()
        th.join()


@pytest.fixture(scope="module")
def server():
    with serve() as s:
        yield s


@pytest.fixture
def reset_files():
    yield

    # Reset the newly added files after the
    # test is completed.
    HTTPTestHandler.dynamic_files.clear()


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


@pytest.mark.parametrize("get_client", [get_aiohttp, get_proxy])
@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in <3.7")
def test_close(get_client):
    fs = fsspec.filesystem("http", skip_instance_cache=True)
    fs.close_session(None, asyncio.run(get_client()))


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


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
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
