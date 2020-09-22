import contextlib
import asyncio
import os
import pytest
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys
import threading
import fsspec

requests = pytest.importorskip("requests")
port = 9898
data = b"\n".join([b"some test data"] * 1000)
realfile = "http://localhost:%i/index/realfile" % port
index = b'<a href="%s">Link</a>' % realfile.encode()
win = os.name == "nt"


class HTTPTestHandler(BaseHTTPRequestHandler):
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
        if self.path.rstrip("/") not in [
            "/index/realfile",
            "/index/otherfile",
            "/index",
        ]:
            self._respond(404)
            return

        d = data if self.path in ["/index/realfile", "/index/otherfile"] else index
        if "Range" in self.headers:
            ran = self.headers["Range"]
            b, ran = ran.split("=")
            start, end = ran.split("-")
            d = d[int(start) : int(end) + 1]
        if "give_length" in self.headers:
            response_headers = {"Content-Length": len(d)}
            self._respond(200, response_headers, d)
        elif "give_range" in self.headers:
            self._respond(200, {"Content-Range": "0-%i/%i" % (len(d) - 1, len(d))}, d)
        else:
            self._respond(200, data=d)

    def do_HEAD(self):
        if "head_ok" not in self.headers:
            self._respond(405)
            return
        d = data if self.path == "/index/realfile" else index
        if self.path.rstrip("/") not in ["/index/realfile", "/index"]:
            self._respond(404)
        elif "give_length" in self.headers:
            response_headers = {"Content-Length": len(d)}
            if "zero_length" in self.headers:
                response_headers["Content-Length"] = 0

            self._respond(200, response_headers)
        elif "give_range" in self.headers:
            self._respond(200, {"Content-Range": "0-%i/%i" % (len(d) - 1, len(d))})
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


def test_list(server):
    h = fsspec.filesystem("http")
    out = h.glob(server + "/index/*")
    assert out == [server + "/index/realfile"]


def test_isdir(server):
    h = fsspec.filesystem("http")
    assert h.isdir(server + "/index/")
    assert not h.isdir(server + "/index/realfile")


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


def test_mcat(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    urla = server + "/index/realfile"
    urlb = server + "/index/otherfile"
    out = h.cat([urla, urlb])
    assert out == {urla: data, urlb: data}


def test_mcat_expand(server):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true "})
    out = h.cat(server + "/index/*")
    assert out == {server + "/index/realfile": data}


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
    fs = fsspec.filesystem("http", asynchronous=False, loop=loop)
    cor = fs._cat([server + "/index/realfile"])
    fut = asyncio.run_coroutine_threadsafe(cor, loop=loop)
    assert fut.result() == [data]


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
def test_async_this_thread(server):
    async def _():
        loop = asyncio.get_event_loop()
        fs = fsspec.filesystem("http", asynchronous=True, loop=loop)

        # fails because client creation has not yet been awaited
        assert isinstance(
            (await fs._cat([server + "/index/realfile"]))[0], RuntimeError
        )
        with pytest.raises(RuntimeError):
            fs.cat([server + "/index/realfile"])

        await fs.set_session()  # creates client

        out = await fs._cat([server + "/index/realfile"])
        del fs
        assert out == [data]

    asyncio.run(_())


def _inner_pass(fs, q, fn):
    # pass the s3 instance, but don't use it; in new process, the instance
    # cache should be skipped to make a new instance
    fs = fsspec.filesystem("http")
    q.put(fs.cat(fn))


@pytest.mark.parametrize("method", ["spawn", "forkserver", "fork"])
def test_processes(server, method):
    import multiprocessing as mp

    if win and method != "spawn":
        pytest.skip("Windows can only spawn")
    ctx = mp.get_context(method)
    fn = server + "/index/realfile"
    fs = fsspec.filesystem("http")

    q = ctx.Queue()
    if os.environ.get("TRAVIS", ""):
        os.chdir("")
    p = ctx.Process(target=_inner_pass, args=(fs, q, fn))
    p.start()
    assert q.get() == fs.cat(fn)
    p.join()
