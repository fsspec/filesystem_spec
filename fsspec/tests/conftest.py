import contextlib
import gzip
import json
import os
import threading
from collections import ChainMap
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace
from typing import ClassVar

import pytest

requests = pytest.importorskip("requests")
data = b"\n".join([b"some test data"] * 1000)
listing = open(
    os.path.join(os.path.dirname(__file__), "data", "listing.html"), "rb"
).read()
win = os.name == "nt"


def _make_realfile(baseurl):
    return f"{baseurl}/index/realfile"


def _make_index_listing(baseurl):
    realfile = _make_realfile(baseurl)
    return b'<a href="%s">Link</a>' % realfile.encode()


def _make_listing(*paths):
    def _make_listing_port(baseurl):
        return "\n".join(
            f'<a href="{baseurl}{f}">Link_{i}</a>' for i, f in enumerate(paths)
        ).encode()

    return _make_listing_port


@pytest.fixture
def reset_files():
    yield

    # Reset the newly added files after the
    # test is completed.
    HTTPTestHandler.dynamic_files.clear()


class HTTPTestHandler(BaseHTTPRequestHandler):
    static_files: ClassVar[dict[str, bytes]] = {
        "/index/realfile": data,
        "/index/otherfile": data,
        "/index": _make_index_listing,
        "/data/20020401": listing,
        "/simple/": _make_listing("/simple/file", "/simple/dir/"),
        "/simple/file": data,
        "/simple/dir/": _make_listing("/simple/dir/file"),
        "/simple/dir/file": data,
        "/unauthorized": AssertionError("shouldn't access"),
    }
    dynamic_files: ClassVar[dict[str, bytes]] = {}

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
        baseurl = f"http://127.0.0.1:{self.server.server_port}"
        file_path = self.path
        if file_path.endswith("/") and file_path.rstrip("/") in self.files:
            file_path = file_path.rstrip("/")
        file_data = self.files.get(file_path)
        if callable(file_data):
            file_data = file_data(baseurl)
        if "give_path" in self.headers:
            return self._respond(200, data=json.dumps({"path": self.path}).encode())
        if "redirect" in self.headers and file_path != "/index/realfile":
            new_url = _make_realfile(baseurl)
            return self._respond(301, {"Location": new_url})
        if file_path == "/unauthorized":
            return self._respond(401)
        if file_data is None:
            return self._respond(404)

        status = 200
        content_range = f"bytes 0-{len(file_data) - 1}/{len(file_data)}"
        if ("Range" in self.headers) and ("ignore_range" not in self.headers):
            ran = self.headers["Range"]
            _b, ran = ran.split("=")
            start, end = ran.split("-")
            if start:
                content_range = f"bytes {start}-{end}/{len(file_data)}"
                file_data = file_data[int(start) : (int(end) + 1) if end else None]
            else:
                # suffix only
                l = len(file_data)
                content_range = f"bytes {l - int(end)}-{l - 1}/{l}"
                file_data = file_data[-int(end) :]
            if "use_206" in self.headers:
                status = 206
        if "give_length" in self.headers:
            if "gzip_encoding" in self.headers:
                file_data = gzip.compress(file_data)
                response_headers = {
                    "Content-Length": len(file_data),
                    "Content-Encoding": "gzip",
                }
            else:
                response_headers = {"Content-Length": len(file_data)}
            self._respond(status, response_headers, file_data)
        elif "give_range" in self.headers:
            self._respond(status, {"Content-Range": content_range}, file_data)
        elif "give_mimetype" in self.headers:
            self._respond(
                status, {"Content-Type": "text/html; charset=utf-8"}, file_data
            )
        else:
            self._respond(status, data=file_data)

    def do_POST(self):
        length = self.headers.get("Content-Length")
        file_path = self.path.rstrip("/")
        if length is None:
            assert self.headers.get("Transfer-Encoding") == "chunked"
            self.files[file_path] = b"".join(self.read_chunks())
        else:
            self.files[file_path] = self.rfile.read(int(length))
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
        r_headers = {}
        if "head_not_auth" in self.headers:
            r_headers["Content-Length"] = 123
            return self._respond(403, r_headers, b"not authorized for HEAD request")
        elif "head_ok" not in self.headers:
            return self._respond(405)

        file_path = self.path.rstrip("/")
        file_data = self.files.get(file_path)
        if file_data is None:
            return self._respond(404)

        if ("give_length" in self.headers) or ("head_give_length" in self.headers):
            if "zero_length" in self.headers:
                r_headers["Content-Length"] = 0
            elif "gzip_encoding" in self.headers:
                file_data = gzip.compress(file_data)
                r_headers["Content-Encoding"] = "gzip"
                r_headers["Content-Length"] = len(file_data)
            else:
                r_headers["Content-Length"] = len(file_data)
        elif "give_range" in self.headers:
            r_headers["Content-Range"] = f"0-{len(file_data) - 1}/{len(file_data)}"
        elif "give_etag" in self.headers:
            r_headers["ETag"] = "xxx"

        if self.headers.get("accept_range") == "none":
            r_headers["Accept-Ranges"] = "none"

        self._respond(200, r_headers)


@contextlib.contextmanager
def serve():
    server_address = ("", 0)
    httpd = HTTPServer(server_address, HTTPTestHandler)
    th = threading.Thread(target=httpd.serve_forever)
    th.daemon = True
    th.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.socket.close()
        httpd.shutdown()
        th.join()


@pytest.fixture(scope="module")
def server():
    with serve() as s:
        server = SimpleNamespace(address=s, realfile=_make_realfile(s))
        yield server
