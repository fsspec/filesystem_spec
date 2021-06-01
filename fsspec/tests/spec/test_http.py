import contextlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from fsspec.implementations.http import HTTPFileSystem
from fsspec.tests.base import BaseFSTests, BaseReadTests

requests = pytest.importorskip("requests")
port = 9898


class HTTPTestHandler(BaseHTTPRequestHandler):
    _FILES = {"/exists"}

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
        if self.path.rstrip("/") not in self._FILES:
            self._respond(404)
            return

        self._respond(200, data=b"data in /exists")

    def do_HEAD(self):
        if "head_ok" not in self.headers:
            self._respond(405)
            return
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
def prefix():
    return f"http://localhost:{port}"


@pytest.fixture
def fs(server):
    return HTTPFileSystem()


class TestFS(BaseFSTests):
    pass


class TestRead(BaseReadTests):
    pass
