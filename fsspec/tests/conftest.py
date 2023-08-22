import contextlib
import gzip
import json
import os
import threading
from collections import ChainMap
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

requests = pytest.importorskip("requests")
port = 9898
data = b"\n".join([b"some test data"] * 1000)
realfile = "http://127.0.0.1:%i/index/realfile" % port
index = b'<a href="%s">Link</a>' % realfile.encode()
listing = open(
    os.path.join(os.path.dirname(__file__), "data", "listing.html"), "rb"
).read()
win = os.name == "nt"

GLOB_EDGE_CASES_TESTS = {
    "argnames": ("path", "recursive", "maxdepth", "expected"),
    "argvalues": [
        ("fil?1", False, None, ["file1"]),
        ("fil?1", True, None, ["file1"]),
        ("file[1-2]", False, None, ["file1", "file2"]),
        ("file[1-2]", True, None, ["file1", "file2"]),
        ("*", False, None, ["file1", "file2"]),
        (
            "*",
            True,
            None,
            [
                "file1",
                "file2",
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir0/nesteddir/nestedfile",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        ("*", True, 1, ["file1", "file2"]),
        (
            "*",
            True,
            2,
            [
                "file1",
                "file2",
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir1/subfile1",
                "subdir1/subfile2",
            ],
        ),
        ("*1", False, None, ["file1"]),
        (
            "*1",
            True,
            None,
            [
                "file1",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        ("*1", True, 2, ["file1", "subdir1/subfile1", "subdir1/subfile2"]),
        (
            "**",
            False,
            None,
            [
                "file1",
                "file2",
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir0/nesteddir/nestedfile",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        (
            "**",
            True,
            None,
            [
                "file1",
                "file2",
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir0/nesteddir/nestedfile",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        ("**", True, 1, ["file1", "file2"]),
        (
            "**",
            True,
            2,
            [
                "file1",
                "file2",
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir0/nesteddir/nestedfile",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        (
            "**",
            False,
            2,
            [
                "file1",
                "file2",
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir1/subfile1",
                "subdir1/subfile2",
            ],
        ),
        ("**1", False, None, ["file1", "subdir0/subfile1", "subdir1/subfile1"]),
        (
            "**1",
            True,
            None,
            [
                "file1",
                "subdir0/subfile1",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        ("**1", True, 1, ["file1"]),
        (
            "**1",
            True,
            2,
            ["file1", "subdir0/subfile1", "subdir1/subfile1", "subdir1/subfile2"],
        ),
        ("**1", False, 2, ["file1", "subdir0/subfile1", "subdir1/subfile1"]),
        ("**/subdir0", False, None, []),
        ("**/subdir0", True, None, ["subfile1", "subfile2", "nesteddir/nestedfile"]),
        ("**/subdir0/nested*", False, 2, []),
        ("**/subdir0/nested*", True, 2, ["nestedfile"]),
        ("subdir[1-2]", False, None, []),
        ("subdir[1-2]", True, None, ["subfile1", "subfile2", "nesteddir/nestedfile"]),
        ("subdir[1-2]", True, 2, ["subfile1", "subfile2"]),
        ("subdir[0-1]", False, None, []),
        (
            "subdir[0-1]",
            True,
            None,
            [
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir0/nesteddir/nestedfile",
                "subdir1/subfile1",
                "subdir1/subfile2",
                "subdir1/nesteddir/nestedfile",
            ],
        ),
        (
            "subdir[0-1]/*fil[e]*",
            False,
            None,
            [
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir1/subfile1",
                "subdir1/subfile2",
            ],
        ),
        (
            "subdir[0-1]/*fil[e]*",
            True,
            None,
            [
                "subdir0/subfile1",
                "subdir0/subfile2",
                "subdir1/subfile1",
                "subdir1/subfile2",
            ],
        ),
    ],
}


@pytest.fixture
def reset_files():
    yield

    # Reset the newly added files after the
    # test is completed.
    HTTPTestHandler.dynamic_files.clear()


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
        if "give_path" in self.headers:
            return self._respond(200, data=json.dumps({"path": self.path}).encode())
        if file_data is None:
            return self._respond(404)

        status = 200
        content_range = "bytes 0-%i/%i" % (len(file_data) - 1, len(file_data))
        if ("Range" in self.headers) and ("ignore_range" not in self.headers):
            ran = self.headers["Range"]
            b, ran = ran.split("=")
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
        else:
            self._respond(status, data=file_data)

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

        if ("give_length" in self.headers) or ("head_give_length" in self.headers):
            response_headers = {"Content-Length": len(file_data)}
            if "zero_length" in self.headers:
                response_headers["Content-Length"] = 0
            elif "gzip_encoding" in self.headers:
                file_data = gzip.compress(file_data)
                response_headers["Content-Encoding"] = "gzip"
                response_headers["Content-Length"] = len(file_data)

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
        yield "http://127.0.0.1:%i" % port
    finally:
        httpd.socket.close()
        httpd.shutdown()
        th.join()


@pytest.fixture(scope="module")
def server():
    with serve() as s:
        yield s
