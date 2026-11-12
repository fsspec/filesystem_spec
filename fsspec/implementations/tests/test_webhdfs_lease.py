"""Writing to WebHDFS while the datanode still holds the lease of the file.

The server below is a minimal WebHDFS (namenode redirecting to datanode). Like
HDFS, its datanode rejects with a 403 ``AlreadyBeingCreatedException`` the
writes that arrive while the lease of the previous write is still held. To keep
the tests deterministic the lease is not timed: after every write it rejects the
next ``lease_held_for`` APPEND attempts.
"""

import json
import math
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

import fsspec

requests = pytest.importorskip("requests")


class FakeHDFS:
    def __init__(self):
        self.files = {}
        self.leases = {}
        self.lease_held_for = 2
        self.appends = 0
        self.rejections = 0
        self.denied = False
        self.lock = threading.Lock()


def remote_exception(exception, message):
    return {"RemoteException": {"exception": exception, "message": message}}


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _body(self):
            length = int(self.headers.get("content-length", 0))
            return self.rfile.read(length) if length else b""

        def _send(self, status, payload=None, headers=None):
            body = json.dumps(payload).encode() if payload is not None else b""
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle(self):
            url = urlparse(self.path)
            query = parse_qs(url.query)
            operation = query["op"][0]
            if url.path.startswith("/webhdfs/v1"):
                # Namenode: redirect the write, with its parameters, to the datanode.
                port = self.server.server_address[1]
                location = f"http://127.0.0.1:{port}/dn{url.path[len('/webhdfs/v1') :]}"
                self._send(307, headers={"Location": f"{location}?{url.query}"})
                return
            path = url.path[len("/dn") :]
            data = self._body()
            with state.lock:
                if operation == "APPEND":
                    state.appends += 1
                    if state.denied:
                        self._send(
                            403,
                            remote_exception(
                                "AccessControlException", "Permission denied"
                            ),
                        )
                        return
                    if state.leases.get(path, 0) > 0:
                        state.leases[path] -= 1
                        state.rejections += 1
                        self._send(
                            403,
                            remote_exception(
                                "AlreadyBeingCreatedException",
                                "Failed to APPEND_FILE: lease held",
                            ),
                        )
                        return
                    state.files[path] += data
                else:
                    if path in state.files and query["overwrite"] == ["false"]:
                        self._send(
                            403,
                            remote_exception(
                                "FileAlreadyExistsException", f"{path} already exists"
                            ),
                        )
                        return
                    state.files[path] = bytearray(data)
                state.leases[path] = state.lease_held_for
            self._send(200)

        do_PUT = do_POST = _handle

    return Handler


@pytest.fixture
def hdfs():
    state = FakeHDFS()
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.port = server.server_address[1]
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def delays(monkeypatch):
    """The waits between retries, recorded instead of slept."""
    waited = []
    monkeypatch.setattr(time, "sleep", waited.append)
    return waited


def webhdfs(state, **kwargs):
    return fsspec.filesystem(
        "webhdfs",
        host="127.0.0.1",
        port=state.port,
        user="test",
        skip_instance_cache=True,
        **kwargs,
    )


def test_many_blocks_survive_a_held_lease(hdfs, delays):
    fs = webhdfs(hdfs)
    payload = bytes(range(256)) * 400  # 100 KiB in ten blocks of 10 KiB
    with fs.open("/lake/table.parquet", "wb", block_size=10 * 1024) as f:
        for start in range(0, len(payload), 10 * 1024):
            f.write(payload[start : start + 10 * 1024])
    assert bytes(hdfs.files["/lake/table.parquet"]) == payload
    # The first block travelled in the CREATE request; each of the other nine
    # was rejected twice before its APPEND went through.
    assert hdfs.rejections == 9 * 2
    assert hdfs.appends == 9 * 3
    assert delays == [0.05, 0.1] * 9


def test_small_file_is_one_request(hdfs, delays):
    fs = webhdfs(hdfs)
    with fs.open("/lake/_SUCCESS", "wb") as f:
        f.write(b"INE-2026")
    assert bytes(hdfs.files["/lake/_SUCCESS"]) == b"INE-2026"
    assert hdfs.appends == 0  # data travelled in the CREATE request


def test_empty_file(hdfs, delays):
    fs = webhdfs(hdfs)
    with fs.open("/lake/empty", "wb"):
        pass
    assert bytes(hdfs.files["/lake/empty"]) == b""


def test_overwrite_replaces_content(hdfs, delays):
    fs = webhdfs(hdfs)
    with fs.open("/lake/f", "wb") as f:
        f.write(b"long old content")
    with fs.open("/lake/f", "wb") as f:
        f.write(b"new")
    assert bytes(hdfs.files["/lake/f"]) == b"new"


def test_exclusive_create_keeps_an_existing_file(hdfs, delays):
    fs = webhdfs(hdfs)
    with fs.open("/lake/new", "xb") as f:
        f.write(b"first")
    assert bytes(hdfs.files["/lake/new"]) == b"first"
    f = fs.open("/lake/new", "xb")
    f.write(b"second")
    with pytest.raises(FileExistsError):
        f.close()
    f.closed = True
    assert bytes(hdfs.files["/lake/new"]) == b"first"


def test_other_rejections_are_not_retried(hdfs, delays):
    fs = webhdfs(hdfs)
    f = fs.open("/lake/f", "wb", block_size=1024)
    f.write(b"x" * 4096)  # the first block creates the file
    hdfs.denied = True
    with pytest.raises(Exception) as error:
        f.write(b"x" * 4096)
    f.closed = True
    assert "403" in str(error.value)
    assert hdfs.appends == 1  # no retry for an AccessControlException
    assert delays == []


def test_gives_up_after_the_retries(hdfs, delays, monkeypatch):
    from fsspec.implementations.webhdfs import WebHDFile

    monkeypatch.setattr(WebHDFile, "append_retries", 2)
    hdfs.lease_held_for = math.inf  # the lease is never released
    fs = webhdfs(hdfs)
    f = fs.open("/lake/f", "wb", block_size=1024)
    f.write(b"x" * 4096)  # the first block creates the file
    with pytest.raises(Exception) as error:
        f.write(b"x" * 4096)  # its lease is still held
    f.closed = True
    assert "403" in str(error.value)
    assert hdfs.appends == 3  # the first attempt and two retries
    assert delays == [0.05, 0.1]
