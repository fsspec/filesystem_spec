import gzip
import io
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from fsspec.asyn import sync
from fsspec.callbacks import Callback
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.http_sync import HTTPFileSystem as SyncHTTPFileSystem
from fsspec.implementations.http_sync import unregister as unregister_sync_http

# Importing the synchronous implementation registers it as the default HTTP
# backend. Restore the normal async registration so this module is isolated.
unregister_sync_http()

DATA = bytes(range(64))


@pytest.fixture(scope="module")
def range_server():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.respond(head=True)

        def do_GET(self):
            self.respond(head=False)

        def respond(self, head):
            requests.append((self.command, self.path, dict(self.headers)))
            data = b"" if self.path == "/empty" else DATA
            if self.path == "/missing":
                self.send_response(404)
                self.end_headers()
                return
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/data")
                self.end_headers()
                return
            if head:
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Accept-Ranges", "bytes")
                self.end_headers()
                return
            value = self.headers.get("Range")
            if value is None or self.path == "/ignore":
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", value)
            if match is None:
                self.send_response(400)
                self.end_headers()
                return
            first = int(match[1])
            last = min(int(match[2]) if match[2] else len(data) - 1, len(data) - 1)
            if first >= len(data):
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(data)}")
                self.end_headers()
                return
            body = data[first : last + 1]
            if self.path == "/shift":
                first += 1
                body = data[first : last + 1]
            self.send_response(206)
            if self.path != "/missing-range":
                total = "*" if self.path == "/unknown" else str(len(data))
                self.send_header("Content-Range", f"bytes {first}-{last}/{total}")
            short = self.path == "/short" or (self.path == "/flaky" and first == 0)
            if short:
                body = body[:3]
            elif self.path == "/long":
                body += b"extra"
            elif self.path == "/encoded":
                self.send_header("Content-Encoding", "gzip")
                body = gzip.compress(body)
            else:
                length = len(body) + 1 if self.path == "/bad-length" else len(body)
                self.send_header("Content-Length", str(length))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(params=["aiohttp", "requests"])
def http_fs(request):
    fs = HTTPFileSystem() if request.param == "aiohttp" else SyncHTTPFileSystem()
    try:
        yield fs
    finally:
        if isinstance(fs, HTTPFileSystem):
            if fs._session is not None:
                sync(fs.loop, fs._session.close)
        else:
            fs.session.close()


@pytest.mark.parametrize(
    "start,end",
    [
        (None, None),
        (0, None),
        (3, 19),
        (None, 8),
        (11, None),
        (-8, None),
        (None, -4),
        (-20, -3),
        (-100, 100),
        (100, None),
        (0, 0),
        (7, 7),
        (9, 2),
    ],
)
def test_http_byte_ranges(http_fs, range_server, tmp_path, start, end):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(b"previous")
    callback = Callback()
    http_fs.get_file(
        url + "/data", target, start=start, end=end, chunk_size=7, callback=callback
    )
    assert target.read_bytes() == DATA[start:end]
    assert callback.size == callback.value == len(DATA[start:end])


@pytest.mark.parametrize("prefix", [None, 0, 5, len(DATA)])
def test_http_resume_requests_only_remaining_bytes(
    http_fs, range_server, tmp_path, prefix
):
    url, requests = range_server
    target = tmp_path / "download"
    if prefix is not None:
        target.write_bytes(DATA[:prefix])
    callback = Callback()
    http_fs.get_file(url + "/data", target, resume=True, callback=callback)
    assert target.read_bytes() == DATA
    assert callback.value == callback.size == len(DATA)
    assert requests[-1][2]["Range"] == f"bytes={prefix or 0}-"


@pytest.mark.parametrize("end", [24, -4, 100])
def test_http_resume_with_end(http_fs, range_server, tmp_path, end):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(DATA[:5])
    callback = Callback()
    http_fs.get_file(url + "/data", target, resume=True, end=end, callback=callback)
    assert target.read_bytes() == DATA[:end]
    assert callback.size == callback.value == len(DATA[:end])


@pytest.mark.parametrize(
    "endpoint,match",
    [
        ("ignore", "does not support"),
        ("shift", "start offset"),
        ("missing-range", "Content-Range"),
        ("bad-length", "Content-Length"),
        ("encoded", "identity"),
    ],
)
def test_bad_http_ranges_preserve_partial_file(
    http_fs, range_server, tmp_path, endpoint, match
):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(DATA[:5])
    with pytest.raises(ValueError, match=match):
        http_fs.get_file(url + "/" + endpoint, target, resume=True)
    assert target.read_bytes() == DATA[:5]


def test_ignored_range_does_not_truncate_existing_file(http_fs, range_server, tmp_path):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(b"keep")
    with pytest.raises(ValueError, match="does not support"):
        http_fs.get_file(url + "/ignore", target, start=5, end=12)
    assert target.read_bytes() == b"keep"


@pytest.mark.parametrize("endpoint", ["data", "ignore"])
def test_new_resume_can_start_with_a_full_response(
    http_fs, range_server, tmp_path, endpoint
):
    url, _ = range_server
    target = tmp_path / "download"
    http_fs.get_file(url + "/" + endpoint, target, resume=True)
    assert target.read_bytes() == DATA


def test_interrupted_http_download_is_resumable(http_fs, range_server, tmp_path):
    url, requests = range_server
    target = tmp_path / "download"
    with pytest.raises(OSError, match="Incomplete download"):
        http_fs.get_file(url + "/flaky", target, resume=True)
    assert target.read_bytes() == DATA[:3]
    http_fs.get_file(url + "/flaky", target, resume=True)
    assert requests[-1][2]["Range"] == "bytes=3-"
    assert target.read_bytes() == DATA


def test_oversized_http_response_does_not_append_invalid_bytes(
    http_fs, range_server, tmp_path
):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(DATA[:5])
    with pytest.raises(OSError, match="more bytes"):
        http_fs.get_file(url + "/long", target, resume=True, chunk_size=7)
    assert DATA.startswith(target.read_bytes())


def test_local_file_larger_than_remote_is_preserved(http_fs, range_server, tmp_path):
    url, _ = range_server
    target = tmp_path / "download"
    original = DATA + b"extra"
    target.write_bytes(original)
    with pytest.raises(ValueError, match="larger than the remote"):
        http_fs.get_file(url + "/data", target, resume=True)
    assert target.read_bytes() == original


@pytest.mark.parametrize("resume", [False, True])
def test_empty_http_file(http_fs, range_server, tmp_path, resume):
    url, _ = range_server
    target = tmp_path / "download"
    http_fs.get_file(url + "/empty", target, resume=resume)
    assert target.read_bytes() == b""


def test_http_headers_are_preserved_without_mutation(http_fs, range_server, tmp_path):
    url, requests = range_server
    target = tmp_path / "download"
    headers = {"X-Download-Test": "yes", "accept-encoding": "gzip"}
    http_fs.get_file(url + "/data", target, start=3, headers=headers)
    assert headers == {"X-Download-Test": "yes", "accept-encoding": "gzip"}
    assert requests[-1][2]["X-Download-Test"] == "yes"
    assert requests[-1][2]["Accept-Encoding"] == "identity"
    assert target.read_bytes() == DATA[3:]
    with pytest.raises(ValueError, match="Range header"):
        http_fs.get_file(url + "/data", target, start=3, headers={"range": "bytes=4-"})
    assert target.read_bytes() == DATA[3:]


def test_http_filelike_destination_is_not_closed(http_fs, range_server):
    url, _ = range_server
    target = io.BytesIO(b"prefix:")
    target.seek(0, 2)
    http_fs.get_file(url + "/data", target, start=3, end=8)
    assert target.getvalue() == b"prefix:" + DATA[3:8]
    assert not target.closed
    with pytest.raises(ValueError, match="filename"):
        http_fs.get_file(url + "/data", target, resume=True)


@pytest.mark.parametrize("endpoint", ["unknown", "redirect"])
def test_http_unknown_total_and_redirects(http_fs, range_server, tmp_path, endpoint):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(DATA[:5])
    http_fs.get_file(url + "/" + endpoint, target, resume=True)
    assert target.read_bytes() == DATA


def test_http_missing_source_preserves_destination(http_fs, range_server, tmp_path):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(b"keep")
    with pytest.raises(FileNotFoundError):
        http_fs.get_file(url + "/missing", target, resume=True)
    assert target.read_bytes() == b"keep"


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_http_invalid_chunk_size_is_rejected(
    http_fs, range_server, tmp_path, chunk_size
):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(b"keep")
    with pytest.raises(ValueError, match="chunk_size"):
        http_fs.get_file(url + "/data", target, start=0, chunk_size=chunk_size)
    assert target.read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_native_async_http_and_batched_downloads(range_server, tmp_path):
    url, requests = range_server
    fs = HTTPFileSystem(asynchronous=True)
    first, second = tmp_path / "first", tmp_path / "second"
    first.write_bytes(DATA[:5])
    second.write_bytes(DATA[:9])
    try:
        await fs._get(
            [url + "/data", url + "/data"],
            [str(first), str(second)],
            resume=True,
        )
        assert first.read_bytes() == second.read_bytes() == DATA
        assert {entry[2]["Range"] for entry in requests[-2:]} == {
            "bytes=5-", "bytes=9-"
        }
        target = io.BytesIO()
        await fs._get_file(url + "/data", target, start=-8, end=-2)
        assert target.getvalue() == DATA[-8:-2]
        assert not target.closed
    finally:
        if fs._session is not None:
            await fs._session.close()


@pytest.mark.parametrize("chunk_size", [1.5, "3"])
def test_http_noninteger_chunk_size_preserves_destination(
    http_fs, range_server, tmp_path, chunk_size
):
    url, _ = range_server
    target = tmp_path / "download"
    target.write_bytes(b"keep")
    with pytest.raises(TypeError):
        http_fs.get_file(url + "/data", target, start=0, chunk_size=chunk_size)
    assert target.read_bytes() == b"keep"
