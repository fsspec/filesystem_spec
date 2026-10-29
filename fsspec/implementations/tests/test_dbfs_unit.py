"""DBFS error handling without a live service or recorded credentials."""

import base64
import json
from unittest.mock import Mock, call

import pytest

requests = pytest.importorskip("requests")

from fsspec.implementations.dbfs import (  # noqa: E402
    DatabricksException,
    DatabricksFileSystem,
)


def response(error_code=None):
    result = requests.Response()
    result.status_code = 503 if error_code else 200
    result._content = json.dumps(
        {"error_code": error_code, "message": error_code} if error_code else {}
    ).encode()
    return result


@pytest.fixture
def fs():
    filesystem = DatabricksFileSystem(
        instance="example.invalid", token="test-token", skip_instance_cache=True
    )
    yield filesystem
    filesystem.session.close()


@pytest.mark.parametrize(
    "error_code", ["PERMISSION_DENIED", "INTERNAL_ERROR", "IO_ERROR"]
)
def test_makedirs_preserves_status_error(fs, monkeypatch, error_code):
    get = Mock(return_value=response(error_code))
    post = Mock(return_value=response())
    monkeypatch.setattr(fs.session, "get", get)
    monkeypatch.setattr(fs.session, "post", post)
    fs.dircache["/parent"] = []

    with pytest.raises(DatabricksException, match=error_code) as exc:
        fs.makedirs("/parent/directory", exist_ok=False)

    assert exc.value.error_code == error_code
    post.assert_not_called()
    assert "/parent" in fs.dircache


@pytest.mark.parametrize("exist_ok", [False, True])
def test_makedirs_missing_path(fs, monkeypatch, exist_ok):
    get = Mock(return_value=response("RESOURCE_DOES_NOT_EXIST"))
    post = Mock(return_value=response())
    monkeypatch.setattr(fs.session, "get", get)
    monkeypatch.setattr(fs.session, "post", post)
    fs.dircache["/parent"] = []

    fs.makedirs("/parent/directory", exist_ok=exist_ok)

    assert get.call_count == (0 if exist_ok else 1)
    post.assert_called_once_with(
        "https://example.invalid/api/2.0/dbfs/mkdirs",
        json={"path": "/parent/directory"},
    )
    assert "/parent" not in fs.dircache


def test_makedirs_existing_path(fs, monkeypatch):
    monkeypatch.setattr(fs.session, "get", Mock(return_value=response()))
    post = Mock(return_value=response())
    monkeypatch.setattr(fs.session, "post", post)

    with pytest.raises(FileExistsError, match="already exists"):
        fs.makedirs("/parent/directory", exist_ok=False)

    post.assert_not_called()


@pytest.mark.parametrize("partial_responses", [0, 1, 2, 1100])
def test_rm_partial_delete_completes_and_invalidates_cache(
    fs, monkeypatch, partial_responses
):
    post = Mock(
        side_effect=[response("PARTIAL_DELETE") for _ in range(partial_responses)]
        + [response()]
    )
    monkeypatch.setattr(fs.session, "post", post)
    fs.dircache["/parent"] = [{"name": "/parent/directory", "type": "directory"}]

    assert fs.rm("/parent/directory", recursive=True) is None

    assert "/parent" not in fs.dircache
    assert post.call_args_list == [
        call(
            "https://example.invalid/api/2.0/dbfs/delete",
            json={"path": "/parent/directory", "recursive": True},
        )
    ] * (partial_responses + 1)


@pytest.mark.parametrize("partial_responses", [0, 1])
@pytest.mark.parametrize("error_code", ["IO_ERROR", "PERMISSION_DENIED"])
def test_rm_preserves_terminal_error(fs, monkeypatch, partial_responses, error_code):
    post = Mock(
        side_effect=[response("PARTIAL_DELETE") for _ in range(partial_responses)]
        + [response(error_code)]
    )
    monkeypatch.setattr(fs.session, "post", post)
    expected = OSError if error_code == "IO_ERROR" else DatabricksException

    with pytest.raises(expected, match=error_code):
        fs.rm("/parent/directory", recursive=True)

    assert post.call_count == partial_responses + 1


@pytest.mark.parametrize("mode,overwrite", [("wb", True), ("xb", False)])
@pytest.mark.parametrize(
    "data",
    [b"", b"new contents", b"x" * (2**20 + 1)],
    ids=["empty", "small", "multi-block"],
)
def test_open_write_mode_controls_overwrite(fs, monkeypatch, mode, overwrite, data):
    created = requests.Response()
    created.status_code = 200
    created._content = b'{"handle": 1}'
    post = Mock(
        side_effect=lambda url, json: created if url.endswith("/create") else response()
    )
    monkeypatch.setattr(fs.session, "post", post)

    with fs.open("/file", mode) as file:
        file.write(data)

    assert post.call_args_list[0] == call(
        "https://example.invalid/api/2.0/dbfs/create",
        json={"path": "/file", "overwrite": overwrite},
    )
    assert post.call_args_list[-1] == call(
        "https://example.invalid/api/2.0/dbfs/close", json={"handle": 1}
    )
    uploaded = b"".join(
        base64.b64decode(request.kwargs["json"]["data"])
        for request in post.call_args_list
        if request.args[0].endswith("/add-block")
    )
    assert uploaded == data


@pytest.mark.parametrize(
    "data",
    [b"", b"new contents", b"x" * (2**20 + 1)],
    ids=["empty", "small", "multi-block"],
)
def test_exclusive_create_existing_file_stops_before_upload(fs, monkeypatch, data):
    def create(url, json):
        if url.endswith("/create") and not json["overwrite"]:
            return response("RESOURCE_ALREADY_EXISTS")
        result = response()
        result._content = b'{"handle": 1}'
        return result

    post = Mock(side_effect=create)
    monkeypatch.setattr(fs.session, "post", post)

    with pytest.raises(FileExistsError, match="RESOURCE_ALREADY_EXISTS"):
        with fs.open("/existing", "xb") as file:
            file.write(data)

    assert post.call_count == 1


@pytest.fixture
def retry_server(monkeypatch):
    """Exercise the real adapter retry loop against a local HTTP server."""
    import socket
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import urlsplit

    state = {"calls": [], "failures": 2, "json": True, "sleeps": []}

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            state["calls"].append((self.command, self.path))
            status = 429 if len(state["calls"]) <= state["failures"] else 200
            body = json.dumps(
                {"error_code": "RATE_LIMIT", "message": "slow down"}
                if status == 429
                else {}
            ).encode()
            if status == 429 and not state["json"]:
                body = b"slow down"
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(body)

        do_GET = handle_request
        do_POST = handle_request

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    original_connect = socket.socket.connect

    def connect(sock, address):
        assert address[0] == "127.0.0.1"
        return original_connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", connect)
    original_send = requests.adapters.HTTPAdapter.send

    def send(adapter, request, **kwargs):
        # Session already selected its production HTTPS adapter. Change only the
        # test destination, retaining the selected adapter's real retry policy.
        parsed = urlsplit(request.url)
        assert parsed.hostname == "example.invalid"
        request.url = f"http://127.0.0.1:{server.server_port}{parsed.path}"
        kwargs["proxies"] = {}
        kwargs["timeout"] = 5
        return original_send(adapter, request, **kwargs)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    monkeypatch.setattr("urllib3.util.retry.time.sleep", state["sleeps"].append)
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("json_response", [False, True])
def test_mkdirs_retries_rate_limit(fs, retry_server, json_response):
    retry_server["json"] = json_response
    fs.makedirs("/parent/directory")
    assert retry_server["calls"] == [("POST", "/api/2.0/dbfs/mkdirs")] * 3
    assert retry_server["sleeps"] == [1, 1]


@pytest.mark.parametrize("json_response", [False, True])
def test_mkdirs_retry_exhaustion_preserves_error(fs, retry_server, json_response):
    retry_server.update(failures=100, json=json_response)
    adapter = fs.session.get_adapter("https://example.invalid/api/2.0/dbfs/mkdirs")
    assert adapter.max_retries.total == 10
    adapter.max_retries = adapter.max_retries.new(total=2)
    expected = DatabricksException if json_response else requests.HTTPError
    with pytest.raises(expected, match="slow down|429"):
        fs.makedirs("/parent/directory")
    assert len(retry_server["calls"]) == 3
    assert retry_server["sleeps"] == [1, 1]


def test_other_post_not_retried(fs, retry_server):
    with pytest.raises(DatabricksException, match="slow down"):
        fs._send_to_api("post", "add-block", {"handle": 1, "data": "YQ=="})
    assert retry_server["calls"] == [("POST", "/api/2.0/dbfs/add-block")]
    assert retry_server["sleeps"] == []
    assert "POST" not in fs.retries.allowed_methods


def test_get_retry_unchanged(fs, retry_server):
    fs._send_to_api("get", "list", {"path": "/parent"})
    assert retry_server["calls"] == [("GET", "/api/2.0/dbfs/list")] * 3
    assert retry_server["sleeps"] == [1, 1]
