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
