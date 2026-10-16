"""DBFS error handling without a live service or recorded credentials."""

import json
from unittest.mock import Mock, call

import pytest

requests = pytest.importorskip("requests")

from fsspec.implementations.dbfs import DatabricksException, DatabricksFileSystem  # noqa: E402


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


@pytest.mark.parametrize("partial_responses", [0, 1, 2])
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
