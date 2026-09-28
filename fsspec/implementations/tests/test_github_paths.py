import base64
import json
from urllib.parse import parse_qs, quote, unquote, urlsplit

import pytest
import requests

from fsspec.implementations.github import GithubFileSystem


@pytest.fixture
def github_contents(monkeypatch):
    files = {}
    calls = []

    def send(session, request, **kwargs):
        calls.append(request)
        url = urlsplit(request.url)
        response = requests.Response()
        response.status_code = 200
        response.url = request.url
        if url.path == "/repos/example/repo/git/trees/main":
            body = {"tree": []}
        else:
            prefix = "/repos/example/repo/contents/"
            assert url.path.startswith(prefix)
            assert parse_qs(url.query) == {"ref": ["main"]}
            path = unquote(url.path[len(prefix) :])
            if path not in files:
                response.status_code = 404
                body = {"message": "Not Found"}
            elif request.method == "DELETE":
                payload = json.loads(request.body)
                assert payload["sha"] == "file-sha"
                assert payload["branch"] == "main"
                del files[path]
                body = {}
            else:
                body = {
                    "sha": "file-sha",
                    "encoding": "base64",
                    "content": base64.b64encode(files[path]).decode(),
                }
        response._content = json.dumps(body).encode()
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    fs = GithubFileSystem(
        org="example",
        repo="repo",
        sha="main",
        username="user",
        token="test-token",
        skip_instance_cache=True,
    )
    return fs, files, calls


@pytest.mark.parametrize(
    "path",
    [
        "data/report#1.csv",
        "data/query?.csv",
        "data/literal%23.csv",
        "data/literal%2F.csv",
        "data/a b.csv",
        "data/café.csv",
        "data/plain.csv",
    ],
)
def test_open_reserved_path_characters(github_contents, path):
    fs, files, calls = github_contents
    files[path] = b"requested file"
    files["data/report"] = b"different file"
    assert fs.cat_file(path) == b"requested file"
    assert urlsplit(calls[-1].url).path == "/repos/example/repo/contents/" + quote(path)
    assert not urlsplit(calls[-1].url).fragment


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize(
    "path", ["data/report#1.csv", "data/query?.csv", "data/literal%23.csv"]
)
def test_remove_reserved_path_characters(github_contents, cached, path):
    fs, files, calls = github_contents
    files[path] = b"requested file"
    files["data/report"] = b"different file"
    if cached:
        fs.dircache["data"] = [{"name": path, "sha": "file-sha"}]
    fs.rm_file(path)
    assert path not in files
    assert files["data/report"] == b"different file"
    assert urlsplit(calls[-1].url).path == "/repos/example/repo/contents/" + quote(path)
    assert [call.method for call in calls[1:]] == (
        ["DELETE"] if cached else ["GET", "DELETE"]
    )
    assert not urlsplit(calls[-1].url).fragment
