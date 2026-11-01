import base64
import json
from unittest.mock import Mock
from urllib.parse import parse_qs, quote, unquote, urlsplit

import pytest
import requests

from fsspec.implementations.github import GithubFileSystem


@pytest.fixture
def github_contents(monkeypatch, request):
    ref = getattr(request, "param", "main")
    files = {}
    calls = []

    def send(session, request, **kwargs):
        calls.append(request)
        url = urlsplit(request.url)
        response = requests.Response()
        response.status_code = 200
        response.url = request.url
        if url.path == "/repos/example/repo":
            body = {"default_branch": ref}
        elif url.path.startswith("/repos/example/repo/git/trees/"):
            assert url.path == "/repos/example/repo/git/trees/" + quote(ref, safe="")
            assert not url.query
            assert not url.fragment
            body = {"tree": []}
        else:
            prefix = "/repos/example/repo/contents/"
            assert url.path.startswith(prefix)
            assert parse_qs(url.query) == {"ref": [ref]}
            assert not url.fragment
            path = unquote(url.path[len(prefix) :])
            if path not in files:
                response.status_code = 404
                body = {"message": "Not Found"}
            elif request.method == "DELETE":
                payload = json.loads(request.body)
                assert payload["sha"] == "file-sha"
                assert payload["branch"] == ref
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
        sha=ref,
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


@pytest.mark.parametrize(
    "github_contents",
    ["release#1", "release&ref=other", "release%23", "feature/topic", "café"],
    indirect=True,
)
def test_reserved_ref_characters(github_contents):
    fs, files, calls = github_contents
    files["file.txt"] = b"requested branch"
    assert fs.cat_file("file.txt") == b"requested branch"
    fs.rm_file("file.txt")
    assert "file.txt" not in files
    assert [call.method for call in calls] == ["GET", "GET", "GET", "DELETE"]


@pytest.mark.parametrize("github_contents", ["release#1"], indirect=True)
def test_default_branch_reserved_characters(github_contents):
    _, files, calls = github_contents
    files["file.txt"] = b"default branch"
    fs = GithubFileSystem(org="example", repo="repo", skip_instance_cache=True)
    assert fs.root == "release#1"
    assert fs.cat_file("file.txt") == b"default branch"


@pytest.mark.parametrize("github_contents", ["release#1"], indirect=True)
def test_explicit_ref_reserved_characters(github_contents):
    fs, files, _ = github_contents
    files["file.txt"] = b"explicit branch"
    ref = fs.root
    fs.root = "main"
    fs.invalidate_cache()
    assert fs.ls("", sha=ref) == []
    assert fs.cat_file("file.txt", sha=ref) == b"explicit branch"


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


@pytest.mark.parametrize("content", [b"", b"file contents", b"\x00\xff\x80"])
def test_open_inline_file_without_http(github_contents, content):
    fs, files, calls = github_contents
    files["file.txt"] = content
    fs.http_fs = None

    with fs.open("file.txt", "rb") as stream:
        assert stream.read() == content
        assert stream.size == len(content)
    assert len(calls) == 2


@pytest.mark.parametrize(
    "content,encoding",
    [(b"", "none"), (b"version https://git-lfs.github.com/spec/v1\n", "base64")],
)
@pytest.mark.parametrize("http_available", [True, False])
def test_open_download_fallback(
    github_contents, monkeypatch, content, encoding, http_available
):
    fs, _, _ = github_contents
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(
        {
            "content": base64.b64encode(content).decode(),
            "encoding": encoding,
            "download_url": "https://example.com/download",
        }
    ).encode()
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)
    if not http_available:
        fs.http_fs = None
        with pytest.raises(ImportError, match=r"fsspec\[http\]"):
            fs._open("file.txt", block_size=32)
        return
    fs.http_fs = Mock()

    assert fs._open("file.txt", block_size=32) is fs.http_fs.open.return_value
    fs.http_fs.open.assert_called_once_with(
        "https://example.com/download", mode="rb", block_size=32, cache_options=None
    )
