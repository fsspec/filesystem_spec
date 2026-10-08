import json
from urllib.parse import urlsplit

import pytest
import requests

from fsspec.implementations.github import GithubFileSystem


@pytest.fixture
def github_tree(monkeypatch):
    trees = {
        "main": [
            {
                "path": "README.md",
                "mode": "100644",
                "type": "blob",
                "size": 12,
                "sha": "readme",
            },
            {"path": "data", "mode": "040000", "type": "tree", "sha": "data-tree"},
        ],
        "data-tree": [
            {
                "path": "notes.txt",
                "mode": "100644",
                "type": "blob",
                "size": 8,
                "sha": "notes",
            },
        ],
    }

    def send(session, request, **kwargs):
        path = urlsplit(request.url).path
        prefix = "/repos/example/repo/git/trees/"
        assert path.startswith(prefix)
        response = requests.Response()
        response.status_code = 200
        response.url = request.url
        response._content = json.dumps({"tree": trees[path[len(prefix) :]]}).encode()
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    return GithubFileSystem(
        org="example", repo="repo", sha="main", skip_instance_cache=True
    )


@pytest.mark.parametrize("path", ["README.md", "data/notes.txt"])
@pytest.mark.parametrize("protocol", ["", "github://"])
@pytest.mark.parametrize("detail", [False, True])
def test_ls_file(github_tree, path, protocol, detail):
    for _ in range(2):
        entries = github_tree.ls(protocol + path, detail=detail)
        if detail:
            assert [entry["name"] for entry in entries] == [path]
            assert entries[0]["type"] == "file"
            assert entries[0]["size"] == (12 if path == "README.md" else 8)
        else:
            assert entries == [path]


def test_ls_directory(github_tree):
    assert github_tree.ls("") == ["README.md", "data"]
    assert github_tree.ls("data") == ["data/notes.txt"]


@pytest.mark.parametrize("path", ["missing.txt", "data/missing.txt"])
def test_ls_missing_file(github_tree, path):
    with pytest.raises(FileNotFoundError, match=path):
        github_tree.ls(path)
