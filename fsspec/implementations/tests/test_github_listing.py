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
        "other": [
            {
                "path": "README.md",
                "mode": "040000",
                "type": "tree",
                "sha": "other-readme-tree",
            },
        ],
        "other-readme-tree": [
            {
                "path": "child",
                "mode": "100644",
                "type": "blob",
                "size": 1,
                "sha": "child",
            },
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


@pytest.mark.parametrize(
    "path",
    [
        "README.md/child",
        "data/notes.txt/child",
        "README.md/child/grandchild",
        "README.md//child",
    ],
)
@pytest.mark.parametrize("protocol", ["", "github://", "github://example:repo@main/"])
def test_path_below_file_is_missing(github_tree, path, protocol):
    requested = protocol + path
    for _ in range(2):
        for detail in (False, True):
            with pytest.raises(FileNotFoundError) as exc:
                github_tree.ls(requested, detail=detail)
            assert exc.value.args == (path,)

        with pytest.raises(FileNotFoundError):
            github_tree.info(requested)
        assert not github_tree.exists(requested)
        assert not github_tree.isdir(requested)
        assert not github_tree.isfile(requested)


@pytest.mark.parametrize("path", ["README.md/", "data/notes.txt/"])
@pytest.mark.parametrize("protocol", ["", "github://", "github://example:repo@main/"])
def test_ls_file_with_trailing_slash(github_tree, path, protocol):
    requested = protocol + path
    assert github_tree.ls(requested) == [path.rstrip("/")]
    assert github_tree.ls(requested, detail=True) == [github_tree.info(requested)]


def test_path_below_file_uses_requested_revision(github_tree):
    path = "README.md/child"
    for _ in range(2):
        with pytest.raises(FileNotFoundError):
            github_tree.ls(path)
        assert github_tree.ls(path, sha="other") == [path]
        info = github_tree.info(path, sha="other")
        assert info["name"] == path
        assert info["type"] == "file"
        assert not github_tree.exists(path)
