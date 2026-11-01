import base64
import json
from urllib.parse import urlsplit

import pytest
import requests

from fsspec.implementations.github import GithubFileSystem


@pytest.fixture
def github_pages(monkeypatch, tmp_path):
    monkeypatch.setenv("NETRC", str(tmp_path / "netrc"))
    pages = {}
    calls = []

    def send(session, request, **kwargs):
        response = requests.Response()
        response.url = request.url
        if "/git/trees/" in urlsplit(request.url).path:
            status, body, link = 200, {"tree": []}, None
        else:
            calls.append((request, kwargs))
            status, body, link = pages[request.url]
        response.status_code = status
        response._content = json.dumps(body).encode()
        if link is not None:
            response.headers["Link"] = link
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    fs = GithubFileSystem(
        org="example",
        repo="repo",
        sha="main",
        username="user",
        token="test-token",
        timeout=(4, 9),
        skip_instance_cache=True,
    )
    return fs, pages, calls


def list_names(fs, kind):
    if kind == "orgs":
        return fs.repos("example")
    if kind == "users":
        return fs.repos("example", is_org=False)
    return getattr(fs, kind)


def endpoint(kind):
    if kind in ("orgs", "users"):
        return f"https://api.github.com/{kind}/example/repos"
    return f"https://api.github.com/repos/example/repo/{kind}"


@pytest.mark.parametrize("kind", ["tags", "branches", "orgs", "users"])
def test_listing_follows_next_link(github_pages, kind):
    fs, pages, calls = github_pages
    first = endpoint(kind)
    second = first + "?page=2&per_page=2"
    third = first + "?page=3&per_page=2"
    pages[first] = (
        200,
        [{"name": "first"}, {"name": "second"}],
        f'<{second}>; rel="next", <{first}?page=99>; rel="last"',
    )
    pages[second] = (
        200,
        [{"name": "third"}],
        f'<{first}>; rel="prev", <{third}>; rel="next"',
    )
    pages[third] = (
        200,
        [{"name": "fourth"}],
        f'<{first}>; rel="prev", <{first}>; rel="first"',
    )

    assert list_names(fs, kind) == ["first", "second", "third", "fourth"]
    assert [request.url for request, _ in calls] == [first, second, third]
    expected_timeout = (
        fs.timeout if kind in ("tags", "branches") else GithubFileSystem.timeout
    )
    expected_auth = (
        "Basic " + base64.b64encode(b"user:test-token").decode()
        if kind in ("tags", "branches")
        else None
    )
    for request, kwargs in calls:
        assert kwargs["timeout"] == expected_timeout
        assert request.headers.get("Authorization") == expected_auth


@pytest.mark.parametrize("kind", ["tags", "branches", "orgs", "users"])
@pytest.mark.parametrize("names", [[], ["only"]])
def test_listing_without_next_link(github_pages, kind, names):
    fs, pages, calls = github_pages
    url = endpoint(kind)
    pages[url] = (200, [{"name": name} for name in names], None)
    assert list_names(fs, kind) == names
    assert len(calls) == 1


@pytest.mark.parametrize("kind", ["tags", "branches", "orgs", "users"])
def test_later_page_error_is_not_a_partial_listing(github_pages, kind):
    fs, pages, calls = github_pages
    first = endpoint(kind)
    second = first + "?page=2"
    pages[first] = (200, [{"name": "first"}], f'<{second}>; rel="next"')
    pages[second] = (403, {"message": "API rate limit exceeded"}, None)
    with pytest.raises(requests.HTTPError, match="403"):
        list_names(fs, kind)
    assert len(calls) == 2


def test_refs_includes_later_tags_and_branches(github_pages):
    fs, pages, calls = github_pages
    for kind in ("tags", "branches"):
        first = endpoint(kind)
        second = first + "?page=2"
        pages[first] = (200, [{"name": "first"}], f'<{second}>; rel="next"')
        pages[second] = (200, [{"name": "second"}], None)
    assert fs.refs == {
        "tags": ["first", "second"],
        "branches": ["first", "second"],
    }
    assert len(calls) == 4
