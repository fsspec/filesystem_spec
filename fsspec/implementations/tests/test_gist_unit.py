import json

import pytest

pytest.importorskip("requests")

import requests

from fsspec.implementations.gist import GistFileSystem


@pytest.fixture
def gist(monkeypatch):
    def get(url, **kwargs):
        response = requests.Response()
        response.status_code = 200
        if url == "https://api.github.com/gists/example":
            response._content = json.dumps(
                {
                    "files": {
                        "data.txt": {
                            "size": 6,
                            "raw_url": "https://example.com/data.txt",
                        }
                    }
                }
            ).encode()
        else:
            assert url == "https://example.com/data.txt"
            response._content = b"abcdef"
        return response

    monkeypatch.setattr(requests, "get", get)
    return GistFileSystem(gist_id="example", skip_instance_cache=True)


@pytest.mark.parametrize("path", ["data.txt", ["data.txt"], "*.txt"])
@pytest.mark.parametrize(
    "bounds,expected", [({"start": 1, "end": 4}, b"bcd"), ({"start": -2}, b"ef")]
)
def test_cat_ranges(gist, path, bounds, expected):
    assert gist.cat_file("data.txt", **bounds) == expected
    result = gist.cat(path, **bounds)
    if isinstance(path, list) or path == "*.txt":
        assert result == {"data.txt": expected}
    else:
        assert result == expected
