import sys

import pytest

import fsspec
from fsspec.implementations.gist import GistFileSystem

if (3, 12) < sys.version_info < (3, 14):
    pytest.skip("Too many tests bust rate limit", allow_module_level=True)


@pytest.mark.parametrize(
    "gist_id,sha",
    [("2656908684d3965b80c2", "2fb2f12f332f7e242b1a2af1f41e30ddf99f24c7")],
)
def test_gist_public_all_files(gist_id, sha):
    fs = fsspec.filesystem("gist", gist_id=gist_id, sha=sha)
    # Listing
    all_files = fs.ls("")
    assert len(all_files) == 2
    # Cat
    data = fs.cat(all_files)
    assert set(data.keys()) == set(all_files)
    for v in data.values():
        assert isinstance(v, bytes)


@pytest.mark.parametrize(
    "gist_id,sha,file",
    [
        (
            "2656908684d3965b80c2",
            "2fb2f12f332f7e242b1a2af1f41e30ddf99f24c7",
            "distributed_error_logs_PY3_7-3-2016",
        )
    ],
)
def test_gist_public_one_file(gist_id, sha, file):
    fs = fsspec.filesystem("gist", gist_id=gist_id, sha=sha, filenames=[file])
    # Listing
    all_files = fs.ls("")
    assert len(all_files) == 1
    # Cat
    data = fs.cat(all_files)
    assert set(data.keys()) == set(all_files)
    for v in data.values():
        assert isinstance(v, bytes)


@pytest.mark.parametrize(
    "gist_id,sha,file",
    [
        (
            "2656908684d3965b80c2",
            "2fb2f12f332f7e242b1a2af1f41e30ddf99f24c7",
            "file-that-doesnt-exist.py",
        )
    ],
)
def test_gist_public_missing_file(gist_id, sha, file):
    with pytest.raises(FileNotFoundError):
        fsspec.filesystem("gist", gist_id=gist_id, sha=sha, filenames=[file])


@pytest.mark.parametrize(
    "gist_id,sha,file,token,user",
    [
        ("gist-id-123", "sha_hash_a0b1", "a_file.txt", "secret_token", "my-user"),
        ("gist-id-123", "sha_hash_a0b1", "a_file.txt", "secret_token", ""),  # No user
        ("gist-id-123", "", "a_file.txt", "secret_token", "my-user"),  # No SHA
    ],
)
def test_gist_url_parse(gist_id, sha, file, token, user):
    if sha:
        fmt_str = f"gist://{user}:{token}@{gist_id}/{sha}/{file}"
    else:
        fmt_str = f"gist://{user}:{token}@{gist_id}/{file}"

    parsed = GistFileSystem._get_kwargs_from_urls(fmt_str)

    expected = {"gist_id": gist_id, "token": token, "filenames": [file]}
    if user:  # Only include username if it's not empty
        expected["username"] = user
    if sha:  # Only include SHA if it's specified
        expected["sha"] = sha

    assert parsed == expected
