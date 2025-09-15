import sys

import pytest

import fsspec
from fsspec.implementations.gist import GistFileSystem

if sys.version_info[:2] != (3, 12):
    pytest.skip("Too many tests bust rate limit", allow_module_level=True)


@pytest.mark.parametrize(
    "gist_id,sha",
    [("d5d7b521d0e5fec8adfc5652b8f3242c", None)],
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
            "d5d7b521d0e5fec8adfc5652b8f3242c",
            None,
            "ex1.ipynb",
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
            "d5d7b521d0e5fec8adfc5652b8f3242c",
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
