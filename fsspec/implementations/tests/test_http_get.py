"""HTTP download path mapping without a remote server."""

import asyncio
from pathlib import Path

import pytest

from fsspec.implementations.http import HTTPFileSystem


@pytest.fixture
def http_files(monkeypatch):
    files = {
        "https://example.invalid/file1": b"one",
        "https://example.invalid/file2": b"two",
        "https://example.invalid/file1?token=value": b"query",
    }
    fs = HTTPFileSystem(asynchronous=True, skip_instance_cache=True)

    async def exists(path, **kwargs):
        return path in files

    async def isdir(path):
        return False

    async def find(path, **kwargs):
        return {
            name: {"name": name, "type": "file", "size": len(data)}
            for name, data in files.items()
            if name.startswith(path) and "?" not in name
        }

    async def get_file(rpath, lpath, **kwargs):
        # Deliberately write a real local file while replacing remote I/O only.
        Path(lpath).write_bytes(files[rpath])  # noqa: ASYNC240

    monkeypatch.setattr(fs, "_exists", exists)
    monkeypatch.setattr(fs, "_isdir", isdir)
    monkeypatch.setattr(fs, "_find", find)
    monkeypatch.setattr(fs, "_get_file", get_file)
    return fs


@pytest.mark.parametrize("query", ["", "?token=value"])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("pathlike", [False, True])
def test_http_get_explicit_filename(http_files, tmp_path, query, existing, pathlike):
    destination = tmp_path / "output.txt"
    if existing:
        destination.write_bytes(b"old")
    asyncio.run(
        http_files._get(
            "https://example.invalid/file1" + query,
            destination if pathlike else str(destination),
        )
    )
    assert destination.read_bytes() == (b"query" if query else b"one")


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("source", ["file1", "file[1]", "file*", "file[12]"])
def test_http_get_directory_and_glob(http_files, tmp_path, source, existing):
    destination = tmp_path / "output"
    if existing:
        destination.mkdir()
    # Literal sources need an explicit trailing separator for a new directory.
    lpath = str(destination) + ("/" if source == "file1" else "")
    asyncio.run(http_files._get("https://example.invalid/" + source, lpath))
    expected = {"file1": b"one"}
    if source in ("file*", "file[12]"):
        expected["file2"] = b"two"
    assert {p.name: p.read_bytes() for p in destination.iterdir()} == expected


def test_http_get_explicit_path_lists(http_files, tmp_path):
    destinations = [str(tmp_path / "first"), str(tmp_path / "second")]
    asyncio.run(
        http_files._get(
            [
                "https://example.invalid/file1?token=value",
                "https://example.invalid/file2",
            ],
            destinations,
        )
    )
    assert [Path(p).read_bytes() for p in destinations] == [b"query", b"two"]


def test_http_get_source_list(http_files, tmp_path):
    destination = tmp_path / "output"
    asyncio.run(
        http_files._get(
            ["https://example.invalid/file1", "https://example.invalid/file2"],
            str(destination),
        )
    )
    assert {p.name: p.read_bytes() for p in destination.iterdir()} == {
        "file1": b"one",
        "file2": b"two",
    }


def test_http_glob_magic_is_protocol_specific(http_files):
    from fsspec.asyn import AsyncFileSystem

    assert not hasattr(http_files, "has_magic")
    assert AsyncFileSystem._has_magic("file?")
    assert not HTTPFileSystem._has_magic("file?token=value")
    for pattern in ("file*", "file[12]"):
        assert AsyncFileSystem._has_magic(pattern)
        assert HTTPFileSystem._has_magic(pattern)
