import io

import pytest

from fsspec.tests.conftest import (
    HTTPTestHandler,
    data,
    reset_files,  # noqa: F401
    server,  # noqa: F401
)


@pytest.fixture(params=["aiohttp", "requests"])
def stream_fs(request):
    if request.param == "aiohttp":
        from fsspec.implementations.http import HTTPFileSystem
    else:
        from fsspec.implementations.http_sync import HTTPFileSystem, unregister

    fs = HTTPFileSystem(skip_instance_cache=True)
    yield fs
    if request.param == "aiohttp" and fs._session is not None:
        fs.close_session(fs.loop, fs._session)
    elif request.param == "requests":
        unregister()


@pytest.mark.parametrize("block_size", [0, 32])
@pytest.mark.parametrize("known_size", [False, True])
@pytest.mark.parametrize("operation", ["readline", "iteration", "readlines"])
def test_http_stream_lines(server, stream_fs, block_size, known_size, operation):
    stream_fs.kwargs["headers"] = {"head_ok": "true"}
    if known_size:
        stream_fs.kwargs["headers"]["give_length"] = "true"
    reference = io.BytesIO(data)
    with stream_fs.open(server.realfile, block_size=block_size) as stream:
        if operation == "readline":
            assert stream.readline() == reference.readline()
            assert stream.tell() == reference.tell()
            assert stream.read(3) == reference.read(3)
            assert stream.readline() == reference.readline()
            assert stream.tell() == reference.tell()
            assert stream.read() == reference.read()
        elif operation == "iteration":
            assert next(stream) == next(reference)
            assert list(stream) == list(reference)
        else:
            assert stream.readlines() == reference.readlines()
            assert stream.readlines() == []
        assert stream.tell() == len(data)


@pytest.mark.parametrize("blocks", [1, 2, 4])
@pytest.mark.parametrize(
    "delimiter,content,expected",
    [
        (b"\n", b"\nfirst\nlast", [b"\n", b"first\n", b"last"]),
        (b"<>", b"first<>second<>last", [b"first<>", b"second<>", b"last"]),
        (b"abab", b"xababtail", [b"xabab", b"tail"]),
        (b"\n", b"", []),
    ],
)
def test_http_stream_delimiter_boundaries(
    server, stream_fs, reset_files, blocks, delimiter, content, expected
):
    HTTPTestHandler.dynamic_files["/stream-lines"] = content
    with stream_fs.open(server.address + "/stream-lines", block_size=0) as stream:
        position = 0
        for value in expected:
            assert stream.readuntil(delimiter, blocks=blocks) == value
            position += len(value)
            assert stream.tell() == position
            assert stream.read(0) == b""
            assert stream.tell() == position
        assert stream.readuntil(delimiter, blocks=blocks) == b""
        assert stream.read() == b""
        assert stream.tell() == len(content)


def test_http_stream_line_lookahead_survives_mixed_reads(server, stream_fs):
    reference = io.BytesIO(data)
    with stream_fs.open(server.realfile, block_size=0) as stream:
        assert stream.readline() == reference.readline()
        buffer = bytearray(31)
        assert stream.readinto(buffer) == len(buffer)
        assert bytes(buffer) == reference.read(len(buffer))
        assert stream.readline() == reference.readline()
        assert stream.read(2**20) == reference.read(2**20)
        assert stream.tell() == reference.tell()
        assert stream.readline() == b""
        assert stream.read() == b""


@pytest.mark.parametrize("method", ["read", "readline", "readuntil", "readinto"])
def test_http_stream_line_lookahead_after_close(server, stream_fs, method):
    with stream_fs.open(server.realfile, block_size=0) as stream:
        assert stream.readline() == data[: data.index(b"\n") + 1]
        position = stream.tell()
        assert stream.read(1) == data[position : position + 1]
        with pytest.raises(ValueError, match="Cannot seek"):
            stream.seek(0)
    with pytest.raises(ValueError, match="closed file"):
        if method == "readinto":
            stream.readinto(bytearray(3))
        else:
            getattr(stream, method)()


def test_http_stream_read_none_after_line_lookahead(server, stream_fs):
    with stream_fs.open(server.realfile, block_size=0) as stream:
        reference = io.BytesIO(data)
        assert stream.readline() == reference.readline()
        assert stream.read(None) == reference.read(None)
        assert stream.tell() == reference.tell()
        assert stream._last_read == stream.leftover == b""
        assert stream.read(None) == b""


def test_http_stream_bounded_rewind(server, stream_fs):
    with stream_fs.open(server.realfile, block_size=0) as stream:
        assert stream.read(8) == data[:8]
        assert stream.read(0) == b""
        assert stream.seek(-3, 1) == 5
        assert stream.seek(3) == 3
        assert stream.read(7) == data[3:10]
        for position, whence in [(0, 0), (1, 1), (0, 2), (-1, 0)]:
            with pytest.raises(ValueError, match="Cannot seek"):
                stream.seek(position, whence)
        assert stream.tell() == 10
        assert stream.read() == data[10:]
        assert stream._last_read == stream.leftover == b""
        with pytest.raises(ValueError, match="Cannot seek"):
            stream.seek(0)
    assert stream._last_read == stream.leftover == b""


def test_http_stream_large_read_limits_rewind(server, stream_fs, monkeypatch):
    from fsspec.spec import AbstractBufferedFile

    monkeypatch.setattr(AbstractBufferedFile, "DEFAULT_BLOCK_SIZE", 32)
    with stream_fs.open(server.realfile, block_size=0) as stream:
        assert stream.read(256) == data[:256]
        assert len(stream._last_read) <= 32
        assert stream.read(0) == b""
        with pytest.raises(ValueError, match="Cannot seek"):
            stream.seek(-33, 1)
        assert stream.tell() == 256
        assert stream.seek(-32, 1) == 224
        assert stream.read(32) == data[224:256]
        assert stream.read(None) == data[256:]
        assert stream._last_read == stream.leftover == b""


def test_http_stream_large_delimiter_blocks(
    server, stream_fs, reset_files, monkeypatch
):
    from fsspec.spec import AbstractBufferedFile

    monkeypatch.setattr(AbstractBufferedFile, "DEFAULT_BLOCK_SIZE", 8)
    content = b"x" * 7 + b"<>" + b"y" * 40 + b"<>tail"
    HTTPTestHandler.dynamic_files["/large-stream-blocks"] = content
    with stream_fs.open(
        server.address + "/large-stream-blocks", block_size=0
    ) as stream:
        assert stream.readuntil(b"<>", blocks=256) == content[:9]
        assert stream.tell() == 9
        assert len(stream._last_read) <= 8
        assert len(stream.leftover) <= 8 + 1024
        assert stream.readuntil(b"<>", blocks=256) == content[9:51]
        assert stream.tell() == 51
        assert stream.readline() == b"tail"
        assert stream.read() == b""
