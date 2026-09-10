import base64
import io
import os
import shutil

import pytest

from fsspec.callbacks import Callback
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.reference import ReferenceFileSystem

DATA = bytes(range(64))


class RecordingCallback(Callback):
    def __init__(self):
        super().__init__()
        self.increments = []

    def relative_update(self, inc=1):
        self.increments.append(inc)
        super().relative_update(inc)


@pytest.fixture(params=["memory", "local", "reference", "dirfs"])
def source(request, m, tmp_path):
    if request.param == "memory":
        m.pipe_file("/source", DATA)
        return m, "/source"
    if request.param == "local":
        path = tmp_path / "source"
        path.write_bytes(DATA)
        return LocalFileSystem(), str(path)
    if request.param == "reference":
        return ReferenceFileSystem({"source": DATA}), "source"
    m.pipe_file("/root/source", DATA)
    return DirFileSystem(path="/root", fs=m), "source"


@pytest.mark.parametrize(
    "start,end",
    [
        (None, None),
        (0, None),
        (3, 19),
        (None, 8),
        (11, None),
        (-8, None),
        (None, -4),
        (-20, -3),
        (-100, 100),
        (100, None),
        (0, 0),
        (7, 7),
        (9, 2),
    ],
)
def test_byte_ranges(source, tmp_path, start, end):
    fs, path = source
    target = tmp_path / "download"
    target.write_bytes(b"old contents")
    callback = RecordingCallback()
    fs.get_file(path, target, start=start, end=end, callback=callback)
    assert target.read_bytes() == DATA[start:end]
    # LocalFileSystem's pre-existing whole-file fast path does not use callbacks.
    if start is not None or end is not None:
        assert callback.size == callback.value == len(DATA[start:end])


@pytest.mark.parametrize("prefix", [None, 0, 5, len(DATA)])
def test_resume_download(source, tmp_path, prefix):
    fs, path = source
    target = tmp_path / "download"
    if prefix is not None:
        target.write_bytes(DATA[:prefix])
    callback = RecordingCallback()
    fs.get_file(path, target, resume=True, callback=callback)
    assert target.read_bytes() == DATA
    assert callback.size == callback.value == len(DATA)
    assert sum(callback.increments) == len(DATA) - (prefix or 0)


@pytest.mark.parametrize("end", [24, -4, 100])
def test_resume_to_end(source, tmp_path, end):
    fs, path = source
    target = tmp_path / "download"
    target.write_bytes(DATA[:5])
    callback = RecordingCallback()
    fs.get_file(path, target, resume=True, end=end, callback=callback)
    expected = DATA[:end]
    assert target.read_bytes() == expected
    assert callback.value == callback.size == len(expected)
    assert sum(callback.increments) == len(expected) - 5


@pytest.mark.parametrize("options", [{"start": 2}, {"end": 3}])
def test_invalid_resume_preserves_destination(source, tmp_path, options):
    fs, path = source
    target = tmp_path / "download"
    target.write_bytes(DATA[:5])
    with pytest.raises(ValueError):
        fs.get_file(path, target, resume=True, **options)
    assert target.read_bytes() == DATA[:5]


def test_oversized_local_file_is_not_truncated(source, tmp_path):
    fs, path = source
    target = tmp_path / "download"
    original = DATA + b"extra"
    target.write_bytes(original)
    with pytest.raises(ValueError, match="larger than the remote"):
        fs.get_file(path, target, resume=True)
    assert target.read_bytes() == original


def test_filelike_range_stays_open(source):
    fs, path = source
    target = io.BytesIO(b"prefix:")
    target.seek(0, 2)
    fs.get_file(path, target, start=3, end=12)
    assert not target.closed
    assert target.getvalue() == b"prefix:" + DATA[3:12]
    with pytest.raises(ValueError, match="filename"):
        fs.get_file(path, target, resume=True)
    assert target.getvalue() == b"prefix:" + DATA[3:12]


@pytest.mark.parametrize("options", [{"start": 2.5}, {"end": "3"}])
def test_invalid_offsets_do_not_touch_destination(source, tmp_path, options):
    fs, path = source
    target = tmp_path / "download"
    target.write_bytes(b"keep")
    with pytest.raises(TypeError):
        fs.get_file(path, target, **options)
    assert target.read_bytes() == b"keep"


def test_outfile_takes_precedence_and_is_not_closed(m, tmp_path):
    m.pipe_file("source", DATA)
    ignored = tmp_path / "ignored"
    target = io.BytesIO()
    m.get_file("source", ignored, outfile=target, start=5, end=15)
    assert not ignored.exists()
    assert target.getvalue() == DATA[5:15]
    assert not target.closed
    with pytest.raises(ValueError, match="filename"):
        m.get_file("source", outfile=target, resume=True)


def test_missing_remote_preserves_destination(m, tmp_path):
    target = tmp_path / "download"
    target.write_bytes(b"keep")
    with pytest.raises(FileNotFoundError):
        m.get_file("missing", target, resume=True)
    assert target.read_bytes() == b"keep"


def test_failed_transfer_can_resume_without_reading_the_prefix(
    m, tmp_path, monkeypatch
):
    m.pipe_file("source", DATA)
    m.blocksize = 3
    target = tmp_path / "download"
    reads = []

    class Reader(io.BytesIO):
        size = len(DATA)

        def __init__(self, fail):
            super().__init__(DATA)
            self.fail = fail

        def read(self, count=-1):
            reads.append((self.tell(), count))
            if self.fail and self.tell() >= 3:
                raise OSError("interrupted")
            return super().read(count)

    monkeypatch.setattr(m, "open", lambda *args, **kwargs: Reader(fail=True))
    with pytest.raises(OSError, match="interrupted"):
        m.get_file("source", target, resume=True)
    assert target.read_bytes() == DATA[:3]
    reads.clear()
    monkeypatch.setattr(m, "open", lambda *args, **kwargs: Reader(fail=False))
    m.get_file("source", target, resume=True)
    assert reads[0][0] == 3
    assert target.read_bytes() == DATA


def test_recursive_get_resumes_each_file_independently(m, tmp_path):
    m.pipe_file("/source/a", DATA)
    m.pipe_file("/source/nested/b", DATA[::-1])
    m.makedirs("/source/empty")
    target = tmp_path / "result"
    target.mkdir()
    (target / "a").write_bytes(DATA[:5])
    m.get("/source/", str(target), recursive=True, resume=True)
    assert (target / "a").read_bytes() == DATA
    assert (target / "nested/b").read_bytes() == DATA[::-1]
    assert (target / "empty").is_dir()


@pytest.mark.parametrize("hardlink", [False, True])
def test_local_source_and_destination_cannot_be_the_same(tmp_path, hardlink):
    source = tmp_path / "source"
    source.write_bytes(DATA)
    target = tmp_path / "target" if hardlink else source
    if hardlink:
        os.link(source, target)
    with pytest.raises(shutil.SameFileError):
        LocalFileSystem().get_file(source, target, start=3, end=9)
    assert source.read_bytes() == DATA


@pytest.mark.parametrize("encoded", [False, True])
@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.asyncio
async def test_native_async_reference_download(tmp_path, encoded, resume):
    value = b"base64:" + base64.b64encode(DATA) if encoded else DATA
    fs = ReferenceFileSystem({"source": value})
    target = tmp_path / "download"
    callback = RecordingCallback()
    if resume:
        target.write_bytes(DATA[:5])
        await fs._get_file("source", target, resume=True, end=-4, callback=callback)
        expected = DATA[:-4]
    else:
        await fs._get_file("source", target, start=-20, end=-4, callback=callback)
        expected = DATA[-20:-4]
    assert target.read_bytes() == expected
    assert callback.size == callback.value == len(expected)


@pytest.mark.asyncio
async def test_native_async_reference_filelike_target():
    fs = ReferenceFileSystem({"source": DATA})
    target = io.BytesIO()
    await fs._get_file("source", target, start=3, end=8)
    assert target.getvalue() == DATA[3:8]
    assert not target.closed


def test_destination_change_while_preparing_resume_is_rejected(tmp_path):
    from fsspec._download import _Download

    target = tmp_path / "download"
    target.write_bytes(DATA[:3])
    download = _Download(target, resume=True)
    download.set_size(len(DATA))
    target.write_bytes(DATA[:5])
    with pytest.raises(ValueError, match="changed"):
        with download.open():
            pytest.fail("A concurrently changed destination was accepted")
    assert target.read_bytes() == DATA[:5]


@pytest.mark.parametrize("return_none", [False, True])
def test_short_writes_and_none_returning_writes(m, return_none):
    class Writer(io.BytesIO):
        def write(self, data):
            if return_none:
                super().write(data)
                return None
            return super().write(data[:2])

    m.pipe_file("source", DATA)
    target = Writer()
    callback = RecordingCallback()
    m.get_file("source", target, start=3, end=15, callback=callback)
    assert target.getvalue() == DATA[3:15]
    assert callback.value == callback.size == 12
    assert not target.closed


@pytest.mark.parametrize("written", [0, -1, 100])
def test_invalid_write_result_raises_instead_of_looping(m, written):
    class Writer(io.BytesIO):
        def write(self, data):
            return written

    m.pipe_file("source", DATA)
    target = Writer()
    with pytest.raises(OSError, match="Destination"):
        m.get_file("source", target, start=0, end=3)
    assert not target.closed


def test_unknown_size_negative_offsets_fail_clearly():
    from fsspec._download import _Download

    download = _Download(io.BytesIO(), start=-3)
    with pytest.raises(ValueError, match="remote size"):
        download.set_size(None)


def test_reference_bulk_get_uses_range_and_resume_options(tmp_path):
    fs = ReferenceFileSystem({"source/a": DATA, "source/b": DATA[::-1]})
    target = tmp_path / "output"
    target.mkdir()
    (target / "a").write_bytes(DATA[:5])
    fs.get("source/", str(target), recursive=True, resume=True)
    assert (target / "a").read_bytes() == DATA
    assert (target / "b").read_bytes() == DATA[::-1]
    fs.get("source/a", str(target / "slice"), start=3, end=12)
    assert (target / "slice").read_bytes() == DATA[3:12]


@pytest.mark.parametrize("backend", ["ftp", "sftp"])
def test_transfer_backends_delegate_ranges(backend, monkeypatch, tmp_path):
    if backend == "ftp":
        from fsspec.implementations.ftp import FTPFileSystem

        cls = FTPFileSystem
    else:
        pytest.importorskip("paramiko")
        from fsspec.implementations.sftp import SFTPFileSystem

        cls = SFTPFileSystem
    # Exercise the adapter's dispatch, without making a network connection.
    fs = object.__new__(cls)
    fs.ftp = io.BytesIO()
    monkeypatch.setattr(fs, "isdir", lambda path: False)
    monkeypatch.setattr(fs, "open", lambda *args, **kwargs: io.BytesIO(DATA))
    target = tmp_path / "download"
    fs.get_file("source", target, start=3, end=12)
    assert target.read_bytes() == DATA[3:12]
    target.write_bytes(DATA[:5])
    fs.get_file("source", target, resume=True)
    assert target.read_bytes() == DATA


@pytest.mark.parametrize(
    "options,seekable",
    [
        ({}, False),
        ({"start": 0}, True),
        ({"end": 8}, True),
        ({"resume": True}, True),
        ({"start": 3, "seekable": False}, False),
    ],
)
def test_arrow_download_selects_seekable_streams(monkeypatch, options, seekable):
    from fsspec.implementations.arrow import ArrowFSWrapper
    from fsspec.spec import AbstractFileSystem

    calls = []
    monkeypatch.setattr(
        AbstractFileSystem,
        "get_file",
        lambda self, rpath, lpath, **kwargs: calls.append(kwargs),
    )
    fs = object.__new__(ArrowFSWrapper)
    fs.get_file("source", "target", **options)
    assert calls == [{**options, "seekable": seekable}]


@pytest.mark.parametrize(
    "status,headers,match",
    [
        (416, {}, "Content-Range"),
        (416, {"Content-Range": "bytes */64"}, "satisfiable"),
        (206, {"Content-Range": "nonsense"}, "Content-Range"),
        (206, {"Content-Range": "bytes 8-4/64"}, "bounds"),
        (206, {"Content-Range": "bytes 3-64/64"}, "bounds"),
        (206, {"Content-Range": "bytes 3-7/64"}, "end offset"),
        (204, {}, "status"),
    ],
)
def test_invalid_http_range_metadata(status, headers, match):
    from fsspec._download import _Download

    download = _Download(io.BytesIO(), start=3, end=12)
    with pytest.raises(ValueError, match=match):
        download.response(status, headers)


def test_generic_nonseekable_stream_can_download_a_bounded_prefix(m, monkeypatch):
    class Reader(io.BytesIO):
        def seekable(self):
            return False

        def seek(self, *args):
            raise io.UnsupportedOperation("seek")

    m.pipe_file("source", DATA)
    monkeypatch.setattr(m, "open", lambda *args, **kwargs: Reader(DATA))
    target = io.BytesIO()
    m.get_file("source", target, end=8)
    assert target.getvalue() == DATA[:8]


@pytest.mark.asyncio
async def test_empty_async_reference_range_does_not_fetch_data(tmp_path, monkeypatch):
    fs = ReferenceFileSystem({"source": DATA})

    async def unexpected_fetch(*args, **kwargs):
        pytest.fail("An empty range should not fetch any bytes")

    monkeypatch.setattr(fs, "_cat_file", unexpected_fetch)
    target = tmp_path / "download"
    await fs._get_file("source", target, start=8, end=8)
    assert target.read_bytes() == b""
