"""GPU integration tests; skipped on installations without CuPy or a device."""

import gc
import io

import pytest

import fsspec
from fsspec.config import conf
from fsspec.implementations.cuda import CUDAFileSystem


@pytest.fixture
def cp():
    cp = pytest.importorskip("cupy")
    try:
        cp.cuda.runtime.getDeviceCount()
        cp.empty(0)
    except cp.cuda.runtime.CUDARuntimeError as exc:
        pytest.skip(str(exc))
    return cp


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "data"
    path.write_bytes(bytes(range(256)) * 32)
    return path


@pytest.mark.parametrize("method", ["pinned", "bounce"])
def test_local(cp, source, method):
    if method == "bounce":
        pytest.importorskip("kvikio")
    stream = cp.cuda.Stream(non_blocking=True)
    fs, path = fsspec.core.url_to_fs(
        f"cuda+file://{source}", method=method, stream=stream
    )
    try:
        assert fs.size(path) == 8192
        data = fs.cat(path)
        assert isinstance(data, cp.ndarray)
        assert fs.cat_file(path, 100, 50).size == 0
        tail = fs.cat_file(path, -5)
        ranges = fs.cat_ranges([path, path], [0, 250], [4, 260])
        gc.collect()
        fs.synchronize()
        assert data.get().tobytes() == source.read_bytes()
        assert tail.get().tobytes() == source.read_bytes()[-5:]
        assert [r.get().tobytes() for r in ranges] == [
            bytes(range(4)),
            bytes(range(250, 256)) + bytes(range(4)),
        ]
    finally:
        fs.close()


def test_file(cp, source):
    stream = cp.cuda.Stream(non_blocking=True)
    fs = fsspec.filesystem("cuda+file", stream=stream)
    with fs.open(source) as file:
        first = file.read(7)
        assert file.tell() == 7
        file.seek(-3, 2)
        dest = cp.empty(8, dtype="u1")
        assert file.readinto(dest) == 3
        assert file.read().size == 0
        file.seek(9000)
        assert file.read().size == 0
        with pytest.raises(TypeError):
            file.readinto(bytearray(1))
        with pytest.raises(ValueError):
            file.seek(-1)
        with pytest.raises(io.UnsupportedOperation):
            file.readline()
    fs.synchronize()
    assert first.get().tobytes() == bytes(range(7))
    assert dest[:3].get().tobytes() == source.read_bytes()[-3:]
    with pytest.raises(ValueError):
        file.read()


def test_write(cp, tmp_path):
    fs = fsspec.filesystem("cuda+file")
    path = tmp_path / "write"
    data = cp.arange(20, dtype="u1")
    fs.pipe_file(path, data)
    assert path.read_bytes() == bytes(range(20))
    with fs.open(path, "ab") as file:
        assert file.write(data[:0]) == 0
        assert file.write(data[:2]) == 2
    assert path.read_bytes() == bytes(range(20)) + b"\x00\x01"
    with pytest.raises(FileExistsError):
        fs.pipe_file(path, data, mode="create")
    with pytest.raises(TypeError):
        fs.pipe_file(path, b"cpu")


def test_pool_and_cross_stream(cp, source):
    fs = fsspec.filesystem("cuda+file", streams=2)
    data = [fs.cat_file(source, start=i, end=i + 32) for i in range(5)]
    consumer = cp.cuda.Stream(non_blocking=True)
    fs.wait(consumer)
    with consumer:
        result = cp.concatenate(data)
    consumer.synchronize()
    fs.synchronize()
    assert result.get().tobytes() == b"".join(
        source.read_bytes()[i : i + 32] for i in range(5)
    )
    pending = fs.cat_file(source, 0, 4)
    assert fs.wait() is cp.cuda.get_current_stream()
    copied = pending.copy()
    fs.synchronize()
    assert copied.get().tobytes() == bytes(range(4))


def test_config_and_metadata(cp, source, monkeypatch):
    monkeypatch.setitem(conf, "cuda", {"method": "bounce", "streams": 2})
    fs = fsspec.filesystem("cuda+file", method="pinned")
    assert fs.method == "pinned"
    assert fs._streams_spec == 2
    assert fs.exists(f"cuda+file://{source}")
    assert fs.glob(str(source.parent / "*")) == [str(source)]
    other = str(source) + ".copy"
    fs.copy(source, other)
    fs.rm(other)
    assert not fs.exists(other)
    with fsspec.open(f"cuda+file://{source}") as file:
        data = file.read(3)
        file.fs.synchronize()
    assert data.get().tobytes() == b"\x00\x01\x02"


def test_memory_and_chaining(cp):
    cpu = fsspec.filesystem("memory")
    cpu.pipe("/cuda-test", b"abcdef")
    fs, path = fsspec.core.url_to_fs("cuda::memory:///cuda-test")
    data = fs.cat(path)
    fs.synchronize()
    assert data.get().tobytes() == b"abcdef"
    fs = fsspec.filesystem("cuda+memory")
    assert fs.cat(["/cuda-test"])["/cuda-test"].dtype == cp.uint8
    fs.close()


def test_reject_unsupported(cp, source):
    fs = fsspec.filesystem("cuda+file")
    for kwargs in [{"mode": "rt"}, {"compression": "gzip"}, {"autocommit": False}]:
        with pytest.raises((ValueError, NotImplementedError)):
            fs.open(source, **kwargs)
    with pytest.raises(NotImplementedError):
        fs.read_block(source, 0, 3, delimiter=b"\n")
    with pytest.raises(ValueError):
        CUDAFileSystem(method="unknown")


def test_cufile(cp, source):
    pytest.importorskip("kvikio")
    from kvikio.cufile_driver import properties

    if not properties.is_gds_available:
        pytest.skip("GDS not available")
    fs = fsspec.filesystem("cuda+file", method="cufile", streams=1)
    data = fs.cat_file(source)
    tail = fs.cat_file(source, 9, 49)
    fs.synchronize()
    assert data.get().tobytes() == source.read_bytes()
    assert tail.get().tobytes() == source.read_bytes()[9:49]


@pytest.mark.parametrize("method", ["pinned", "kvikio", "kvikio-s3"])
def test_http(cp, method):
    pytest.importorskip("aiohttp")
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    content = bytes(range(256)) * 32

    class Handler(BaseHTTPRequestHandler):
        def respond(self, head=False):
            start, end = 0, len(content) - 1
            partial = "Range" in self.headers
            if partial:
                first, last = self.headers["Range"].removeprefix("bytes=").split("-")
                start, end = int(first), min(int(last), end)
            self.send_response(206 if partial else 200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            if partial:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(content)}")
            self.end_headers()
            if not head:
                self.wfile.write(content[start : end + 1])

        def do_HEAD(self):
            self.respond(True)

        def do_GET(self):
            self.respond()

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        fs = None
        try:
            url = f"cuda+http://127.0.0.1:{server.server_port}/data"
            if method == "kvikio-s3":
                # Exercise the real S3 KvikIO factory and signed HTTP transport
                # with metadata supplied locally (no cloud credentials needed).
                from types import SimpleNamespace

                metadata = SimpleNamespace(
                    protocol="s3",
                    _strip_protocol=lambda p: p,
                    size=lambda p: len(content),
                )
                fs = CUDAFileSystem(
                    fs=metadata,
                    method="kvikio",
                    streams=2,
                    backend_options={
                        "aws_region_name": "us-east-1",
                        "aws_access_key_id": "test",
                        "aws_secret_access_key": "test",
                        "aws_endpoint_url": f"http://127.0.0.1:{server.server_port}",
                    },
                )
                path = "bucket/data"
            else:
                fs, path = fsspec.core.url_to_fs(url, method=method, streams=2)
            result = fs.cat_file(path, 7, 100)
            with fs.open(path) as file:
                whole = file.read()
            fs.synchronize()
            assert result.get().tobytes() == content[7:100]
            assert whole.get().tobytes() == content
        finally:
            if fs is not None:
                fs.close()
            server.shutdown()
            thread.join()


def test_cufile_deferred_error(cp, source, monkeypatch):
    from types import SimpleNamespace

    import kvikio

    closed = []
    calls = []

    class Handle:
        def __init__(self, path, mode):
            calls.append((path, mode))

        def raw_read_async(self, out, stream, size, file_offset):
            calls.append((stream, size, file_offset))
            return SimpleNamespace(check_bytes_done=lambda: size - 1)

        def close(self):
            closed.append(True)

    monkeypatch.setattr(kvikio, "CuFile", Handle)
    stream = cp.cuda.Stream(non_blocking=True)
    fs = fsspec.filesystem("cuda+file", method="cufile", stream=stream)
    out = fs.cat_file(source, 9, 25)
    assert out.size == 16
    assert calls[-1] == (stream.ptr, 16, 9)
    with pytest.raises(OSError, match="Short CUDA read"):
        fs.synchronize()
    assert closed == [True]


def test_short_host_read(cp, source, monkeypatch):
    cpu = fsspec.filesystem("file")
    monkeypatch.setattr(cpu, "size", lambda path: 10000)
    fs = CUDAFileSystem(fs=cpu)
    with pytest.raises(OSError, match="Short read"):
        fs.cat_file(source)


def test_cuda_registration_without_gpu():
    fs, path = fsspec.core.url_to_fs("cuda+memory:///metadata")
    fs.fs.pipe_file(path, b"metadata")
    assert fs.info(path)["size"] == 8
    assert fs.exists(path)
    fs.rm([path])


def test_cuobject_callback_bridge(cp, monkeypatch):
    import ctypes
    from types import SimpleNamespace

    from fsspec.implementations.cuda_cuobject import readinto

    calls = []
    out = cp.empty(7, dtype="u1")

    class NativeRead:
        def __call__(self, ptr, size, offset, callback, context):
            assert ptr == out.data.ptr
            token = ctypes.create_string_buffer(b"rdma-token")
            return callback(context, ctypes.addressof(token), 10, size, offset)

    monkeypatch.setattr(
        ctypes, "CDLL", lambda path: SimpleNamespace(fsspec_cuobject_read=NativeRead())
    )

    def control(path, token, size, offset):
        calls.append((path, token, size, offset))
        return size

    assert readinto("bucket/key", out, 42, library="bridge.so", control=control) == 7
    assert calls == [("bucket/key", b"rdma-token", 7, 42)]

    def failed(*args):
        raise RuntimeError("RDMA declined")

    with pytest.raises(RuntimeError, match="RDMA declined"):
        readinto("bucket/key", out, 42, library="bridge.so", control=failed)

    with pytest.raises(OSError, match="cuObject read failed"):
        readinto("bucket/key", out, 42, library="bridge.so", control=lambda *a: 0)
