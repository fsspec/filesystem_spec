"""GPU integration tests; skipped on installations without CUDA Core or a device."""

import gc
import io

import pytest

import fsspec
from fsspec.config import conf
from fsspec.implementations.cuda import CUDAFileSystem, _buffer_view, _host_view


@pytest.fixture
def gpu():
    from types import SimpleNamespace

    core = pytest.importorskip("cuda.core")
    try:
        device = core.Device()
        device.set_current()
        stream = device.create_stream()
    except (RuntimeError, core.CUDAError) as exc:
        pytest.skip(str(exc))
    yield SimpleNamespace(core=core, device=device, stream=stream)
    stream.sync()


def download(buffer, gpu, stream=None):
    stream = gpu.stream if stream is None else stream
    if buffer.size == 0:
        return b""
    host = gpu.core.LegacyPinnedMemoryResource().allocate(buffer.size, stream=stream)
    host.copy_from(buffer, stream=stream)
    stream.sync()
    return bytes(_host_view(host))


def upload(data, gpu):
    host = gpu.core.LegacyPinnedMemoryResource().allocate(len(data), stream=gpu.stream)
    _host_view(host)[:] = data
    out = gpu.device.allocate(len(data), stream=gpu.stream)
    if data:
        out.copy_from(host, stream=gpu.stream)
    gpu.stream.sync()
    return out


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "data"
    path.write_bytes(bytes(range(256)) * 32)
    return path


@pytest.mark.parametrize("method", ["pinned", "bounce"])
def test_local(gpu, source, method):
    if method == "bounce":
        pytest.importorskip("kvikio")
    stream = gpu.device.create_stream()
    fs, path = fsspec.core.url_to_fs(
        f"cuda+file://{source}", method=method, stream=stream
    )
    try:
        assert fs.size(path) == 8192
        data = fs.cat(path)
        assert isinstance(data, gpu.core.Buffer)
        assert fs.cat_file(path, 100, 50).size == 0
        tail = fs.cat_file(path, -5)
        ranges = fs.cat_ranges([path, path], [0, 250], [4, 260])
        gc.collect()
        fs.synchronize()
        assert download(data, gpu) == source.read_bytes()
        assert download(tail, gpu) == source.read_bytes()[-5:]
        assert [download(r, gpu) for r in ranges] == [
            bytes(range(4)),
            bytes(range(250, 256)) + bytes(range(4)),
        ]
    finally:
        fs.close()


def test_file(gpu, source):
    stream = gpu.device.create_stream()
    fs = fsspec.filesystem("cuda+file", stream=stream)
    with fs.open(source) as file:
        first = file.read(7)
        assert file.tell() == 7
        file.seek(-3, 2)
        dest = gpu.device.allocate(8, stream=stream)
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
    assert download(first, gpu) == bytes(range(7))
    assert download(_buffer_view(dest, size=3), gpu) == source.read_bytes()[-3:]
    with pytest.raises(ValueError):
        file.read()


def test_write(gpu, tmp_path):
    fs = fsspec.filesystem("cuda+file")
    path = tmp_path / "write"
    data = upload(bytes(range(20)), gpu)
    fs.pipe_file(path, data)
    assert path.read_bytes() == bytes(range(20))
    with fs.open(path, "ab") as file:
        assert file.write(_buffer_view(data, size=0)) == 0
        assert file.write(_buffer_view(data, size=2)) == 2
    assert path.read_bytes() == bytes(range(20)) + b"\x00\x01"
    with pytest.raises(FileExistsError):
        fs.pipe_file(path, data, mode="create")
    with pytest.raises(TypeError):
        fs.pipe_file(path, b"cpu")


def test_pool_and_cross_stream(gpu, source):
    fs = fsspec.filesystem("cuda+file", streams=2)
    data = [fs.cat_file(source, start=i, end=i + 32) for i in range(5)]
    consumer = gpu.device.create_stream()
    fs.wait(consumer)
    result = gpu.device.allocate(5 * 32, stream=consumer)
    for i, buffer in enumerate(data):
        _buffer_view(result, i * 32, 32).copy_from(buffer, stream=consumer)
    consumer.sync()
    fs.synchronize()
    assert download(result, gpu) == b"".join(
        source.read_bytes()[i : i + 32] for i in range(5)
    )
    pending = fs.cat_file(source, 0, 4)
    default = fs.wait()
    assert default is gpu.device.default_stream
    copied = pending.copy_to(stream=default)
    default.sync()
    fs.synchronize()
    assert download(copied, gpu) == bytes(range(4))


def test_config_and_metadata(gpu, source, monkeypatch):
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
    assert download(data, gpu) == b"\x00\x01\x02"


def test_memory_and_chaining(gpu):
    cpu = fsspec.filesystem("memory")
    cpu.pipe("/cuda-test", b"abcdef")
    fs, path = fsspec.core.url_to_fs("cuda::memory:///cuda-test")
    data = fs.cat(path)
    fs.synchronize()
    assert download(data, gpu) == b"abcdef"
    fs = fsspec.filesystem("cuda+memory")
    assert isinstance(fs.cat(["/cuda-test"])["/cuda-test"], gpu.core.Buffer)
    fs.close()


def test_reject_unsupported(gpu, source):
    fs = fsspec.filesystem("cuda+file")
    for kwargs in [{"mode": "rt"}, {"compression": "gzip"}, {"autocommit": False}]:
        with pytest.raises((ValueError, NotImplementedError)):
            fs.open(source, **kwargs)
    with pytest.raises(NotImplementedError):
        fs.read_block(source, 0, 3, delimiter=b"\n")
    with pytest.raises(ValueError):
        CUDAFileSystem(method="unknown")


def test_cufile(gpu, source):
    pytest.importorskip("kvikio")
    from kvikio.cufile_driver import properties

    if not properties.is_gds_available:
        pytest.skip("GDS not available")
    fs = fsspec.filesystem("cuda+file", method="cufile", streams=1)
    data = fs.cat_file(source)
    tail = fs.cat_file(source, 9, 49)
    fs.synchronize()
    assert download(data, gpu) == source.read_bytes()
    assert download(tail, gpu) == source.read_bytes()[9:49]


@pytest.mark.parametrize("method", ["pinned", "kvikio", "kvikio-s3"])
def test_http(gpu, method):
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
            assert download(result, gpu) == content[7:100]
            assert download(whole, gpu) == content
        finally:
            if fs is not None:
                fs.close()
            server.shutdown()
            thread.join()


def test_cufile_deferred_error(gpu, source, monkeypatch):
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
    stream = gpu.device.create_stream()
    fs = fsspec.filesystem("cuda+file", method="cufile", stream=stream)
    out = fs.cat_file(source, 9, 25)
    assert out.size == 16
    assert calls[-1] == (int(stream.handle), 16, 9)
    with pytest.raises(OSError, match="Short CUDA read"):
        fs.synchronize()
    assert closed == [True]


def test_short_host_read(gpu, source, monkeypatch):
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


def test_cuobject_callback_bridge(gpu, monkeypatch):
    import ctypes
    from types import SimpleNamespace

    from fsspec.implementations.cuda_cuobject import readinto

    calls = []
    out = gpu.device.allocate(7, stream=gpu.stream)

    class NativeRead:
        def __call__(self, ptr, size, offset, callback, context):
            assert ptr == int(out.handle)
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


def test_buffer_views_and_lifetime(gpu, source):
    fs = fsspec.filesystem("cuda+file", stream=gpu.stream)
    allocation = gpu.device.allocate(16, stream=gpu.stream)
    allocation.fill(255, stream=gpu.stream)
    destination = _buffer_view(allocation, 4, 8)
    assert destination.owner is allocation
    with fs.open(source) as file:
        assert file.readinto(destination) == 8
    del allocation
    gc.collect()
    fs.synchronize()
    assert (
        download(destination.owner, gpu) == b"\xff" * 4 + bytes(range(8)) + b"\xff" * 4
    )
    destination.close()
    with fs.open(source) as file, pytest.raises(ValueError, match="closed"):
        file.readinto(destination)


@pytest.mark.parametrize("protocol", ["cai", "dlpack"])
def test_foreign_buffers(gpu, source, tmp_path, protocol):
    allocation = gpu.device.allocate(12, stream=gpu.stream)

    class Array:
        __cuda_array_interface__ = {
            "shape": (2, 3),
            "typestr": "<u2",
            "strides": None,
            "data": (int(allocation.handle), False),
            "version": 3,
            "stream": int(gpu.stream.handle),
        }

    class Tensor:
        def __dlpack__(self, **kwargs):
            return allocation.__dlpack__(**kwargs)

        def __dlpack_device__(self):
            return allocation.__dlpack_device__()

    buffer = Array() if protocol == "cai" else Tensor()
    fs = fsspec.filesystem("cuda+file", stream=gpu.stream)
    with fs.open(source) as file:
        assert file.readinto(buffer) == 12
    fs.synchronize()
    assert download(allocation, gpu) == bytes(range(12))
    target = tmp_path / "foreign"
    fs.pipe_file(target, buffer)
    assert target.read_bytes() == bytes(range(12))


def test_invalid_foreign_buffers(gpu, source, tmp_path):
    allocation = upload(b"abcdef", gpu)

    class ReadOnly:
        __cuda_array_interface__ = {
            "shape": (6,),
            "typestr": "|u1",
            "data": (int(allocation.handle), True),
            "version": 3,
            "stream": int(gpu.stream.handle),
        }

    class Strided:
        __cuda_array_interface__ = {
            "shape": (3,),
            "strides": (2,),
            "typestr": "|u1",
            "data": (int(allocation.handle), False),
            "version": 3,
            "stream": int(gpu.stream.handle),
        }

    fs = fsspec.filesystem("cuda+file", stream=gpu.stream)
    with fs.open(source) as file:
        with pytest.raises(ValueError, match="read-only"):
            file.readinto(ReadOnly())
        with pytest.raises(ValueError, match="contiguous"):
            file.readinto(Strided())
    target = tmp_path / "readonly-source"
    fs.pipe_file(target, ReadOnly())
    assert target.read_bytes() == b"abcdef"


def test_stream_options(gpu, source):
    from types import SimpleNamespace

    stream = gpu.device.create_stream()
    for option in (
        stream,
        int(stream.handle),
        SimpleNamespace(__cuda_stream__=stream.__cuda_stream__),
    ):
        fs = fsspec.filesystem("cuda+file", stream=option)
        result = fs.cat_file(source, 3, 8)
        fs.synchronize()
        assert download(result, gpu) == bytes(range(3, 8))
    fs = fsspec.filesystem("cuda+file", streams=[stream, gpu.stream])
    result = fs.cat_file(source, 3, 8)
    fs.synchronize()
    assert download(result, gpu) == bytes(range(3, 8))


@pytest.mark.parametrize("method", ["pinned", "bounce"])
def test_io_without_cupy(gpu, source, tmp_path, monkeypatch, method):
    import builtins

    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "cupy" or name.startswith("cupy."):
            raise AssertionError("CUDA Buffer I/O must not import CuPy")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    fs = fsspec.filesystem("cuda+file", method=method, streams=2)
    result = fs.cat_file(source)
    fs.synchronize()
    assert isinstance(result, gpu.core.Buffer)
    target = tmp_path / "copy"
    fs.pipe_file(target, result)
    assert target.read_bytes() == source.read_bytes()
