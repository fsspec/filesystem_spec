"""Experimental CUDA-buffer filesystem facade (no asyncio API).

Optional imports are deferred until data is transferred. See ``docs/source/gpu.rst``
for completion, ownership, and backend semantics.
"""

import ctypes
import io
import itertools
import math
import threading

from fsspec import AbstractFileSystem, filesystem
from fsspec.implementations.chained import ChainedFileSystem
from fsspec.utils import stringify_path

# KvikIO's Python CuFile constructor reads process-global defaults.
_cufile_lock = threading.Lock()


def _host_view(buffer):
    """Expose a host-accessible Buffer to Python I/O, retaining its owner."""
    if buffer.is_closed or not buffer.is_host_accessible:
        raise ValueError("Buffer must be open and host accessible")
    array = (ctypes.c_ubyte * buffer.size).from_address(int(buffer.handle))
    array._owner = buffer
    return memoryview(array).cast("B")


def _buffer_view(buffer, offset=0, size=None):
    """Create a non-owning byte range that keeps its allocation alive."""
    from cuda.core import Buffer

    if buffer.is_closed:
        raise ValueError("CUDA buffer is closed")
    size = buffer.size - offset if size is None else size
    if offset < 0 or size < 0 or offset + size > buffer.size:
        raise ValueError("Buffer view is outside the allocation")
    return Buffer.from_handle(int(buffer.handle) + offset, size, owner=buffer)


class _KvikioBuffer:
    """Adapt a Buffer to KvikIO's CUDA Array Interface without an array library."""

    def __init__(self, buffer, stream):
        self.buffer = buffer
        self.__cuda_array_interface__ = {
            "shape": (buffer.size,),
            "typestr": "|u1",
            "data": (int(buffer.handle), False),
            "version": 3,
            "stream": int(stream.handle) or 1,
        }


def cuda_filesystem_class(protocol):
    """Build a facade for a registered underlying protocol."""
    target = protocol.removeprefix("cuda+")
    from fsspec.registry import get_filesystem_class

    base = get_filesystem_class(target)
    return type(
        "CUDA" + base.__name__,
        (CUDAFileSystem,),
        {
            "protocol": ("cuda", protocol),
            "_target_protocol": target,
            "__module__": __name__,
        },
    )


class CUDAFileSystem(ChainedFileSystem):
    """Wrap an fsspec filesystem, returning cuda.core.Buffer byte allocations.

    ``method`` is one of ``pinned`` (default), ``cufile``, ``bounce``,
    ``kvikio``, or ``cuobject``. ``stream`` selects one CUDA stream;
    ``streams`` selects a round-robin pool (an integer creates that many
    nonblocking streams). With neither set, use the device's CUDA Core default
    stream. Options can also be set in fsspec's ``cuda``
    configuration section.

    Metadata and server-side manipulation use the underlying filesystem.
    Call ``synchronize()`` to finish transfers and report deferred I/O errors.
    File close does not synchronize reads. The filesystem owns pending transfers.
    """

    protocol = "cuda"
    _target_protocol = "file"
    cachable = False

    def __init__(
        self,
        fs=None,
        target_protocol=None,
        target_options=None,
        method="pinned",
        stream=None,
        streams=None,
        backend_options=None,
        fo=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if method not in {"pinned", "cufile", "bounce", "kvikio", "cuobject"}:
            raise ValueError(f"Unknown CUDA I/O method: {method}")
        if stream is not None and streams is not None:
            raise ValueError("Specify stream or streams, not both")
        self.fs = (
            fs
            if fs is not None
            else filesystem(
                target_protocol or self._target_protocol,
                **(kwargs | (target_options or {})),
            )
        )
        self.method = method
        self.backend_options = backend_options or {}
        self._stream_spec = stream
        self._streams_spec = streams
        self._streams = None
        self._pending = []
        self._lock = threading.RLock()

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        if path.startswith("cuda+"):
            path = path[5:]
        elif path.startswith("cuda://"):
            path = path[7:]
        if cls.protocol == "cuda":
            return path
        from fsspec.registry import get_filesystem_class

        return get_filesystem_class(cls._target_protocol)._strip_protocol(path)

    def _path(self, path):
        if isinstance(path, (list, tuple, set)):
            return [self._path(p) for p in path]
        path = stringify_path(path)
        if path.startswith("cuda+"):
            path = path[5:]
        elif path.startswith("cuda://"):
            path = path[7:]
        return self.fs._strip_protocol(path)

    def _stream(self, stream=None):
        from cuda.core import Device, Stream

        device = Device()
        device.set_current()

        def wrap(s):
            if isinstance(s, Stream):
                return s
            if isinstance(s, int):
                return Stream.from_handle(s)
            return device.create_stream(s)

        if stream is not None:
            return wrap(stream)
        with self._lock:
            if self._streams is None and self._streams_spec is not None:
                spec = self._streams_spec
                if isinstance(spec, (int, str)):
                    if int(spec) < 1:
                        raise ValueError("streams must be positive")
                    spec = [device.create_stream() for _ in range(int(spec))]
                spec = [wrap(s) for s in spec]
                if not spec:
                    raise ValueError("streams must not be empty")
                self._streams = itertools.cycle(spec)
            if self._streams is not None:
                return next(self._streams)
        if self._stream_spec is not None:
            return wrap(self._stream_spec)
        return device.default_stream

    def _retain(self, stream, resources, check=None, close=None):
        event = stream.record()
        with self._lock:
            self._pending.append((event, (stream, resources), check, close))

    def _reap(self):
        # Release staging allocations from completed transfers during long
        # sequences of reads. Never reclaim a buffer while DMA can access it.
        with self._lock:
            ready, pending = [], []
            for transfer in self._pending:
                (ready if transfer[0].is_done else pending).append(transfer)
            self._pending = pending
        self._finish(ready)

    def synchronize(self):
        """Wait for all submitted I/O, releasing resources and raising errors."""
        with self._lock:
            pending, self._pending = self._pending, []
        self._finish(pending)

    @staticmethod
    def _finish(pending):
        error = None
        for event, resources, check, close in pending:
            try:
                event.sync()
                if check is not None:
                    check()
            except Exception as exc:
                error = error or exc
            finally:
                if close is not None:
                    try:
                        close()
                    except Exception as exc:
                        error = error or exc
        if error is not None:
            raise error

    def wait(self, stream=None):
        """Order a consumer stream after pending transfers, without a host wait.

        Use synchronize separately to check deferred cuFile completion counts.
        """
        from cuda.core import Device

        stream = Device().default_stream if stream is None else self._stream(stream)
        with self._lock:
            for event, *_ in self._pending:
                stream.wait(event)
        return stream

    close = synchronize

    def __del__(self):
        if getattr(self, "_pending", None):
            self.synchronize()

    def _device_bytes(self, buffer, stream, writable=True):
        from cuda.core import Buffer
        from cuda.core.utils import StridedMemoryView

        if isinstance(buffer, Buffer):
            if buffer.is_closed:
                raise ValueError("CUDA buffer is closed")
            if not buffer.is_device_accessible:
                raise ValueError("Buffer must be device accessible")
            return buffer
        if not (
            hasattr(buffer, "__cuda_array_interface__") or hasattr(buffer, "__dlpack__")
        ):
            raise TypeError("Expected a CUDA Buffer or contiguous CUDA object")
        view = StridedMemoryView.from_any_interface(
            buffer, stream_ptr=int(stream.handle) or 1
        )
        if not view.is_device_accessible:
            raise ValueError("Buffer must be device accessible")
        strides = view.strides
        expected = 1
        for axis in reversed(range(len(view.shape))):
            if (
                view.shape[axis] > 1
                and strides is not None
                and strides[axis] != expected
            ):
                raise ValueError("CUDA buffer must be C contiguous")
            expected *= view.shape[axis]
        if writable and view.readonly:
            raise ValueError("CUDA buffer is read-only")
        size = math.prod(view.shape) * view.dtype.itemsize
        return Buffer.from_handle(view.ptr, size, owner=view)

    def _readinto(self, path, out, offset, stream):
        from cuda.core import LegacyPinnedMemoryResource

        self._reap()
        size = out.size
        if not size:
            return 0
        method = self.method
        if method == "cufile":
            if self.fs.protocol not in ("file", "local", ("file", "local")):
                raise ValueError("cufile requires a local filesystem")
            import kvikio
            from kvikio import defaults

            # Force direct mode; do not silently turn a requested direct read
            # into POSIX I/O. Restore the process default immediately after open.
            with (
                _cufile_lock,
                defaults.set("compat_mode", kvikio.CompatMode.OFF),
            ):
                handle = kvikio.CuFile(path, "r")
            adapter = _KvikioBuffer(out, stream)
            try:
                future = handle.raw_read_async(
                    adapter, int(stream.handle), size=size, file_offset=offset
                )
            except BaseException:
                handle.close()
                raise

            def check():
                count = future.check_bytes_done()
                if count != size:
                    raise OSError(f"Short CUDA read: expected {size}, got {count}")

            self._retain(stream, (out, adapter, handle, future), check, handle.close)
            return size
        if method == "cuobject":
            # cuObject exposes a C++ callback interface, not a Python SDK.
            # The native bridge supplies the RDMA token to a storage control
            # callback; the callback must verify the server's RDMA reply.
            from fsspec.implementations.cuda_cuobject import readinto

            stream.sync()
            count = readinto(path, out, offset, **self.backend_options)
            self._retain(stream, (out,))
            return count
        if method == "kvikio":
            import kvikio

            protocol = self.fs.protocol
            protocols = (protocol,) if isinstance(protocol, str) else protocol
            if "s3" in protocols:
                bucket, key = path.removeprefix("s3://").split("/", 1)
                handle = kvikio.RemoteFile.open_s3(bucket, key, **self.backend_options)
            elif "http" in protocols or "https" in protocols:
                handle = kvikio.RemoteFile.open_http(path, **self.backend_options)
            else:
                raise ValueError("kvikio method requires HTTP or S3")
            # RemoteFile has no CUDA stream API. Its future must complete on
            # the host before data can be consumed on the selected stream.
            stream.sync()
            with handle:
                count = handle.pread(
                    _KvikioBuffer(out, stream), size=size, file_offset=offset
                ).get()
            if count != size:
                raise OSError(f"Short remote read: expected {size}, got {count}")
            self._retain(stream, (out,))
            return count
        host = LegacyPinnedMemoryResource().allocate(size, stream=stream)
        host_view = _host_view(host)
        if method == "bounce":
            import kvikio

            if self.fs.protocol not in ("file", "local", ("file", "local")):
                raise ValueError("bounce requires a local filesystem")
            with kvikio.CuFile(path, "r") as handle:
                count = handle.pread(host_view, size=size, file_offset=offset).get()
        else:
            with self.fs.open(path, "rb") as file:
                file.seek(offset)
                count = 0
                while count < size:
                    n = file.readinto(host_view[count:])
                    if not n:
                        break
                    count += n
        if count != size:
            raise OSError(f"Short read: expected {size}, got {count}")
        out.copy_from(host, stream=stream)
        self._retain(stream, (host, out))
        return count

    def cat_file(self, path, start=None, end=None, stream=None, **kwargs):
        path = self._path(path)
        size = self.fs.size(path)
        start, end, _ = slice(start, end).indices(size)
        stream = self._stream(stream)
        out = stream.device.allocate(max(0, end - start), stream=stream)
        self._readinto(path, out, start, stream)
        return out

    def open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        compression=None,
        stream=None,
        **kwargs,
    ):
        if compression is not None:
            raise NotImplementedError("CUDA file compression is not implemented")
        if mode not in {"rb", "wb", "xb", "ab"}:
            raise ValueError("CUDA files support rb, wb, xb, and ab modes")
        if kwargs.get("autocommit") is False or self._intrans:
            raise NotImplementedError("CUDA file transactions are not implemented")
        return CUDAFile(self, self._path(path), mode, self._stream(stream))

    def read_block(self, fn, offset, length, delimiter=None):
        if delimiter is not None:
            raise NotImplementedError("CUDA delimiter reads are not implemented")
        return self.cat_file(fn, offset, None if length is None else offset + length)

    # These implementations route all byte transfers through our CUDA methods.
    cat = AbstractFileSystem.cat
    cat_ranges = AbstractFileSystem.cat_ranges
    pipe = AbstractFileSystem.pipe
    pipe_file = AbstractFileSystem.pipe_file


class CUDAFile(io.RawIOBase):
    """Binary file whose read/readinto/write buffers reside on a CUDA device."""

    def __init__(self, fs, path, mode, stream):
        super().__init__()
        self.fs, self.path, self.mode, self.stream = fs, path, mode, stream
        self.name = path
        self.loc = 0
        self._file = None
        if mode == "rb":
            self.size = fs.fs.size(path)
        else:
            self._file = fs.fs.open(path, mode)
            self.loc = self._file.tell()
            self.size = self.loc

    def readable(self):
        return self.mode == "rb" and not self.closed

    def writable(self):
        return self.mode != "rb" and not self.closed

    def seekable(self):
        return True

    def tell(self):
        self._checkClosed()
        return self.loc

    def seek(self, offset, whence=0):
        self._checkClosed()
        if whence not in (0, 1, 2):
            raise ValueError("Invalid whence")
        loc = offset + (0 if whence == 0 else self.loc if whence == 1 else self.size)
        if loc < 0:
            raise ValueError("Negative seek position")
        if self._file is not None:
            loc = self._file.seek(offset, whence)
        self.loc = loc
        return loc

    def read(self, size=-1):
        self._checkClosed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        count = max(0, self.size - self.loc)
        if size is not None and size >= 0:
            count = min(count, size)
        out = self.stream.device.allocate(count, stream=self.stream)
        self.readinto(out)
        return out

    read1 = read
    readall = read

    def readinto(self, buffer):
        self._checkClosed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        out = self.fs._device_bytes(buffer, self.stream)
        count = min(out.size, max(0, self.size - self.loc))
        self.fs._readinto(
            self.path, _buffer_view(out, size=count), self.loc, self.stream
        )
        self.loc += count
        return count

    readinto1 = readinto

    def write(self, buffer):
        from cuda.core import LegacyPinnedMemoryResource

        self._checkClosed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        data = self.fs._device_bytes(buffer, self.stream, writable=False)
        if data.size == 0:
            return 0
        host = LegacyPinnedMemoryResource().allocate(data.size, stream=self.stream)
        host.copy_from(data, stream=self.stream)
        # A synchronous fsspec writer cannot consume a pending D2H copy.
        self.stream.sync()
        count = self._file.write(_host_view(host))
        self.loc += count
        self.size = max(self.loc, self.size)
        return count

    def readline(self, size=-1):
        raise io.UnsupportedOperation("CUDA line reads are not implemented")

    def flush(self):
        if self._file is not None and not self._file.closed:
            self._file.flush()

    def close(self):
        if not self.closed:
            try:
                super().close()
            finally:
                if self._file is not None:
                    self._file.close()


def _delegate(name):
    def method(self, path, *args, **kwargs):
        return getattr(self.fs, name)(self._path(path), *args, **kwargs)

    method.__name__ = name
    return method


def _delegate_pair(name):
    def method(self, path1, path2, *args, **kwargs):
        return getattr(self.fs, name)(
            self._path(path1), self._path(path2), *args, **kwargs
        )

    method.__name__ = name
    return method


for _name in (
    "ls",
    "info",
    "size",
    "exists",
    "isfile",
    "isdir",
    "glob",
    "find",
    "walk",
    "du",
    "mkdir",
    "makedirs",
    "rmdir",
    "rm",
    "rm_file",
    "touch",
    "created",
    "modified",
    "checksum",
    "ukey",
    "sign",
):
    setattr(CUDAFileSystem, _name, _delegate(_name))
for _name in ("mv", "copy", "cp_file"):
    setattr(CUDAFileSystem, _name, _delegate_pair(_name))
