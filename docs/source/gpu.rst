Experimental CUDA buffer I/O
===========================

``cuda+file``, ``cuda+http``, ``cuda+https``, and ``cuda+s3`` wrap the existing
filesystems and change byte reads to return one-dimensional CuPy ``uint8`` arrays.
The prefix works with other registered protocols as well. CuPy and a working CUDA
runtime are needed for transfers, but are not imported for metadata operations.
KvikIO is optional except for methods that explicitly select it.

.. code-block:: python

   import cupy as cp
   import fsspec

   stream = cp.cuda.Stream(non_blocking=True)
   fs = fsspec.filesystem("cuda+file", method="pinned", stream=stream)
   data = fs.cat_file("/data/example.bin", start=4096, end=8192)
   with stream:
       # Work submitted here follows the transfer.
       result = data + 1
   fs.synchronize()  # also checks deferred I/O errors

   with fs.open("/data/example.bin", "rb") as file:
       file.seek(32)
       destination = cp.empty(100, dtype="u1")
       count = file.readinto(destination)
   fs.synchronize()

``cat``, ``cat_file``, ``cat_ranges``, ``read_block`` without delimiters,
``read``, ``read1``, ``readall``, ``readinto``, and ``readinto1`` use CUDA buffers.
``readinto`` accepts writable C-contiguous objects implementing the CUDA array
interface and returns the byte count. Reads at EOF produce empty arrays or zero.
Negative ``cat_file`` bounds follow Python slice semantics.

``pipe``, ``pipe_file`` and binary ``write`` consume CUDA buffers. Writes currently
stage through pinned host memory and wait for the device-to-host copy before
calling the underlying writer; they are synchronous. Binary modes ``rb``, ``wb``,
``ab`` and ``xb`` are supported. Text, line reads, compression, transactions,
and delimiter scanning are outside this prototype.

Directory listing, globbing, stat, copying, moving, deletion and other metadata
operations use the wrapped filesystem. Host upload/download methods retain their
usual fsspec behavior. CPU byte consumers cannot use CUDA file objects unchanged.

Configuration and streams
-------------------------

Use ``stream=`` for a CuPy stream or a raw stream pointer, or ``streams=`` for a
sequence of streams or a positive integer creating a pool of nonblocking streams.
Pools select streams round-robin per ``cat_file`` or opened file. Without either
option the calling thread's current CuPy stream is used. Use one filesystem per
device and do not change its device while transfers are pending. External stream
owners must keep those streams alive. Per-call ``stream=`` overrides the selection.

Options are read from fsspec's existing ``cuda`` and ``cuda+<protocol>`` config
sections. Explicit constructor/storage options take precedence:

.. code-block:: python

   from fsspec.config import conf
   conf["cuda"] = {"method": "pinned", "streams": 4}
   fs, path = fsspec.core.url_to_fs("cuda+file:///data/example.bin")

A JSON config can contain ``{"cuda": {"method": "pinned", "streams": 4}}``.
Live stream objects belong in Python storage options; raw stream pointer integers
are only valid in their originating process. Pass underlying credentials/options
with ``target_options=`` (or as additional constructor keywords).
``backend_options=`` holds KvikIO remote factory options or cuObject bridge options.
Underlying S3 filesystem credentials and KvikIO credentials are separate; supply
both explicitly or configure the corresponding environment variables.

Wrapping an existing filesystem is supported with
``CUDAFileSystem(fs=existing, method="pinned")``; URL chaining also works, for
example ``cuda::s3://bucket/key``.

Completion and ownership
------------------------

This is CUDA stream concurrency, not an asyncio interface. Returned arrays may
still have pending transfers. Consume them on the selected stream or call
``fs.wait(consumer_stream)`` before using them on a different stream. A host wait
via ``fs.synchronize()`` or ``fs.close()`` completes submitted reads and checks
cuFile byte counts. A file's ``close()`` does not wait for pending reads.

The filesystem retains destination buffers, pinned staging allocations, streams,
cuFile handles and futures until completion. Completed transfers are reclaimed
on subsequent reads; synchronization releases the remaining resources. Keep the
filesystem alive through submission and call ``synchronize`` explicitly to handle
errors. Waiting on a stream alone does not check cuFile's deferred I/O errors.
Avoid concurrent mutation of destination buffers and source files while reading.
There is currently no bounded staging pool or backpressure mechanism.

Methods
-------

``pinned`` (default)
    Read using the ordinary fsspec file's ``readinto`` into page-locked host
    memory, then enqueue an asynchronous host-to-device copy. Works for all
    wrapped protocols, including local, HTTP, S3 and memory. The host read itself
    blocks; only the device copy is asynchronous.

``cufile``
    Local files only. Use KvikIO ``CuFile.raw_read_async`` on the chosen CUDA
    stream. Set KvikIO compatibility mode OFF while constructing the handle,
    restoring its previous setting immediately afterward. Direct I/O errors are
    surfaced rather than silently selecting POSIX fallback. Requires working
    GDS and cuFile stream support. Alignment, driver configuration and filesystem
    support determine whether the hardware supports a given request; cuFile may
    internally stage unaligned requests. The prototype does not claim that every
    byte bypasses a bounce buffer.

``bounce``
    Local files only. Use KvikIO ``CuFile.pread`` into pinned host memory, wait
    for that host future, then enqueue a CUDA copy. This provides an explicit
    host bounce path, including on systems without GDS. ``pinned`` selects the
    equivalent standard-fsspec host read instead.

``kvikio``
    HTTP/HTTPS and S3. Use ``RemoteFile.open_http`` or ``RemoteFile.open_s3``
    and ``pread`` into device memory. RemoteFile exposes host futures rather
    than a CUDA stream submission API: this prototype synchronizes the selected
    stream before submission and waits for remote completion on the host before
    returning. Thus remote transfers are correct but do not yet overlap with
    work on that stream. Select ``pinned`` for standard-fsspec remote reads
    followed by an asynchronous CUDA copy.

``cuobject``
    Use the optional native bridge below. No pinned fallback. Native RDMA
    completion currently blocks the host; it is ordered after the selected
    stream's prior work. Requires a compatible storage service, RDMA networking
    and CUDA Toolkit cuObject client libraries.

cuObject bridge
---------------

cuObject provides a C++ client and requires the storage SDK to send its RDMA
metadata in the control request. It does not expose a generic Python S3 client.
``examples/cuda/cuobject_bridge.cpp`` provides a minimal C ABI, compiled separately
from the dependency-free fsspec package. For a CUDA Toolkit conda environment:

.. code-block:: sh

   CUDA_TARGET="$CONDA_PREFIX/targets/x86_64-linux"
   c++ -std=c++17 -shared -fPIC examples/cuda/cuobject_bridge.cpp \
       -I"$CUDA_TARGET/include" -L"$CUDA_TARGET/lib" \
       -Wl,-rpath,"$CUDA_TARGET/lib" -lcuobjclient -o /tmp/fsspec_cuobject.so

Configure with:

.. code-block:: python

   fs = fsspec.filesystem(
       "cuda+s3",
       method="cuobject",
       backend_options={
           "library": "/tmp/fsspec_cuobject.so",
           "control": signed_rdma_get,
       },
       target_options={"endpoint_url": "https://storage.example"},
   )

The storage-specific callback ``signed_rdma_get(path, token, size, offset)`` must
send a signed range GET for the given object, adding ``x-amz-rdma-token`` from
``token`` and requesting exactly ``size`` bytes at ``offset``. It must wait for
completion, verify the server's successful ``x-amz-rdma-reply``, and return the
actual byte count. It must raise if the server declines RDMA; an ordinary HTTP
response body cannot fill the GPU allocation. Signing, authentication and reply
interpretation belong to that storage SDK. This callback must support invocation
from native worker threads. Callback exceptions propagate to the Python caller.
The native bridge keeps the cuObject client alive throughout the operation.

The bridge can be compiled locally without an RDMA endpoint; testing an actual
transfer requires a compatible service. See the
`NVIDIA cuObject documentation <https://docs.nvidia.com/gpudirect-storage/cuobject/>`_
and `KvikIO API <https://docs.rapids.ai/api/kvikio/stable/api/>`_ for prerequisites.
