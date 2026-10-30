Experimental CUDA buffer I/O
===========================

``cuda+file``, ``cuda+http``, ``cuda+https``, and ``cuda+s3`` wrap the existing
filesystems and change byte reads to return ``cuda.core.Buffer`` allocations.
The prefix works with other registered protocols as well. CUDA Core (1.2 or newer)
and a working CUDA runtime are needed for transfers, but are not imported for
metadata operations. Install the Python dependency with ``pip install fsspec[cuda]``.
CuPy is not required. KvikIO is optional except for methods that explicitly select it.

.. code-block:: python

   from cuda.core import Buffer, Device
   import fsspec

   device = Device()
   device.set_current()
   stream = device.create_stream()
   fs = fsspec.filesystem("cuda+file", method="pinned", stream=stream)
   data = fs.cat_file("/data/example.bin", start=4096, end=8192)
   assert isinstance(data, Buffer)
   assert data.size == 4096  # bytes, independent of any dtype or shape
   # A device copy submitted here follows the read on the same stream.
   result = data.copy_to(stream=stream)
   fs.synchronize()  # also checks deferred I/O errors

   with fs.open("/data/example.bin", "rb") as file:
       file.seek(32)
       destination = device.allocate(100, stream=stream)
       count = file.readinto(destination)
   fs.synchronize()

``cat``, ``cat_file``, ``cat_ranges``, ``read_block`` without delimiters,
``read``, ``read1``, ``readall``, ``readinto``, and ``readinto1`` use CUDA buffers.
``readinto`` accepts device-accessible ``Buffer`` objects and returns the byte
count. Writable C-contiguous CUDA Array Interface and DLPack objects are also
accepted, without converting them through an array library. Foreign object
producer streams are ordered before the I/O stream through CUDA Core's interop
utilities. Native Buffer inputs must already be ready for use on the selected
stream. Reads at EOF produce zero-size buffers or a zero byte count.
Negative ``cat_file`` bounds follow Python slice semantics.

Buffers expose their byte size as ``.size`` and pointer as ``int(buffer.handle)``.
For a zero-copy byte range, retain the allocation through ``owner=``:

.. code-block:: python

   subset = Buffer.from_handle(int(data.handle) + 128, 256, owner=data)

Array libraries can import the allocation through DLPack to apply their own dtype
and shape interpretation. ``Buffer`` itself does not supply array arithmetic.

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

Use ``stream=`` for a CUDA Core stream, an object implementing ``__cuda_stream__``,
or a raw stream pointer. Use ``streams=`` for a
sequence of streams or a positive integer creating a pool of nonblocking streams.
Pools select streams round-robin per ``cat_file`` or opened file. Without either
option the device's CUDA Core default stream is used: the legacy default unless
``CUDA_PYTHON_CUDA_PER_THREAD_DEFAULT_STREAM`` selects the per-thread default.
There is no implicit selection of an array library's current stream. Use one
filesystem per device and do not change its device while transfers are pending.
Raw stream pointer owners must keep those streams alive. Per-call ``stream=``
overrides the selection.

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

This is CUDA stream concurrency, not an asyncio interface. Returned buffers may
still have pending transfers. Consume them on the selected stream or call
``fs.wait(consumer_stream)`` before using them on a different stream. A host wait
via ``fs.synchronize()`` or ``fs.close()`` completes submitted reads and checks
cuFile byte counts. A file's ``close()`` does not wait for pending reads.
``fs.wait()`` without an argument orders the device's CUDA Core default stream.

Returned allocations are owned by CUDA Core memory resources, with deallocation
ordered on their allocation stream. Keep a buffer alive until all consumers have
finished, including consumers on other streams. To release it asynchronously on
a different consumer stream, use ``buffer.set_deallocation_stream(consumer_stream)``
after establishing the required read-to-consumer ordering. Non-owning byte views
keep their allocation owners alive; do not explicitly close the owner while a
view still needs it.

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

Caching
-------

Whole files on local storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Wrap a ``filecache`` or ``simplecache`` filesystem to reuse its local files.
``cache_files`` proactively downloads complete files and returns their local
filenames; it finishes before returning and does not initialize CUDA. Repeated
paths are downloaded once. Paths are literal filenames, without glob expansion.

.. code-block:: python

   from fsspec.implementations.cuda import CUDAFileSystem

   disk = fsspec.filesystem(
       "filecache",
       target_protocol="s3",
       cache_storage="/local/nvme/fsspec-cache",
       check_files=True,
   )
   fs = CUDAFileSystem(fs=disk, method="cufile", stream=stream)
   paths = ["bucket/a.bin", "bucket/b.bin"]
   local_paths = fs.cache_files(paths)  # synchronous remote-to-local download
   data = fs.cat_file(paths[0], start=4096, end=8192)
   fs.synchronize()

Alternatively, construct the same layers with a single chained URL. Pass each
layer's options under its protocol name:

.. code-block:: python

   fs, path = fsspec.core.url_to_fs(
       "cuda::filecache::s3://bucket/a.bin",
       cuda={"method": "cufile", "stream": stream},
       filecache={
           "cache_storage": "/local/nvme/fsspec-cache",
           "check_files": True,
       },
   )
   local_path = fs.cache_files(path)
   data = fs.cat_file(path, start=4096, end=8192)
   fs.synchronize()

The same prefetch operation is available directly as ``disk.cache_files(paths)``.
A single filename returns a string; an iterable returns a list in input order.
Downloads use the existing host download methods. CUDA reads resolve the cached
filename and use local cuFile I/O, so they do not read the payload through Python
host buffers. GDS still requires a supported local filesystem and hardware.
Files missing from the disk cache are also downloaded on first read.

Use ``method="pinned"`` to trial the same cache without GDS, or ``"bounce"``
for the explicit KvikIO host path. ``kvikio`` and ``cuobject`` are remote readers
and cannot be selected with a whole-file cache. Compression is unsupported in
this CUDA composition. Source listings and size queries still use the wrapped
filesystem. Disk expiry and version checking follow the selected cache's policy;
``simplecache`` assumes immutable files. Keep source files stable during reads.
Do not remove or replace cache files while cuFile transfers are pending.

Blocks in device memory
~~~~~~~~~~~~~~~~~~~~~~~

Set ``block_cache_size`` to a positive byte budget to cache blocks on demand in
GPU memory. This works with any CUDA transfer method and can also be combined
with a whole-file disk cache.

.. code-block:: python

   fs = fsspec.filesystem(
       "cuda+s3",
       method="pinned",  # or a configured direct remote transfer method
       stream=stream,
       block_size=4 * 2**20,
       block_cache_size=256 * 2**20,
       cache_check_files=True,
       cache_expiry=3600,
   )
   first = fs.cat_file("bucket/a.bin", 0, 1024)  # fetches the first block
   again = fs.cat_file("bucket/a.bin", 32, 64)  # device-to-device cache hit
   fs.synchronize()
   fs.clear_device_cache("bucket/a.bin")  # omit path to drop all blocks
   fs.close()

The filesystem instance shares an LRU cache across opened files and streams.
Blocks are fetched at ``block_size`` granularity (default 5 MiB), with the final
block clipped to EOF. Returned data is copied into an independent allocation,
and ``readinto`` copies directly into the caller's device buffer. Modifying a
result does not modify cached bytes. Cross-stream hits wait on the fill's CUDA
event. Eviction retains allocations still needed by pending I/O or copies.
Deferred transfer failures clear the device cache when reported.

``block_cache_size=0`` disables device caching, which is the default. Blocks
larger than the budget are used for the current read but are not retained in the
cache. The budget covers cache residency, not result buffers or pending
transfers; call ``synchronize`` periodically to release completed resources.
``close`` synchronizes and clears the device cache. File close does not.

``cache_check_files`` defaults to false. When enabled, the source ``ukey`` is
checked on each nonempty read and forms part of the block key along with file
size, device and offset. Its reliability depends on the source filesystem.
``cache_expiry`` defaults to one week from the block's fill; zero disables expiry.
For external changes, clear the device cache or enable source checks. CUDA writes
and delegated mutation operations clear affected device data. If both cache tiers
are enabled, configure disk-cache validity separately: checking the remote version
for GPU blocks cannot refresh a disk cache configured to serve stale files.
