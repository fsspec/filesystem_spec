Async
=====



``fsspec`` supports asynchronous operations on certain implementations. This
allows for concurrent calls within bulk operations such as ``cat`` (fetch
the contents of many files at once) even from normal code, and for the direct
use of fsspec in async code without blocking.
Async implementations derive from the class ``fsspec.asyn.AsyncFileSystem``.
The class attribute ``async_impl`` can be used to test whether an
implementation is async of not.

``AsyncFileSystem`` contains ``async def`` coroutine versions of the methods of
``AbstractFileSystem``. By convention, these methods are prefixed with "_"
to indicate that they are not to called directly in normal code, only
when you know what you are doing. In most cases, the code is identical or
slightly modified by replacing sync calls with ``await`` calls to async
functions.

The only async implementation built into ``fsspec`` is ``HTTPFileSystem``.

Synchronous API
---------------

The methods of ``AbstractFileSystem`` are available and can be called from
normal code. They call and wait on the corresponding async function. The
*work* is carried out in a separate threads, so if there are many fsspec
operations in flight at once, launched from many threads, they will still
all be processed on the same IO-dedicated thread.

Most users should not be aware that their code is running async.

Note that the sync functions are wrapped using ``sync_wrapper``, which
copies the docstrings from ``AbstractFileSystem``, unless they are
explicitly given in the implementation.

Example:

.. code-block:: python

    fs = fsspec.filesystem("http")
    out = fs.cat([url1, url2, url3])  # fetches data concurrently

Coroutine batching
------------------

The various methods which create many coroutines to be passed to the event loop
for processing may be batched: submitting a certain number in one go and waiting
for them to complete before launching more. This is important to work around
local open-file limits (which can be <~100) and not to swamp the heap.

``fsspec.asyn._run_coros_in_chunks`` controls this process, but from the user's point
of view, there are three ways to affect it. In increasing order or precedence:

    - the global variables ``fsspec.asyn._DEFAULT_BATCH_SIZE`` and
      ``fsspec.asyn._NOFILES_DEFAULT_BATCH_SIZE`` (for calls involving local files or not,
      respectively)

    - config keys "gather_batch_size" and "nofiles_gather_batch_size"

    - the ``batch_size`` keyword, accepted by the batch methods of an async filesystem.


Using from Async
----------------

File system instances can be created with ``asynchronous=True``. This
implies that the instantiation is happening within a coroutine, so
the various async method can be called directly with ``await``, as is
normal in async code.

Note that, because ``__init__`` is a blocking function, any creation
of asynchronous resources will be deferred. You will normally need to
explicitly ``await`` a coroutine to create them. Since garbage collection
also happens in blocking code, you may wish to explicitly await
resource destructors too. Example:

.. code-block:: python

    async def work_coroutine():
        fs = fsspec.filesystem("http", asynchronous=True)
        session = await fs.set_session()  # creates client
        out = await fs._cat([url1, url2, url3])  # fetches data concurrently
        await session.close()  # explicit destructor

    asyncio.run(work_coroutine())

Bring your own loop
-------------------

For the non-asynchronous case, ``fsspec`` will normally create an asyncio
event loop on a specific thread. However, the calling application may prefer
IO processes to run on a loop that is already around and running (in another
thread). The loop needs to be asyncio compliant, but does not necessarily need
to be an ``ayncio.events.AbstractEventLoop``. Example:

.. code-block:: python

    loop = ...  # however a loop was made, running on another thread
    fs = fsspec.filesystem("http", loop=loop)
    out = fs.cat([url1, url2, url3])  # fetches data concurrently


Implementing new backends
-------------------------

Async file systems should derive from ``AsyncFileSystem``, and implement the
``async def _*`` coroutines there. These functions will either have sync versions
automatically generated is the name is in the ``async_methods`` list, or
can be directly created using ``sync_wrapper``.

.. code-block:: python

   class MyFileSystem(AsyncFileSystem):

       async def _my_method(self):
           ...

       my_method = sync_wrapper(_my_method)


These functions must **not call** methods or functions which themselves are synced,
but should instead ``await`` other coroutines. Calling methods which do not require sync,
such as ``_strip_protocol`` is fine.

Note that ``__init__``, cannot be ``async``, so it might need to allocate async
resources using the ``sync`` function, but *only* if ``asynchronous=False``. If it
is ``True``, you probably need to require the caller to await a coroutine that
creates those resources. Similarly, any destructor (e.g., ``__del__``) will run from normal
code, and possibly after the loop has stopped/closed.

To call ``sync``, you will need to pass the associated event loop, which will be
available as the attribute ``.loop``.

.. autosummary::
   fsspec.asyn.AsyncFileSystem
   fsspec.asyn.get_loop
   fsspec.asyn.sync
   fsspec.asyn.sync_wrapper

.. autoclass:: fsspec.asyn.AsyncFileSystem
   :members:

.. autofunction:: fsspec.asyn.get_loop

.. autofunction:: fsspec.asyn.sync

.. autofunction:: fsspec.asyn.sync_wrapper

.. raw:: html

    <script data-goatcounter="https://fsspec.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>

AsyncFileSystemWrapper
----------------------

The `AsyncFileSystemWrapper` class is an experimental feature that allows you to convert
a synchronous filesystem into an asynchronous one. This is useful for quickly integrating
synchronous filesystems into workflows that may expect `AsyncFileSystem` instances.

Basic Usage
~~~~~~~~~~~

To use `AsyncFileSystemWrapper`, wrap any synchronous filesystem to work in an asynchronous context.
In this example, the synchronous `LocalFileSystem` is wrapped, creating an `AsyncFileSystem` instance
backed by the normal, synchronous methods of `LocalFileSystem`:

.. code-block:: python

    import asyncio
    import fsspec
    from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

    async def async_copy_file():
        sync_fs = fsspec.filesystem('file')  # by-default synchronous, local filesystem
        async_fs = AsyncFileSystemWrapper(sync_fs)
        return await async_fs._copy('/source/file.txt', '/destination/file.txt')

    asyncio.run(async_copy_file())

Limitations
-----------

This is experimental. Users should not expect this wrapper to magically make things faster.
It is primarily provided to allow usage of synchronous filesystems with interfaces that expect
`AsyncFileSystem` instances.
