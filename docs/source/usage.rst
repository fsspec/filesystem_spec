Usage
=====

This is quick-start documentation to help people get familiar with the layout and functioning of ``fsspec``.

Instantiate a file-system
-------------------------

``fsspec`` provides an abstract file-system interface as a base class, to be used by other filesystems.
A file-system instance is an object for manipulating files on some
remote store, local files, files within some wrapper, or anything else that is capable of producing
file-like objects.

Some concrete implementations are bundled with ``fsspec`` and others can be installed separately. They
can be instantiated directly, or the ``registry`` can be used to find them.

Direct instantiation using the name of the class such as ``LocalFileSystem``, ``ZipFileSystem`` or
``S3FileSystem``:

.. code-block:: python

    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()

Look-up via registry:

.. code-block:: python

    import fsspec

    fs = fsspec.filesystem('file')

The argument passed here is the protocol name which maps across to the corresponding implementation
class ``LocalFileSystem``. Other examples are ``zip`` which maps across to ``ZipFileSystem`` and
``s3`` which maps across to ``S3FileSystem``.

Many filesystems also take extra parameters, some of which may be options - see :doc:`api`, or use
:func:`fsspec.get_filesystem_class` to get the class object and inspect its docstring.

.. code-block:: python

    import fsspec

    fs = fsspec.filesystem('ftp', host=host, port=port, username=user, password=pw)

The list of implemented ``fsspec`` protocols can be retrieved using :func:`fsspec.available_protocols`.

.. note::

   The full list of the available protocols and the implementations that they map across to is
   divided into two sections:

    - Implementations built into ``fsspec``: :ref:`implementations`
    - Implementations in separate packages: :ref:`external_implementations`

Use a file-system
-----------------

File-system instances offer a large number of methods for getting information about and manipulating files
for the given back-end. Although some specific implementations may not offer all features (e.g., ``http``
is read-only), generally all normal operations, such as ``ls``, ``rm``,  should be expected to work (see the
full list: :class:`fsspec.spec.AbstractFileSystem`).
Note that this quick-start will prefer posix-style naming, but
many common operations are aliased: ``cp()`` and ``copy()`` are identical, for instance.
Functionality is generally chosen to be as close to the builtin ``os`` module's working for things like
``glob`` as possible. The following block of operations should seem very familiar.

.. code-block:: python

    fs.mkdir("/remote/output")
    fs.touch("/remote/output/success")  # creates empty file
    assert fs.exists("/remote/output/success")
    assert fs.isfile("/remote/output/success")
    assert fs.cat("/remote/output/success") == b""  # get content as bytestring
    fs.copy("/remote/output/success", "/remote/output/copy")
    assert fs.ls("/remote/output", detail=False) == ["/remote/output/success", "/remote/output/copy")
    fs.rm("/remote/output", recursive=True)

The ``open()`` method will return a file-like object which can be passed to any other library that expects
to work with python files, or used by your own code as you would a normal python file object.
These will normally be binary-mode only, but may implement internal buffering
in order to limit the number of reads from a remote source. They respect the use of ``with`` contexts. If
you have ``pandas`` installed, for example, you can do the following:

.. code-block:: python

    f = fs.open("/remote/path/notes.txt", "rb")
    lines = f.readline()  # read to first b"\n"
    f.seek(-10, 2)
    foot = f.read()  # read last 10 bytes of file
    f.close()

    import pandas as pd
    with fs.open('/remote/data/myfile.csv') as f:
        df = pd.read_csv(f, sep='|', header=None)

Higher-level
------------

For many situations, the only function that will be needed is :func:`fsspec.open_files()`, which will return
:class:`fsspec.core.OpenFile` instances created from a single URL and parameters to pass to the backend(s).
This supports text-mode and compression on the fly, and the objects can be serialized for passing between
processes or machines (so long as each has access to the same backend file-system). The protocol (i.e.,
backend) is inferred from the URL passed, and glob characters are expanded in read mode (search for files)
or write mode (create names). Critically, the file on the backend system is not actually opened until the
``OpenFile`` instance is used in a ``with`` context.

.. code-block:: python

    of = fsspec.open("github://dask:fastparquet@main/test-data/nation.csv", "rt")
    # of is an OpenFile container object. The "with" context below actually opens it
    with of as f:
        # now f is a text-mode file
        for line in f:
            # iterate text lines
            print(line)
            if "KENYA" in line:
                break

Compressed files
----------------

Use the ``compression`` argument to :func:`fsspec.open` to read or write an
individual compressed file. This wraps the file object; it does not compress the
whole file-system. For example, write a gzip-compressed text file and read it back:

.. code-block:: python

    import fsspec

    with fsspec.open("example.txt.gz", "wt", compression="gzip", encoding="utf-8") as f:
        f.write("Hello from fsspec!\n")

    with fsspec.open("example.txt.gz", "rt", compression="infer", encoding="utf-8") as f:
        assert f.read() == "Hello from fsspec!\n"

``compression="infer"`` selects a registered codec from the filename suffix,
such as ``.gz`` for gzip. Compression is not inferred by default:
``compression=None`` leaves the bytes unchanged, even if the filename ends in
``.gz``. The same ``compression`` argument is available on :func:`fsspec.open_files`.

Available codecs depend on your Python installation and optional packages.
Call :func:`fsspec.available_compressions` to see the codecs registered in the
current process. For example, ``"gzip"`` is a codec name, while ``None`` means no
compression.

For Zstandard (``compression="zstd"``, or ``compression="infer"`` with a ``.zst``
filename), fsspec first tries the standard-library ``compression.zstd`` module
on Python 3.14 and later. On older Python versions, install ``backports.zstd``:

.. code-block:: console

    python -m pip install backports.zstd

If that backend is unavailable, fsspec also supports the ``zstandard`` package
as a fallback. Other optional codecs include ``lz4`` (install ``lz4``) and
``snappy`` (install ``python-snappy``). Install optional packages before importing
fsspec, or restart your Python process after installation so the codecs can be
registered.

Explicitly requesting an unavailable codec raises ``ValueError`` with a message
such as ``Compression type zstd not supported``. Inference only recognizes
suffixes registered by available codecs: if a suffix is not recognized,
``compression="infer"`` passes through the stored bytes without compression or
decompression. Use an explicit codec name
when you want a missing dependency to produce an error.

.. raw:: html

    <script data-goatcounter="https://fsspec.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
