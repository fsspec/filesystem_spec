Usage
=====

This is quick-start documentation to help people get familiar with the layout and functioning of ``fsspec``.

Instantiate a file-system
-------------------------

``fsspec`` provides an abstract file-system interface as a template for other filesystems. In this context,
"interface" means an API for working with files on the given file-system, which can mean files on some
remote store, local files, files within some wrapper, or anything else that is capable of producing
file-like objects.

Some concrete implementations are bundled with ``fsspec`` and others can be installed separately. They
can be instantiated directly, or the `registry` can be used to find them.

Direct instantiation:

.. code-block:: python

   from fsspec.implementations.local import LocalFileSystem
   fs = LocalFileSystem()

Look-up via registry:

.. code-block:: python

   import fsspec
   fs = fsspec.filesystem('file')

Many filesystems also take extra parameters, some of which may be options - see :doc:`api`.

.. code-block:: python

   import fsspec
   fs = fsspec.filesystem('ftp', host=host, port=port,
                          username=user, password=pw)

Use a file-system
-----------------

File-system instances offer a large number of methods for getting information about and manipulating files
for the given back-end. Although some specific implementations may not offer all features (e.g., ``http``
is read-only), generally all normal operations, such as ``ls``, ``rm``,  should be expected to work (see the
full list: :class:`fsspec.spec.AbstractFileSystem`).
Note that this quick-start will prefer posix-style naming, but
many common operations are aliased: ``cp()`` and ``copy()`` are identical, for instance.
Functionality is generally chosen to be as close to the builtin ``os`` module's working for things like
``glob`` as possible.

The ``open()`` method will return a file-like object which can be passed to any other library that expects
to work with python files. These will normally be binary-mode only, but may implement internal buffering
in order to limit the number of reads from a remote source. They respect the use of ``with`` contexts. If
you have ``pandas`` installed, for example, you can do the following:

.. code-block:: python

    with fs.open('https://raw.githubusercontent.com/dask/'
                 'fastparquet/master/test-data/nation.csv') as f:
        df = pd.read_csv(f, sep='|', header=None)

Higher-level
------------

For many situations, the only function that will be needed is :func:`fsspec.open_files()`, which will return
:class:`fsspec.core.OpenFile` instances created from a single URL and parameters to pass to the backend.
This supports text-mode and compression on the fly, and the objects can be serialized for passing between
processes or machines (so long as each has access to the same backend file-system). The protocol (i.e.,
backend) is inferred from the URL passed, and glob characters are expanded in read mode (search for files)
or write mode (create names). Critically, the file on the backend system is not actually opened until the
``OpenFile`` instance is used in a ``with`` context. For the example above:

.. code-block:: python

   files = fsspec.open_files('https://raw.githubusercontent.com/dask/'
                             'fastparquet/master/test-data/nation.csv', mode='r')
   # files is a list of not-yet-open objects
   with files[0] as f:
       # now f is a text-mode file
       df = pd.read_csv(f, sep='|', header=None)

