Developing with fsspec
----------------------

Whereas the majority of the documentation describes the use of ``fsspec``
from the end-user's point of view, ``fsspec`` is used by many libraries
as the primary/only interface to file operations.

Clients of the library
~~~~~~~~~~~~~~~~~~~~~~

The most common entrance point for libraries which wish to rely on ``fsspec``
will be ``open`` or ``open_files``, as a way of generating an object compatible
with the python file interface. This actually produces an ``OpenFile`` instance,
which can be serialised across a network, and resources are only engaged when
entering a context, e.g.

.. code-block:: python

    with fsspec.open("protocol://path", 'rb', param=value) as f:
        process_file(f)

Note the backend-specific parameters that can be passed in this call.

In cases where the caller wants to control the context directly, they can use the
``open`` method of the ``OpenFile``, or get the filesystem object directly,
skipping the ``OpenFile`` route. In the latter case, text encoding and compression
are **not** handled for you. The file-like object can also be used as a context
manager, or the ``close()`` method must be called explicitly to release resources.

.. code-block:: python

    # OpenFile route
    of = fsspec.open("protocol://path", 'rb', param=value)
    f = of.open()
    process_file(f)
    f.close()

    # filesystem class route, context
    fs = fsspec.filesystem("protocol", param=value)
    with fs.open("path", "rb") as f:
        process_file(f)

    # filesystem class route, explicit close
    fs = fsspec.filesystem("protocol", param=value)
    f = fs.open("path", "rb")
    process_file(f)
    f.close()

Implementing a backend
~~~~~~~~~~~~~~~~~~~~~~

The class ``AbstractFileSystem`` provides a template of the methods
that a potential implementation should supply, as well as default
implementation of functionality that depends on these. Methods that
*could* be implemented are marked with ``NotImplementedError`` or
``pass`` (the latter specifically for directory operations that might
not be required for some backends where directories are emulated.

Note that not all of the methods need to be implemented: for example,
some implementations may be read-only, in which case things like ``pipe``,
``put``, ``touch``, ``rm``, etc., can be left as not-implemented
(or you might implement them and raise PermissionError, OSError 30 or some
read-only exception).

We may eventually refactor ``AbstractFileSystem`` to split the default implementation,
the set of methods that you might implement in a new backend, and the
documented end-user API.

In order to register a new backend with fsspec, new backends should register
themselves using the `entry_points <https://setuptools.readthedocs.io/en/latest/userguide/quickstart.html#entry-points-and-automatic-script-creation>`_
facility from setuptools. In particular, if you want to register a new
filesystem protocol ``myfs`` which is provided by the ``MyFS`` class in
the ``myfs`` package, add the following to your ``setup.py``:

.. code-block:: python

    setuptools.setup(
        ...
        entry_points={
            'fsspec.specs': [
                'myfs=myfs.MyFS',
            ],
        },
        ...
    )


Alternatively, the previous method of registering a new backend can be used.
That is, new backends must register themselves on import
(``register_implementation``) or post a PR to the ``fsspec`` repo
asking to be included in ``fsspec.registry.known_implementations``.

Implementing async
~~~~~~~~~~~~~~~~~~

Starting in version 0.7.5, we provide async operations for some methods
of some implementations. Async support in storage implementations is
optional. Special considerations are required for async
development, see :doc:`async`.

Developing the library
~~~~~~~~~~~~~~~~~~~~~~

The following can be used to install ``fsspec`` in development mode

.. code-block::

   git clone https://github.com/fsspec/filesystem_spec
   cd filesystem_spec
   pip install -e .

A number of additional dependencies are required to run tests, see "ci/environment*.yml", as
well as Docker. Most implementation-specific tests should skip if their requirements are
not met.

Development happens by submitting pull requests (PRs) on github.
This repo adheres to flake8 and black coding conventions. You may wish to install
commit hooks if you intend to make PRs, as linting is done as part of the CI.

Docs use sphinx and the numpy docstring style. Please add an entry to the changelog
along with any PR.

.. raw:: html

    <script data-goatcounter="https://fsspec.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
