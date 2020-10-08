FSSPEC: Filesystem interfaces for Python
======================================

Filesystem Spec (FSSPEC) is a project to unify various projects and classes to work with remote filesystems and
file-system-like abstractions using a standard pythonic interface.


.. _highlight:

Highlights
----------

- based on s3fs and gcsfs
- ``fsspec`` instances are serializable and can be passed between processes/machines
- the ``OpenFiles`` file-like instances are also serializable
- implementations provide random access, to enable only the part of a file required to be read; plus a template
  to base other file-like classes on
- file access can use transparent compression and text-mode
- any file-system directory can be viewed as a key-value/mapping store
- if installed, all file-system classes also subclass from ``pyarrow.filesystem.FileSystem``, so
  can work with any arrow function expecting such an instance
- writes can be transactional: stored in a temporary location and only moved to the final
  destination when the transaction is committed
- FUSE: mount any path from any backend to a point on your file-system
- cached instances tokenised on the instance parameters

These are described further in the :doc:`features` section.

Installation
------------

   pip install fsspec

Not all included filesystems are usable by default without installing extra
dependencies. For example to be able to access data in S3::

   pip install fsspec[s3]

or

   conda install -c conda-forge fsspec

Implementations
---------------

This repo contains several file-system implementations, see :ref:`implementations`. However,
the external projects ``s3fs`` and ``gcsfs`` depend on ``fsspec`` and share the same behaviours.
``Dask`` and ``Intake`` use ``fsspec`` internally for their IO needs.

The current list of known implementations can be found as follows

.. code-block:: python

    from fsspec.registry import known_implementations
    known_implementations

These are only imported on request, which may fail if a required dependency is missing. The dictionary
:py:class:`fsspec.registry` contains all imported implementations, and can be mutated by user code, if necessary.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   intro.rst
   usage.rst
   features.rst
   api.rst
   changelog.rst
   developer.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
