``fsspec``: Filesystem interfaces for Python
======================================

Filesystem Spec (``fsspec``) is a project to provide a unified pythonic interface to
local, remote and embedded file systems and bytes storage.

Brief Overview
--------------

There are many places to store bytes, from in memory, to the local disk, cluster
distributed storage, to the cloud. Many files also contain internal mappings of names to bytes,
maybe in a hierarchical directory-oriented tree. Working with all these different
storage media, and their associated libraries, is a pain. ``fsspec`` exists to
provide a familiar API that will work the same whatever the storage backend.
As much as possible, we iron out the quirks specific to each implementation,
so you need do no more than provide credentials for each service you access
(if needed) and thereafter not have to worry about the implementation again.

Why
---

``fsspec`` provides two main concepts: a set of filesystem classes with uniform APIs
(i.e., functions such as ``cp``, ``rm``, ``cat``, ``mkdir``, ...) supplying operations on a range of
storage systems; and top-level convenience functions like :func:`fsspec.open`, to allow
you to quickly get from a URL to a file-like object that you can use with a third-party
library or your own code.

The section :doc:`background` gives motivation and history of this project, but
most users will want to skip straight to :doc:`usage` to find out how to use
the package and :doc:`features` to see the long list of added functionality
included along with the basic file-system interface.


Who uses ``fsspec``?
--------------------

You can use ``fsspec``'s file objects with any python function that accepts
file objects, because of *duck typing*.

You may well be using ``fsspec`` already without knowing it.
The following libraries use ``fsspec`` internally for path and file handling:

#. `Dask`_, the parallel, out-of-core and distributed
   programming platform
#. `Intake`_, the data source cataloguing and loading
   library and its plugins
#. `pandas`_, the tabular data analysis package
#. `xarray`_ and `zarr`_, multidimensional array
   storage and labelled operations
#. `DVC`_, version control system
   for machine learning projects

``fsspec`` filesystems are also supported by:

#. `pyarrow`_, the in-memory data layout engine

... plus many more that we don't know about.

.. _Dask: https://dask.org/
.. _Intake: https://intake.readthedocs.io/
.. _pandas: https://pandas.pydata.org/
.. _xarray: http://xarray.pydata.org/
.. _zarr: https://zarr.readthedocs.io/
.. _DVC: https://dvc.org/
.. _pyarrow: https://arrow.apache.org/docs/python/


Installation
------------

`fsspec` can be installed from PyPI or conda and has no dependencies of its own

.. code-block:: sh

   pip install fsspec
   conda install -c conda-forge fsspec

Not all filesystem implementations are available without installing extra
dependencies. For example to be able to access data in S3, you can use the optional
pip install syntax below, or install the specific package required

.. code-block:: sh

   pip install fsspec[gcs]
   conda install -c conda-forge gcsfs

`fsspec` attempts to provide the right message when you attempt to use a filesystem
for which you need additional dependencies.
The current list of known implementations can be found as follows

.. code-block:: python

    from fsspec.registry import known_implementations

    known_implementations
