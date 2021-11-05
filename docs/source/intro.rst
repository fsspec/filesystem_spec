Background
==========

Python provides a standard interface for open files, so that alternate implementations of file-like object can
work seamlessly with many function which rely only on the methods of that standard interface. A number of libraries
have implemented a similar concept for file-systems, where file operations can be performed on a logical file-system
which may be local, structured data store or some remote service.

This repository is intended to be a place to define a standard interface that such file-systems should adhere to,
such that code using them should not have to know the details of the implementation in order to operate on any of
a number of backends. With hope, the community can come together to
define an interface that is the best for the highest number of users, and having the specification, makes developing
other file-system implementations simpler.

History
-------

We have been involved in building a number of remote-data file-system implementations, principally
in the context of the `Dask`_ project. In particular, several are listed
in `docs`_ with links to the specific repositories.
With common authorship, there is much that is similar between the implementations, for example posix-like naming
of the operations, and this has allowed Dask to be able to interact with the various backends and parse generic
URLs in order to select amongst them. However, *some* extra code was required in each case to adapt the peculiarities
of each implementation with the generic usage that Dask demanded. People may find the
`code`_ which parses URLs and creates file-system
instances interesting.

.. _Dask: http://dask.pydata.org/en/latest/
.. _docs: http://dask.pydata.org/en/latest/remote-data-services.html
.. _code: https://github.com/dask/dask/blob/master/dask/bytes/core.py#L266

At the same time, the Apache `Arrow`_ project was also concerned with a similar problem,
particularly a common interface to local and HDFS files, for example the
`hdfs`_ interface (which actually communicated with HDFS
with a choice of driver). These are mostly used internally within Arrow, but Dask was modified in order to be able
to use the alternate HDFS interface (which solves some security issues with `hdfs3`). In the process, a
`conversation`_
was started, and I invite all interested parties to continue the conversation in this location.

.. _Arrow: https://arrow.apache.org/
.. _hdfs: https://arrow.apache.org/docs/python/filesystems.html
.. _conversation: https://github.com/dask/dask/issues/2880

There is a good argument that this type of code has no place in Dask, which is concerned with making graphs
representing computations, and executing those graphs on a scheduler. Indeed, the file-systems are generally useful,
and each has a user-base wider than just those that work via Dask.

Influences
----------

The following places to consider, when choosing the definitions of how we would like the file-system specification
to look:

#. python's `os`_ module and its `path` namespace; also other file-connected
   functionality in the standard library
#. posix/bash method naming conventions that linux/unix/osx users are familiar with; or perhaps their Windows variants
#. the existing implementations for the various backends (e.g.,
   `gcsfs`_ or Arrow's
   `hdfs`_)
#. `pyfilesystems`_, an attempt to do something similar, with a
   plugin architecture. This conception has several types of local file-system, and a lot of well-thought-out
   validation code.

.. _os: https://docs.python.org/3/library/os.html
.. _gcsfs: http://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem
.. _pyfilesystems: https://docs.pyfilesystem.org/en/latest/index.html

Other similar work
------------------

It might have been conceivable to reuse code in ``pyfilesystems``, which has an established interface and several
implementations of its own. However, it supports none of the :ref:`highlight`, critical to
cloud and parallel access, and would not be easy to
coerce. Following on the success of ``s3fs`` and ``gcsfs``, and their use within Dask, it seemed best to
have an interface as close to those as possible. See a
`discussion`_ on the topic.

.. _discussion: https://github.com/fsspec/filesystem_spec/issues/5

Other newer technologies such as `smart_open`_ and ``pyarrow``'s newer file-system rewrite also have some
parts of the functionality presented here, that might suit some use cases better.

.. _smart_open: https://github.com/RaRe-Technologies/smart_open

Structure of the package
------------------------

The best place to get a feel for the contents of ``fsspec`` is by looking through the :doc:`usage` and
:doc:`api` sections. In addition, the source code will be interesting for those who wish to subclass and
develop new file-system implementations. ``fsspec/spec.py`` contains the main abstract file-system class
to derive from, ``AbstractFileSystem``.

.. _zarr: https://zarr.readthedocs.io
