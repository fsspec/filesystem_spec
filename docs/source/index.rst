fsspec's: python filesystem interfaces
======================================

Filesystem Spec is a project to unify various projects and classes to work with remote filesystems and
file-system-like abstractions using a standard pythonic interface.


.. _highlight:

Highlights
----------

- ``fsspec`` instances are serializable and can be passe between processes/machines
- the ``OpenFiles`` file-like instances are also serializable
- implementations provide random access, to enable only the part of a file required to be read; plus a template
  to base other file-like classes on
- file access can use transparent compression and text-mode
- any file-system directory can be viewed as a key-value/mapping store
- if installed, all file-system classes also subclass from ``pyarrow.filesystem.FileSystem``, so
  can work with any arrow function expecting such an instance
- writes can be transactional: stored in a temporary location and only moved to the final
  destination when the transaction is committed


Installation
------------

   pip install fsspec

or

   conda install -c conda-forge fsspec


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   intro.rst
   usage.rst
   api.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
