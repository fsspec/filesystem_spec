API Reference
=============

.. currentmodule:: fsspec

User Functions
--------------

.. autosummary::
   fsspec.open_files
   fsspec.open
   fsspec.filesystem
   fsspec.get_filesystem_class
   fsspec.get_mapper

.. autofunction:: fsspec.open_files
.. autofunction:: fsspec.open
.. autofunction:: fsspec.filesystem
.. autofunction:: fsspec.get_filesystem_class
.. autofunction:: fsspec.get_mapper

Base Classes
------------

.. autosummary::
   fsspec.spec.AbstractFileSystem
   fsspec.spec.Transaction
   fsspec.spec.AbstractBufferedFile
   fsspec.FSMap
   fsspec.core.OpenFile

.. autoclass:: fsspec.spec.AbstractFileSystem
   :members:

.. autoclass:: fsspec.spec.Transaction
   :members:

.. autoclass:: fsspec.spec.AbstractBufferedFile
   :members:

.. autoclass:: fsspec.FSMap
   :members:

.. autoclass:: fsspec.core.OpenFile
   :members:

Built-in Implementations
------------------------

.. autosummary::
   fsspec.implementations.ftp.FTPFileSystem
   fsspec.implementations.hdfs.PyArrowHDFS
   fsspec.implementations.http.HTTPFileSystem
   fsspec.implementations.local.LocalFileSystem
   fsspec.implementations.memory.MemoryFileSystem
   fsspec.implementations.sftp.SFTPFileSystem
   fsspec.implementations.webhdfs.WebHDFS
   fsspec.implementations.zip.ZipFileSystem

.. autoclass:: fsspec.implementations.ftp.FTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.hdfs.PyArrowHDFS
   :members: __init__

.. autoclass:: fsspec.implementations.http.HTTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.local.LocalFileSystem

.. autoclass:: fsspec.implementations.memory.MemoryFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.sftp.SFTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.webhdfs.WebHDFS
   :members: __init__

.. autoclass:: fsspec.implementations.zip.ZipFileSystem
   :members: __init__
