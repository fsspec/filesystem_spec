API Reference
=============

.. currentmodule:: fsspec

User Functions
--------------

.. autosummary::
   fsspec.open_files
   fsspec.open
   fsspec.open_local
   fsspec.filesystem
   fsspec.get_filesystem_class
   fsspec.get_mapper
   fsspec.fuse.run
   fsspec.gui.FileSelector

.. autofunction:: fsspec.open_files
.. autofunction:: fsspec.open
.. autofunction:: fsspec.open_local
.. autofunction:: fsspec.filesystem
.. autofunction:: fsspec.get_filesystem_class
.. autofunction:: fsspec.get_mapper
.. autofunction:: fsspec.fuse.run
.. autoclass:: fsspec.gui.FileSelector
   :members:

Base Classes
------------

.. autosummary::
   fsspec.spec.AbstractFileSystem
   fsspec.spec.Transaction
   fsspec.spec.AbstractBufferedFile
   fsspec.FSMap
   fsspec.core.OpenFile
   fsspec.core.BaseCache
   fsspec.core.get_fs_token_paths
   fsspec.dircache.DirCache

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

.. autoclass:: fsspec.core.BaseCache
   :members:

.. autofunction:: fsspec.core.get_fs_token_paths

.. autoclass:: fsspec.dircache.DirCache
   :members: __init__

.. _implementations:

Built-in Implementations
------------------------

.. autosummary::
   fsspec.implementations.ftp.FTPFileSystem
   fsspec.implementations.hdfs.PyArrowHDFS
   fsspec.implementations.dask.DaskWorkerFileSystem
   fsspec.implementations.http.HTTPFileSystem
   fsspec.implementations.local.LocalFileSystem
   fsspec.implementations.memory.MemoryFileSystem
   fsspec.implementations.github.GithubFileSystem
   fsspec.implementations.sftp.SFTPFileSystem
   fsspec.implementations.webhdfs.WebHDFS
   fsspec.implementations.zip.ZipFileSystem
   fsspec.implementations.cached.CachingFileSystem
   fsspec.implementations.cached.WholeFileCacheFileSystem
   fsspec.implementations.cached.SimpleCacheFileSystem
   fsspec.implementations.github.GithubFileSystem
   fsspec.implementations.git.GitFileSystem

.. autoclass:: fsspec.implementations.ftp.FTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.hdfs.PyArrowHDFS
   :members: __init__

.. autoclass:: fsspec.implementations.dask.DaskWorkerFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.http.HTTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.local.LocalFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.memory.MemoryFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.github.GithubFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.sftp.SFTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.webhdfs.WebHDFS
   :members: __init__

.. autoclass:: fsspec.implementations.zip.ZipFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.cached.CachingFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.cached.WholeFileCacheFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.cached.SimpleCacheFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.github.GithubFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.git.GitFileSystem
   :members: __init__

Other Known Implementations
---------------------------

- `s3fs`_ for Amazon S3 and other compatible stores
- `gcsfs`_ for Google Cloud Storage
- `adl`_ for Azure DataLake storage
- `abfs`_ for Azure Blob service
- `dropbox`_ for access to dropbox shares
- `gdrive`_ to access Google Drive and shares (experimental)

.. _s3fs: https://s3fs.readthedocs.io/en/latest/
.. _gcsfs: https://gcsfs.readthedocs.io/en/latest/
.. _adl: https://github.com/dask/adlfs
.. _abfs: https://github.com/dask/adlfs
.. _dropbox: https://github.com/MarineChap/intake_dropbox
.. _gdrive: https://github.com/intake/gdrivefs

.. _readbuffering:

Read Buffering
--------------

.. autosummary::

  fsspec.caching.ReadAheadCache
  fsspec.caching.BytesCache
  fsspec.caching.MMapCache
  fsspec.caching.BlockCache

.. autoclass:: fsspec.caching.ReadAheadCache
   :members:

.. autoclass:: fsspec.caching.BytesCache
   :members:

.. autoclass:: fsspec.caching.MMapCache
   :members:

.. autoclass:: fsspec.caching.BlockCache
   :members:
