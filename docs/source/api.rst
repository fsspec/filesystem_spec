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
   fsspec.archive.AbstractArchiveFileSystem
   fsspec.FSMap
   fsspec.asyn.AsyncFileSystem
   fsspec.core.OpenFile
   fsspec.core.OpenFiles
   fsspec.core.BaseCache
   fsspec.core.get_fs_token_paths
   fsspec.core.url_to_fs
   fsspec.dircache.DirCache
   fsspec.registry.ReadOnlyRegistry
   fsspec.registry.register_implementation
   fsspec.callbacks.Callback
   fsspec.callbacks.NoOpCallback
   fsspec.callbacks.DotPrinterCallback

.. autoclass:: fsspec.spec.AbstractFileSystem
   :members:

.. autoclass:: fsspec.spec.Transaction
   :members:

.. autoclass:: fsspec.spec.AbstractBufferedFile
   :members:

.. autoclass:: fsspec.archive.AbstractArchiveFileSystem
   :members:

.. autoclass:: fsspec.FSMap
   :members:

.. autoclass:: fsspec.core.OpenFile
   :members:

.. autoclass:: fsspec.core.OpenFiles

.. autoclass:: fsspec.core.BaseCache
   :members:

.. autofunction:: fsspec.core.get_fs_token_paths

.. autofunction:: fsspec.core.url_to_fs

.. autoclass:: fsspec.dircache.DirCache
   :members: __init__

.. autoclass:: fsspec.registry.ReadOnlyRegistry
   :members: __init__

.. autofunction:: fsspec.registry.register_implementation

.. autoclass:: fsspec.callbacks.Callback
   :members:

.. autoclass:: fsspec.callbacks.NoOpCallback
   :members:

.. autoclass:: fsspec.callbacks.DotPrinterCallback
   :members:

.. _implementations:

Built-in Implementations
------------------------

.. autosummary::
   fsspec.implementations.ftp.FTPFileSystem
   fsspec.implementations.hdfs.PyArrowHDFS
   fsspec.implementations.arrow.ArrowFSWrapper
   fsspec.implementations.arrow.HadoopFileSystem
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
   fsspec.implementations.git.GitFileSystem
   fsspec.implementations.smb.SMBFileSystem
   fsspec.implementations.jupyter.JupyterFileSystem
   fsspec.implementations.libarchive.LibArchiveFileSystem
   fsspec.implementations.dbfs.DatabricksFileSystem
   fsspec.implementations.reference.ReferenceFileSystem
   fsspec.implementations.prefix.PrefixFileSystem

.. autoclass:: fsspec.implementations.ftp.FTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.hdfs.PyArrowHDFS
   :members: __init__

.. autoclass:: fsspec.implementations.arrow.ArrowFSWrapper
   :members: __init__

.. autoclass:: fsspec.implementations.hdfs.HadoopFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dask.DaskWorkerFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.http.HTTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.local.LocalFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.memory.MemoryFileSystem
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

.. autoclass:: fsspec.implementations.smb.SMBFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.jupyter.JupyterFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.libarchive.LibArchiveFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dbfs.DatabricksFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.reference.ReferenceFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.prefix.PrefixFileSystem
   :members: __init__, open

Other Known Implementations
---------------------------

- `s3fs`_ for Amazon S3 and other compatible stores
- `gcsfs`_ for Google Cloud Storage
- `adl`_ for Azure DataLake storage
- `abfs`_ for Azure Blob service
- `dropbox`_ for access to dropbox shares
- `ocifs`_ for access to Oracle Cloud Object Storage
- `gdrive`_ to access Google Drive and shares (experimental)
- `wandbfs`_ to access Wandb run data (experimental)

.. _s3fs: https://s3fs.readthedocs.io/en/latest/
.. _gcsfs: https://gcsfs.readthedocs.io/en/latest/
.. _adl: https://github.com/dask/adlfs
.. _abfs: https://github.com/dask/adlfs
.. _dropbox: https://github.com/MarineChap/intake_dropbox
.. _ocifs: https://pypi.org/project/ocifs
.. _gdrive: https://github.com/fsspec/gdrivefs
.. _wandbfs: https://github.com/jkulhanek/wandbfs

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
