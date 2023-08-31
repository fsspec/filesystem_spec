API Reference
=============

.. currentmodule:: fsspec

User Functions
--------------

.. autosummary::
   fsspec.available_compressions
   fsspec.available_protocols
   fsspec.filesystem
   fsspec.fuse.run
   fsspec.generic.rsync
   fsspec.get_filesystem_class
   fsspec.get_mapper
   fsspec.gui.FileSelector
   fsspec.open
   fsspec.open_files
   fsspec.open_local

.. autofunction:: fsspec.available_compressions
.. autofunction:: fsspec.available_protocols
.. autofunction:: fsspec.filesystem
.. autofunction:: fsspec.fuse.run
.. autofunction:: fsspec.generic.rsync
.. autofunction:: fsspec.get_filesystem_class
.. autofunction:: fsspec.get_mapper
.. autoclass:: fsspec.gui.FileSelector
   :members:
.. autofunction:: fsspec.open
.. autofunction:: fsspec.open_files
.. autofunction:: fsspec.open_local

Base Classes
------------

.. autosummary::
   fsspec.archive.AbstractArchiveFileSystem
   fsspec.asyn.AsyncFileSystem
   fsspec.callbacks.Callback
   fsspec.callbacks.DotPrinterCallback
   fsspec.callbacks.NoOpCallback
   fsspec.callbacks.TqdmCallback
   fsspec.core.BaseCache
   fsspec.core.OpenFile
   fsspec.core.OpenFiles
   fsspec.core.get_fs_token_paths
   fsspec.core.url_to_fs
   fsspec.dircache.DirCache
   fsspec.FSMap
   fsspec.generic.GenericFileSystem
   fsspec.registry.register_implementation
   fsspec.spec.AbstractBufferedFile
   fsspec.spec.AbstractFileSystem
   fsspec.spec.Transaction

.. autoclass:: fsspec.archive.AbstractArchiveFileSystem
   :members:

.. autoclass:: fsspec.callbacks.Callback
   :members:

.. autoclass:: fsspec.callbacks.DotPrinterCallback
   :members:

.. autoclass:: fsspec.callbacks.NoOpCallback
   :members:

.. autoclass:: fsspec.callbacks.TqdmCallback
   :members:

.. autoclass:: fsspec.core.BaseCache
   :members:

.. autoclass:: fsspec.core.OpenFile
   :members:

.. autoclass:: fsspec.core.OpenFiles

.. autofunction:: fsspec.core.get_fs_token_paths

.. autofunction:: fsspec.core.url_to_fs

.. autoclass:: fsspec.dircache.DirCache
   :members: __init__

.. autoclass:: fsspec.FSMap
   :members:

.. autoclass:: fsspec.generic.GenericFileSystem

.. autofunction:: fsspec.registry.register_implementation

.. autoclass:: fsspec.spec.AbstractBufferedFile
   :members:

.. autoclass:: fsspec.spec.AbstractFileSystem
   :members:

.. autoclass:: fsspec.spec.Transaction
   :members:

.. _implementations:

Built-in Implementations
------------------------

.. autosummary::
   fsspec.implementations.arrow.ArrowFSWrapper
   fsspec.implementations.arrow.HadoopFileSystem
   fsspec.implementations.cached.CachingFileSystem
   fsspec.implementations.cached.SimpleCacheFileSystem
   fsspec.implementations.cached.WholeFileCacheFileSystem
   fsspec.implementations.dask.DaskWorkerFileSystem
   fsspec.implementations.dbfs.DatabricksFileSystem
   fsspec.implementations.dirfs.DirFileSystem
   fsspec.implementations.ftp.FTPFileSystem
   fsspec.implementations.git.GitFileSystem
   fsspec.implementations.github.GithubFileSystem
   fsspec.implementations.http.HTTPFileSystem
   fsspec.implementations.jupyter.JupyterFileSystem
   fsspec.implementations.libarchive.LibArchiveFileSystem
   fsspec.implementations.local.LocalFileSystem
   fsspec.implementations.memory.MemoryFileSystem
   fsspec.implementations.reference.ReferenceFileSystem
   fsspec.implementations.sftp.SFTPFileSystem
   fsspec.implementations.smb.SMBFileSystem
   fsspec.implementations.tar.TarFileSystem
   fsspec.implementations.webhdfs.WebHDFS
   fsspec.implementations.zip.ZipFileSystem

.. autoclass:: fsspec.implementations.arrow.ArrowFSWrapper
   :members: __init__

.. autoclass:: fsspec.implementations.arrow.HadoopFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.cached.CachingFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.cached.SimpleCacheFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.cached.WholeFileCacheFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dask.DaskWorkerFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dbfs.DatabricksFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dirfs.DirFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.ftp.FTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.git.GitFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.github.GithubFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.http.HTTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.jupyter.JupyterFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.libarchive.LibArchiveFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.local.LocalFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.memory.MemoryFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.reference.ReferenceFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.sftp.SFTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.smb.SMBFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.tar.TarFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.webhdfs.WebHDFS
   :members: __init__

.. autoclass:: fsspec.implementations.zip.ZipFileSystem
   :members: __init__

.. _external_implementations:

Other Known Implementations
---------------------------

- `abfs`_ for Azure Blob service
- `adl`_ for Azure DataLake storage
- `boxfs`_ for access to Box file storage
- `dropbox`_ for access to dropbox shares
- `dvc`_ to access DVC/Git repository as a filesystem
- `gcsfs`_ for Google Cloud Storage
- `gdrive`_ to access Google Drive and shares (experimental)
- `huggingface_hub`_ to access the Hugging Face Hub filesystem, with protocol "hf://"
- `lakefs`_ for lakeFS data lakes
- `ocifs`_ for access to Oracle Cloud Object Storage
- `ocilake`_ for OCI Data Lake storage
- `ossfs`_ for Alibaba Cloud (Aliyun) Object Storage System (OSS)
- `s3fs`_ for Amazon S3 and other compatible stores
- `wandbfs`_ to access Wandb run data (experimental)
- `webdav4`_ for WebDAV

.. _abfs: https://github.com/dask/adlfs
.. _adl: https://github.com/dask/adlfs
.. _boxfs: https://github.com/IBM/boxfs
.. _dropbox: https://github.com/MarineChap/intake_dropbox
.. _dvc: https://github.com/iterative/dvc
.. _gcsfs: https://gcsfs.readthedocs.io/en/latest/
.. _gdrive: https://github.com/fsspec/gdrivefs
.. _huggingface_hub: https://huggingface.co/docs/huggingface_hub/main/en/guides/hf_file_system
.. _lakefs: https://github.com/appliedAI-Initiative/lakefs-spec
.. _ocifs: https://pypi.org/project/ocifs
.. _ocilake: https://github.com/oracle/ocifs
.. _ossfs: https://github.com/fsspec/ossfs
.. _s3fs: https://s3fs.readthedocs.io/en/latest/
.. _wandbfs: https://github.com/jkulhanek/wandbfs
.. _webdav4: https://github.com/skshetry/webdav4

.. _readbuffering:

Read Buffering
--------------

.. autosummary::
   fsspec.caching.BlockCache
   fsspec.caching.BytesCache
   fsspec.caching.MMapCache
   fsspec.caching.ReadAheadCache
   fsspec.caching.FirstChunkCache
   fsspec.caching.BackgroundBlockCache

.. autoclass:: fsspec.caching.BlockCache
   :members:

.. autoclass:: fsspec.caching.BytesCache
   :members:

.. autoclass:: fsspec.caching.MMapCache
   :members:

.. autoclass:: fsspec.caching.ReadAheadCache
   :members:

.. autoclass:: fsspec.caching.FirstChunkCache
   :members:

.. autoclass:: fsspec.caching.BackgroundBlockCache
   :members:

Utilities
---------

.. autosummary::

   fsspec.utils.read_block

.. autofunction:: fsspec.utils.read_block

.. raw:: html

    <script data-goatcounter="https://fsspec.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
