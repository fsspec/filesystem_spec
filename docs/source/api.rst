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
   fsspec.implementations.data.DataFileSystem
   fsspec.implementations.dbfs.DatabricksFileSystem
   fsspec.implementations.dirfs.DirFileSystem
   fsspec.implementations.ftp.FTPFileSystem
   fsspec.implementations.gist.GistFileSystem
   fsspec.implementations.git.GitFileSystem
   fsspec.implementations.github.GithubFileSystem
   fsspec.implementations.http.HTTPFileSystem
   fsspec.implementations.jupyter.JupyterFileSystem
   fsspec.implementations.libarchive.LibArchiveFileSystem
   fsspec.implementations.local.LocalFileSystem
   fsspec.implementations.memory.MemoryFileSystem
   fsspec.implementations.reference.ReferenceFileSystem
   fsspec.implementations.reference.LazyReferenceMapper
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

.. autoclass:: fsspec.implementations.data.DataFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dbfs.DatabricksFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.dirfs.DirFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.ftp.FTPFileSystem
   :members: __init__

.. autoclass:: fsspec.implementations.gist.GistFileSystem
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

.. autoclass:: fsspec.implementations.reference.LazyReferenceMapper
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


Note that most of these projects are hosted outside of the `fsspec` organisation. Please read their
documentation carefully before using any particular package.

- `abfs`_ for Azure Blob service, with protocol "abfs://"
- `adl`_ for Azure DataLake storage, with protocol "adl://"
- `alluxiofs`_ to access fsspec implemented filesystem with Alluxio distributed cache
- `boxfs`_ for access to Box file storage, with protocol "box://"
- `csvbase`_ for access to csvbase.com hosted CSV files, with protocol "csvbase://"
- `dropbox`_ for access to dropbox shares, with protocol "dropbox://"
- `dvc`_ to access DVC/Git repository as a filesystem
- `fsspec-encrypted`_ for transparent encryption on top of other fsspec filesystems.
- `gcsfs`_ for Google Cloud Storage, with protocol "gs://" or "gcs://"
- `gdrive`_ to access Google Drive and shares (experimental)
- `git`_ to access Git repositories
- `huggingface_hub`_ to access the Hugging Face Hub filesystem, with protocol "hf://"
- `hdfs-native`_ to access Hadoop filesystem, with protocol "hdfs://"
- `httpfs-sync`_ to access HTTP(s) files in a synchronous manner to offer an alternative to the aiohttp-based implementation.
- `ipfsspec`_ for the InterPlanetary File System (IPFS), with protocol "ipfs://"
- `irods`_ for access to iRODS servers, with protocol "irods://"
- `lakefs`_ for lakeFS data lakes, with protocol "lakefs://"
- `morefs`_ for `OverlayFileSystem`, `DictFileSystem`, and others
- `obstore`_: zero-dependency access to Amazon S3, Google Cloud Storage, and Azure Blob Storage using the underlying Rust `object_store`_ library, with protocols "s3://", "gs://", and "abfs://".
- `ocifs`_ for access to Oracle Cloud Object Storage, with protocol "oci://"
- `ocilake`_ for OCI Data Lake storage
- `ossfs`_ for Alibaba Cloud (Aliyun) Object Storage System (OSS)
- `p9fs`_ for 9P (Plan 9 Filesystem Protocol) servers
- `PyAthena`_ for S3 access to Amazon Athena, with protocol "s3://" or "s3a://"
- `PyDrive2`_ for Google Drive access
- `fsspec-proxy`_ for "pyscript:" URLs via a proxy server
- `s3fs`_ for Amazon S3 and other compatible stores, with protocol "s3://"
- `sshfs`_ for access to SSH servers, with protocol "ssh://" or "sftp://"
- `swiftspec`_ for OpenStack SWIFT, with protocol "swift://"
- `tosfs`_ for ByteDance volcano engine Tinder Object Storage (TOS)
- `wandbfs`_ to access Wandb run data (experimental)
- `wandbfsspec`_ to access Weights & Biases (experimental)
- `webdav4`_ for WebDAV, with protocol "webdav://" or "dav://"
- `xrootd`_ for xrootd, with protocol "root://"
- `msgraphfs`_ for Microsoft storage (ie Sharepoint) using the drive API through Microsoft Graph, with protocol "msgd://"

.. _abfs: https://github.com/dask/adlfs
.. _adl: https://github.com/dask/adlfs
.. _alluxiofs: https://github.com/fsspec/alluxiofs
.. _boxfs: https://github.com/IBM/boxfs
.. _csvbase: https://github.com/calpaterson/csvbase-client
.. _dropbox: https://github.com/fsspec/dropboxdrivefs
.. _dvc: https://github.com/iterative/dvc
.. _fsspec-encrypted: https://github.com/thevgergroup/fsspec-encrypted
.. _fsspec-proxy: https://github.com/fsspec/fsspec-proxy
.. _gcsfs: https://gcsfs.readthedocs.io/en/latest/
.. _gdrive: https://github.com/fsspec/gdrivefs
.. _git: https://github.com/iterative/scmrepo
.. _hdfs-native: https://github.com/Kimahriman/hdfs-native/blob/master/python/hdfs_native/fsspec.py
.. _httpfs-sync: https://github.com/moradology/httpfs-sync
.. _huggingface_hub: https://huggingface.co/docs/huggingface_hub/main/en/guides/hf_file_system
.. _ipfsspec: https://github.com/fsspec/ipfsspec
.. _irods: https://github.com/xwcl/irods_fsspec
.. _lakefs: https://github.com/aai-institute/lakefs-spec
.. _morefs: https://github.com/iterative/morefs
.. _object_store: https://docs.rs/object_store/latest/object_store/
.. _obstore: https://developmentseed.org/obstore/latest/
.. _ocifs: https://ocifs.readthedocs.io/en/latest/
.. _ocilake: https://github.com/oracle/ocifs
.. _ossfs: https://github.com/fsspec/ossfs
.. _p9fs: https://github.com/pbchekin/p9fs-py
.. _PyAthena: https://github.com/laughingman7743/PyAthena
.. _PyDrive2: https://github.com/iterative/PyDrive2
.. _s3fs: https://s3fs.readthedocs.io/en/latest/
.. _sshfs: https://github.com/fsspec/sshfs
.. _swiftspec: https://github.com/fsspec/swiftspec
.. _tosfs: https://tosfs.readthedocs.io/en/latest/
.. _wandbfs: https://github.com/jkulhanek/wandbfs
.. _wandbfsspec: https://github.com/alvarobartt/wandbfsspec
.. _webdav4: https://github.com/skshetry/webdav4
.. _xrootd: https://github.com/CoffeaTeam/fsspec-xrootd
.. _msgraphfs: https://github.com/acsone/msgraphfs

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
