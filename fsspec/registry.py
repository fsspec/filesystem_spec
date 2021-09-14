import importlib

__all__ = ["registry", "get_filesystem_class", "default"]

# mapping protocol: implementation class object
_registry = {}  # internal, mutable


class ReadOnlyError(TypeError):
    pass


class ReadOnlyRegistry(dict):
    """Dict-like registry, but immutable

    Maps backend name to implementation class

    To add backend implementations, use ``register_implementation``
    """

    def __init__(self, target):
        self.target = target

    def __getitem__(self, item):
        return self.target[item]

    def __delitem__(self, key):
        raise ReadOnlyError

    def __setitem__(self, key, value):
        raise ReadOnlyError

    def clear(self):
        raise ReadOnlyError

    def __contains__(self, item):
        return item in self.target

    def __iter__(self):
        yield from self.target


def register_implementation(name, cls, clobber=True, errtxt=None):
    """Add implementation class to the registry

    Parameters
    ----------
    name: str
        Protocol name to associate with the class
    cls: class or str
        if a class: fsspec-compliant implementation class (normally inherits from
        ``fsspec.AbstractFileSystem``, gets added straight to the registry. If a
        str, the full path to an implementation class like package.module.class,
        which gets added to known_implementations,
        so the import is deferred until the filesystem is actually used.
    clobber: bool (optional)
        Whether to overwrite a protocol with the same name; if False, will raise
        instead.
    errtxt: str (optional)
        If given, then a failure to import the given class will result in this
        text being given.
    """
    if isinstance(cls, str):
        if name in known_implementations and clobber is False:
            raise ValueError(
                "Name (%s) already in the known_implementations and clobber "
                "is False" % name
            )
        known_implementations[name] = {
            "class": cls,
            "err": errtxt or "%s import failed for protocol %s" % (cls, name),
        }

    else:
        if name in registry and clobber is False:
            raise ValueError(
                "Name (%s) already in the registry and clobber is False" % name
            )
        _registry[name] = cls


registry = ReadOnlyRegistry(_registry)
default = "file"

# protocols mapped to the class which implements them. This dict can
# updated with register_implementation
known_implementations = {
    "file": {"class": "fsspec.implementations.local.LocalFileSystem"},
    "memory": {"class": "fsspec.implementations.memory.MemoryFileSystem"},
    "dropbox": {
        "class": "dropboxdrivefs.DropboxDriveFileSystem",
        "err": (
            'DropboxFileSystem requires "dropboxdrivefs",'
            '"requests" and "dropbox" to be installed'
        ),
    },
    "http": {
        "class": "fsspec.implementations.http.HTTPFileSystem",
        "err": 'HTTPFileSystem requires "requests" and "aiohttp" to be installed',
    },
    "https": {
        "class": "fsspec.implementations.http.HTTPFileSystem",
        "err": 'HTTPFileSystem requires "requests" and "aiohttp" to be installed',
    },
    "zip": {"class": "fsspec.implementations.zip.ZipFileSystem"},
    "tar": {"class": "fsspec.implementations.tar.TarFileSystem"},
    "gcs": {
        "class": "gcsfs.GCSFileSystem",
        "err": "Please install gcsfs to access Google Storage",
    },
    "gs": {
        "class": "gcsfs.GCSFileSystem",
        "err": "Please install gcsfs to access Google Storage",
    },
    "gdrive": {
        "class": "gdrivefs.GoogleDriveFileSystem",
        "err": "Please install gdrivefs for access to Google Drive",
    },
    "sftp": {
        "class": "fsspec.implementations.sftp.SFTPFileSystem",
        "err": 'SFTPFileSystem requires "paramiko" to be installed',
    },
    "ssh": {
        "class": "fsspec.implementations.sftp.SFTPFileSystem",
        "err": 'SFTPFileSystem requires "paramiko" to be installed',
    },
    "ftp": {"class": "fsspec.implementations.ftp.FTPFileSystem"},
    "hdfs": {
        "class": "fsspec.implementations.hdfs.PyArrowHDFS",
        "err": "pyarrow and local java libraries required for HDFS",
    },
    "arrow_hdfs": {
        "class": "fsspec.implementations.arrow.HadoopFileSystem",
        "err": "pyarrow and local java libraries required for HDFS",
    },
    "webhdfs": {
        "class": "fsspec.implementations.webhdfs.WebHDFS",
        "err": 'webHDFS access requires "requests" to be installed',
    },
    "s3": {"class": "s3fs.S3FileSystem", "err": "Install s3fs to access S3"},
    "s3a": {"class": "s3fs.S3FileSystem", "err": "Install s3fs to access S3"},
    "wandb": {"class": "wandbfs.WandbFS", "err": "Install wandbfs to access wandb"},
    "adl": {
        "class": "adlfs.AzureDatalakeFileSystem",
        "err": "Install adlfs to access Azure Datalake Gen1",
    },
    "abfs": {
        "class": "adlfs.AzureBlobFileSystem",
        "err": "Install adlfs to access Azure Datalake Gen2 and Azure Blob Storage",
    },
    "az": {
        "class": "adlfs.AzureBlobFileSystem",
        "err": "Install adlfs to access Azure Datalake Gen2 and Azure Blob Storage",
    },
    "cached": {"class": "fsspec.implementations.cached.CachingFileSystem"},
    "blockcache": {"class": "fsspec.implementations.cached.CachingFileSystem"},
    "filecache": {"class": "fsspec.implementations.cached.WholeFileCacheFileSystem"},
    "simplecache": {"class": "fsspec.implementations.cached.SimpleCacheFileSystem"},
    "dask": {
        "class": "fsspec.implementations.dask.DaskWorkerFileSystem",
        "err": "Install dask distributed to access worker file system",
    },
    "dbfs": {
        "class": "fsspec.implementations.dbfs.DatabricksFileSystem",
        "err": "Install the requests package to use the DatabricksFileSystem",
    },
    "github": {
        "class": "fsspec.implementations.github.GithubFileSystem",
        "err": "Install the requests package to use the github FS",
    },
    "git": {
        "class": "fsspec.implementations.git.GitFileSystem",
        "err": "Install pygit2 to browse local git repos",
    },
    "smb": {
        "class": "fsspec.implementations.smb.SMBFileSystem",
        "err": 'SMB requires "smbprotocol" or "smbprotocol[kerberos]" installed',
    },
    "jupyter": {
        "class": "fsspec.implementations.jupyter.JupyterFileSystem",
        "err": "Jupyter FS requires requests to be installed",
    },
    "jlab": {
        "class": "fsspec.implementations.jupyter.JupyterFileSystem",
        "err": "Jupyter FS requires requests to be installed",
    },
    "libarchive": {
        "class": "fsspec.implementations.libarchive.LibArchiveFileSystem",
        "err": "LibArchive requires to be installed",
    },
    "reference": {"class": "fsspec.implementations.reference.ReferenceFileSystem"},
}


def get_filesystem_class(protocol):
    """Fetch named protocol implementation from the registry

    The dict ``known_implementations`` maps protocol names to the locations
    of classes implementing the corresponding file-system. When used for the
    first time, appropriate imports will happen and the class will be placed in
    the registry. All subsequent calls will fetch directly from the registry.

    Some protocol implementations require additional dependencies, and so the
    import may fail. In this case, the string in the "err" field of the
    ``known_implementations`` will be given as the error message.
    """
    if not protocol:
        protocol = default

    if protocol not in registry:
        if protocol not in known_implementations:
            raise ValueError("Protocol not known: %s" % protocol)
        bit = known_implementations[protocol]
        try:
            register_implementation(protocol, _import_class(bit["class"]))
        except ImportError as e:
            raise ImportError(bit["err"]) from e
    cls = registry[protocol]
    if getattr(cls, "protocol", None) in ("abstract", None):
        cls.protocol = protocol

    return cls


def _import_class(cls, minv=None):
    """Take a string FQP and return the imported class or identifier

    clas is of the form "package.module.klass" or "package.module:subobject.klass"
    """
    if ":" in cls:
        mod, name = cls.rsplit(":", 1)
        mod = importlib.import_module(mod)
        for part in name.split("."):
            mod = getattr(mod, part)
        return mod
    else:
        mod, name = cls.rsplit(".", 1)
        mod = importlib.import_module(mod)
        return getattr(mod, name)


def filesystem(protocol, **storage_options):
    """Instantiate filesystems for given protocol and arguments

    ``storage_options`` are specific to the protocol being chosen, and are
    passed directly to the class.
    """
    cls = get_filesystem_class(protocol)
    return cls(**storage_options)
