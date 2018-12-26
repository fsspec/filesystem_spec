import importlib
__all__ = ['registry', 'get_filesystem_class', 'default']

# mapping protocol: implementation class
registry = {}
default = 'file'

known_implementations = {
    'file': {'class': 'fsspec.implementations.local.LocalFileSystem'},
    'memory': {'class': 'fsspec.implementations.memory.MemoryFileSystem'},
    'http': {'class': 'fsspec.implementations.http.HTTPFileSystem',
             'err': 'HTTPFileSystem requires "requests" to be installed'},
    'https': {'class': 'fsspec.implementations.http.HTTPFileSystem',
              'err': 'HTTPFileSystem requires "requests" to be installed'},
    'zip': {'class': 'fsspec.implementations.zip.ZipFileSystem'},
    'gcs': {'class': 'gcsfs.GCSFileSystem', 'err': 'Please install gcsfs'},
    'sftp': {'class': 'fsspec.implementations.sftp.SFTPFileSystem',
             'err': 'SFTPFileSystem requires "paramiko" to be installed'},
    'ftp': {'class': 'fsspec.implementations.ftp.FTPFileSystem'}
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
    if protocol is None:
        protocol = default
    if protocol not in registry:
        if protocol not in known_implementations:
            raise ValueError("Protocol not known: %s" % protocol)
        bit = known_implementations[protocol]
        mod, name = bit['class'].rsplit('.', 1)
        err = None
        try:
            mod = importlib.import_module(mod)
        except ImportError:
            err = ImportError(bit['err'])
        except Exception as e:
            err = e
        if err is not None:
            raise RuntimeError(str(err))
        registry[protocol] = getattr(mod, name)
    cls = registry[protocol]
    if cls.protocol == 'abstract' or cls.protocol is None:
        cls.protocol = protocol

    return cls


def filesystem(protocol, **storage_options):
    """Instantiate filesystems for given protocol and arguments

    ``storage_options`` are specific to the protocol being chosen, and are
    passed directly to the class.
    """
    cls = get_filesystem_class(protocol)
    return cls(**storage_options)
