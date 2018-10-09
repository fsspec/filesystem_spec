import importlib
__all__ = ['registry', 'get_filesystem_class', 'default']

# mapping protocol: implementation class
registry = {}
default = 'file'

known_implementations = {
    'file': {'class': 'fsspec.LocalFileSystem', 'err': ""},
    'memory': {'class': 'fsspec.MemoryFileSystem', 'err': ''},
    'http': {'class': 'fsspec.implementations.http.HTTPFileSystem',
             'err': 'HTTPFileSystem requires requests to be installed'},
    'https': {'class': 'fsspec.implementations.http.HTTPFileSystem',
              'err': 'HTTPFileSystem requires requests to be installed'}
}


def get_filesystem_class(protocol):
    if not protocol:
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
            raise err
        registry[protocol] = getattr(mod, name)
        if registry[protocol].protocol == 'abstract':
            registry[protocol].protocol = protocol

    return registry[protocol]
