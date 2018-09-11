import importlib
__all__ = ['registry', 'get_filesystem_class', 'default']

# mapping protocol: implementation class
registry = {}
default = 'file'

known_implementations = {
    'file': 'fsspec.LocalFileSystem',
    'memory': 'fsspec.MemoryFileSystem'
}


def get_filesystem_class(protocol):
    if not protocol:
        protocol = default
    if protocol not in registry:
        if protocol not in known_implementations:
            raise ValueError("Protocol not known: %s" % protocol)
        mod, name = known_implementations[protocol].rsplit('.', 1)
        mod = importlib.import_module(mod)
        registry[protocol] = getattr(mod, name)
        if registry[protocol].protocol == 'abstract':
            registry[protocol].protocol = protocol

    return registry[protocol]
