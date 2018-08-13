import importlib
__all__ = ['registry', 'get_filesystem_class', 'default']

registry = {}
default = 'fsspec.local.LocalFileSystem'

known_implementations = {
    'file': default,
}


def get_filesystem_class(protocol):
    if protocol not in registry:
        if protocol not in known_implementations:
            raise ValueError("Protocol not known: %s" % protocol)
        mod, name = protocol.rsplit('.', 1)
        mod = importlib.import_module(mod)
        registry[protocol] = getattr(mod, name)

    return registry[protocol]
