from .core import filesystem, get_filesystem_class, split_protocol

_generic_fs = {}


def set_generic_fs(protocol, **storage_options):
    _generic_fs[protocol] = filesystem(protocol ** storage_options)


default_method = "default"


def _resolve_fs(url, method=None, protocol=None, storage_options=None):
    method = method or default_method
    protocol = protocol or split_protocol(url)[0]
    storage_options = storage_options or {}
    if method == "default":
        return filesystem(protocol)
    if method == "generic":
        return _generic_fs[protocol]
    if method == "current":
        cls = get_filesystem_class(protocol)
        return cls.current()
    if method == "options":
        return filesystem(protocol, **storage_options.get(protocol, {}))
    raise ValueError(f"Unknown FS resolution method: {method}")


def info(url, method=None, protocol=None, storage_options=None, **kwargs):
    fs = _resolve_fs(url, method, protocol, storage_options)
    out = fs.info(url, **kwargs)
    out["name"] = fs._unstrip_protocol(out["name"])
    return out
