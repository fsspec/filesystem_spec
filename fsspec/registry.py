__all__ = ["registry", "get_filesystem_class"]

# internal, mutable
_registry: dict[str, type] = {}
registry = _registry


def register_implementation(name, cls, clobber=False, errtxt=None):
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
            if cls != known_implementations[name]["class"]:
                raise ValueError(
                    f"Name ({name}) already in the known_implementations and clobber "
                    f"is False"
                )
        else:
            known_implementations[name] = {
                "class": cls,
                "err": errtxt or f"{cls} import failed for protocol {name}",
            }

    else:
        if name in registry and clobber is False:
            if _registry[name] is not cls:
                raise ValueError(
                    f"Name ({name}) already in the registry and clobber is False"
                )
        else:
            _registry[name] = cls


# protocols mapped to the class which implements them. This dict can be
# updated with register_implementation
known_implementations = {
    "data": {"class": "fsspec.implementations.data.DataFileSystem"},
    "memory": {"class": "fsspec.implementations.memory.MemoryFileSystem"},
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
    if protocol not in registry:
        if protocol not in known_implementations:
            raise ValueError(f"Protocol not known: {protocol}")
        bit = known_implementations[protocol]
        try:
            register_implementation(protocol, _import_class(bit["class"]))
        except ImportError as e:
            raise ImportError(bit.get("err")) from e
    cls = registry[protocol]
    if getattr(cls, "protocol", None) in ("abstract", None):
        cls.protocol = protocol

    return cls


def _import_class(fqp: str):
    """Take a fully-qualified path and return the imported class or identifier.

    ``fqp`` is of the form "package.module.klass" or
    "package.module:subobject.klass".

    Warnings
    --------
    This can import arbitrary modules. Make sure you haven't installed any modules
    that may execute malicious code at import time.
    """
    import sys

    if ":" in fqp:
        mod, name = fqp.rsplit(":", 1)
    else:
        mod, name = fqp.rsplit(".", 1)

    mod = sys.modules[mod]
    for part in name.split("."):
        mod = getattr(mod, part)

    if not isinstance(mod, type):
        raise TypeError(f"{fqp} is not a class")

    return mod


def filesystem(protocol, **storage_options):
    """Instantiate filesystems for given protocol and arguments

    ``storage_options`` are specific to the protocol being chosen, and are
    passed directly to the class.
    """
    cls = get_filesystem_class(protocol)
    return cls(**storage_options)


def available_protocols():
    """Return a list of the implemented protocols.

    Note that any given protocol may require extra packages to be importable.
    """
    return list(known_implementations)
