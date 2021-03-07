try:
    from importlib.metadata import entry_points
except ImportError:  # python < 3.8
    try:
        from importlib_metadata import entry_points
    except ImportError:
        entry_points = None


from . import caching
from ._version import get_versions
from .core import get_fs_token_paths, open, open_files, open_local
from .mapping import FSMap, get_mapper
from .registry import (
    filesystem,
    get_filesystem_class,
    register_implementation,
    registry,
)
from .spec import AbstractArchiveFileSystem, AbstractFileSystem

__version__ = get_versions()["version"]
del get_versions


__all__ = [
    "AbstractFileSystem",
    "AbstractArchiveFileSystem",
    "FSMap",
    "filesystem",
    "register_implementation",
    "get_filesystem_class",
    "get_fs_token_paths",
    "get_mapper",
    "open",
    "open_files",
    "open_local",
    "registry",
    "caching",
]

if entry_points is not None:
    try:
        entry_points = entry_points()
    except TypeError:
        pass  # importlib-metadata < 0.8
    else:
        for spec in entry_points.get("fsspec.specs", []):
            err_msg = f"Unable to load filesystem from {spec}"
            register_implementation(
                spec.name, spec.value.replace(":", "."), errtxt=err_msg
            )
