try:
    from importlib.metadata import entry_points
except ImportError:  # python < 3.8
    from importlib_metadata import entry_points

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
from .spec import AbstractFileSystem

__version__ = get_versions()["version"]
del get_versions


__all__ = [
    "AbstractFileSystem",
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

entry_points = entry_points()
for spec in entry_points.get("fsspec.specs", []):
    err_msg = f"Unable to load filesystem from {spec}"
    register_implementation(spec.name, spec.module, errtxt=err_msg)
