from ._version import get_versions

from .spec import AbstractFileSystem
from .registry import get_filesystem_class, registry, filesystem
from .mapping import FSMap, get_mapper
from .core import open_files, get_fs_token_paths, open
from . import caching
from .dircache import DirCache, StaleKeyError

__version__ = get_versions()["version"]
del get_versions


__all__ = [
    "AbstractFileSystem",
    "FSMap",
    "DirCache",
    "StaleKeyError",
    "filesystem",
    "get_filesystem_class",
    "get_fs_token_paths",
    "get_mapper",
    "open",
    "open_files",
    "registry",
    "caching",
]
