
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .spec import AbstractFileSystem
from .registry import get_filesystem_class, registry, filesystem
from .mapping import FSMap, get_mapper
from .core import open_files, get_fs_token_paths, open
