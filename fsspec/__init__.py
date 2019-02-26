
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .spec import AbstractFileSystem

try:
    # Do this first before accidentally importing anything that depends
    # on AbstractFileSystem
    import pyarrow as pa

    class AbstractFileSystem(AbstractFileSystem, pa.filesystem.DaskFileSystem):
        pass
except ImportError:
    pass

from .registry import get_filesystem_class, registry, filesystem
from .mapping import FSMap, get_mapper
from .core import open_files, get_fs_token_paths, open

