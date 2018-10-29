
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .spec import AbstractFileSystem
from .registry import get_filesystem_class, registry
from .implementations import LocalFileSystem, MemoryFileSystem
from .mapping import FSMap, get_mapper

try:
    import pyarrow as pa

    class AbstractFileSystem(AbstractFileSystem, pa.filesystem.DaskFileSystem):
        pass
except ImportError:
    pass
