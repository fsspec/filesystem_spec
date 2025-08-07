from __future__ import annotations

import logging

from fsspec.implementations.memory import MemoryFileSystem

logger = logging.getLogger("fsspec.localmemoryfs")


class LocalMemoryFileSystem(MemoryFileSystem):
    """A filesystem based on a dict of BytesIO objects

    This is a local filesystem so different instances of this class
    point to different memory filesystems.
    """

    store = None
    pseudo_dirs = None
    protocol = "localmemory"
    root_marker = "/"
    _intrans = False
    cachable = False  # same as: skip_instance_cache = True

    def __init__(self, *args, **kwargs):
        self.logger = logger  # global
        self.store: dict[str, Any] = {}  # local
        self.pseudo_dirs = [""]  # local
        super().__init__(*args, **kwargs)
