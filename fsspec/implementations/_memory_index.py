"""Path containers with mutation-aware directory indexes for memory filesystems."""

from collections.abc import MutableMapping
from threading import RLock


class _DirectoryIndex:
    def _init_index(self):
        self._directories = {}
        self._index_lock = RLock()

    def _index_path(self, path, delta, *, include_self=False):
        if include_self:
            self._count_directory(path, delta)
        while "/" in path:
            path = path.rsplit("/", 1)[0]
            self._count_directory(path, delta)

    def _count_directory(self, path, delta):
        count = self._directories.get(path, 0) + delta
        if count:
            self._directories[path] = count
        else:
            self._directories.pop(path, None)

    def has_directory(self, path):
        with self._index_lock:
            return path in self._directories


class _MemoryStore(_DirectoryIndex, dict):
    """A dict that counts the ancestors of its file paths.

    Keep the index on the shared container, not a filesystem instance. Updating
    the value of an existing path does not change its ancestors.
    """

    def __init__(self, *args, **kwargs):
        self._init_index()
        super().__init__()
        for path in self:
            self._index_path(path, 1)
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        with self._index_lock:
            new = key not in self
            super().__setitem__(key, value)
            if new:
                self._index_path(key, 1)

    def __delitem__(self, key):
        with self._index_lock:
            super().__delitem__(key)
            self._index_path(key, -1)

    def clear(self):
        with self._index_lock:
            super().clear()
            self._directories.clear()

    def update(self, *args, **kwargs):
        with self._index_lock:
            # Use the public mutation hook, including partial updates before an
            # input iterator raises. dict.update would bypass __setitem__.
            MutableMapping.update(self, *args, **kwargs)

    def setdefault(self, key, default=None):
        with self._index_lock:
            if key not in self:
                self[key] = default
            return self[key]

    def pop(self, key, *args):
        with self._index_lock:
            if len(args) > 1:
                return super().pop(key, *args)
            if key in self:
                value = self[key]
                del self[key]
                return value
            return super().pop(key, *args)

    def popitem(self):
        with self._index_lock:
            key, value = super().popitem()
            self._index_path(key, -1)
            return key, value

    def __ior__(self, other):
        self.update(other)
        return self

    @classmethod
    def fromkeys(cls, iterable, value=None):
        return cls((key, value) for key in iterable)

    def __reduce__(self):
        # Exclude the lock and derived index. The item iterator preserves cycles
        # through MemoryFile.fs when an independent filesystem is pickled.
        with self._index_lock:
            return type(self), (), {}, None, iter(list(self.items()))

    def __setstate__(self, state):
        # Some serializers fill dict subclasses without calling __setitem__.
        with self._index_lock:
            self._directories.clear()
            for path in self:
                self._index_path(path, 1)


class _MemoryDirectories(_DirectoryIndex, list):
    """A list that indexes explicit directories and their implied ancestors."""

    def __init__(self, iterable=()):
        self._init_index()
        super().__init__(iterable)
        self._reindex()

    def _reindex(self):
        self._directories.clear()
        for path in self:
            self._index_path(path, 1, include_self=True)

    def append(self, path):
        with self._index_lock:
            super().append(path)
            self._index_path(path, 1, include_self=True)

    def extend(self, iterable):
        with self._index_lock:
            # list.extend can partially succeed when an iterator raises.
            try:
                super().extend(iterable)
            finally:
                self._reindex()

    def insert(self, index, path):
        with self._index_lock:
            super().insert(index, path)
            self._index_path(path, 1, include_self=True)

    def pop(self, index=-1):
        with self._index_lock:
            path = super().pop(index)
            self._index_path(path, -1, include_self=True)
            return path

    def remove(self, path):
        with self._index_lock:
            super().remove(path)
            self._index_path(path, -1, include_self=True)

    def clear(self):
        with self._index_lock:
            super().clear()
            self._directories.clear()

    def __setitem__(self, key, value):
        with self._index_lock:
            try:
                super().__setitem__(key, value)
            finally:
                self._reindex()

    def __delitem__(self, key):
        with self._index_lock:
            super().__delitem__(key)
            self._reindex()

    def __iadd__(self, iterable):
        self.extend(iterable)
        return self

    def __imul__(self, count):
        with self._index_lock:
            super().__imul__(count)
            self._reindex()
        return self

    def sort(self, *args, **kwargs):
        with self._index_lock:
            try:
                super().sort(*args, **kwargs)
            finally:
                self._reindex()

    def __reduce__(self):
        with self._index_lock:
            return type(self), (list(self),)
