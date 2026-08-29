import functools
import threading
import time
from collections import OrderedDict, defaultdict
from collections.abc import MutableMapping


def _locked(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return func(self, *args, **kwargs)

    return wrapper


class DirCache(MutableMapping):
    """
    Thread-safe Unified Entry-Index Caching of directory listings and file metadata.

    Decouples single object metadata storage (`_entries`) from directory tree
    indexing (`_children`) and listing completeness (`_fully_cached_dirs`).

    Parameters
    ----------
    use_listings_cache: bool
        If False, this cache never returns items, but always reports KeyError,
        and setting items has no effect.
    listings_expiry_time: int or float (optional)
        Time in seconds that a listing is considered valid. If None,
        listings do not expire.
    max_paths: int (optional)
        The maximum number of path entries retained in cache; 'recent'
        refers to when the entry was set or accessed.
    """

    def __init__(
        self,
        use_listings_cache=True,
        listings_expiry_time=None,
        max_paths=None,
        **kwargs,
    ):
        self.use_listings_cache = use_listings_cache
        self.listings_expiry_time = listings_expiry_time
        self.max_paths = max_paths

        self._lock = threading.RLock()
        self._entries = OrderedDict()
        self._children = defaultdict(set)
        self._fully_cached_dirs = {}

    @staticmethod
    def _parent(path: str) -> str:
        clean = path.rstrip("/")
        if "/" not in clean:
            return ""
        return clean.rsplit("/", 1)[0]

    def _calc_expiry(self) -> float:
        return (
            time.time() + self.listings_expiry_time
            if self.listings_expiry_time is not None
            else float("inf")
        )

    @_locked
    def get_info(self, path: str):
        """O(1) thread-safe lookup for single item metadata."""
        if not self.use_listings_cache:
            return None

        path = path.rstrip("/")
        if path not in self._entries:
            return None

        info, expiry = self._entries[path]
        if time.time() > expiry:
            self._evict_entry(path)
            return None

        self._entries.move_to_end(path)
        return info

    @_locked
    def save_info(self, path: str, info: dict, expiry: float | None = None):
        """Thread-safe cache for single item info."""
        if not self.use_listings_cache:
            return

        path = path.rstrip("/")
        parent = self._parent(path)
        expiry = expiry if expiry is not None else self._calc_expiry()

        self._entries[path] = (info, expiry)
        self._entries.move_to_end(path)
        self._children[parent].add(path)
        self._enforce_capacity()

    @_locked
    def __getitem__(self, item):
        if not self.use_listings_cache:
            raise KeyError(item)

        path = item.rstrip("/")

        # Check full directory listing
        if path in self._fully_cached_dirs:
            if time.time() > self._fully_cached_dirs[path]:
                self._invalidate_dir(path)
                raise KeyError(item)

            res = []
            for child in list(self._children.get(path, set())):
                info = self.get_info(child)
                if info is None:
                    self._fully_cached_dirs.pop(path, None)
                    raise KeyError(item)
                res.append(info)
            return sorted(res, key=lambda x: x.get("name", ""))

        # Fallback to single item info lookup
        info = self.get_info(path)
        if info is not None:
            return [info]

        raise KeyError(item)

    @_locked
    def __setitem__(self, key, value):
        if not self.use_listings_cache:
            return

        dir_path = key.rstrip("/")
        if isinstance(value, list):
            expiry = self._calc_expiry()
            child_paths = set()
            for item in value:
                child_path = item.get("name", "").rstrip("/")
                if child_path:
                    self.save_info(child_path, item, expiry=expiry)
                    child_paths.add(child_path)

            self._children[dir_path] = child_paths
            self._fully_cached_dirs[dir_path] = expiry
        elif isinstance(value, dict):
            self.save_info(dir_path, value)

    @_locked
    def __delitem__(self, key):
        path = key.rstrip("/")
        found = False

        if path in self._fully_cached_dirs:
            self._invalidate_dir(path)
            found = True

        if path in self._entries:
            self._evict_entry(path)
            found = True

        if not found:
            raise KeyError(key)

    @_locked
    def _invalidate_dir(self, dir_path: str):
        self._fully_cached_dirs.pop(dir_path, None)
        children = self._children.pop(dir_path, set())
        for child in children:
            if child in self._fully_cached_dirs:
                self._invalidate_dir(child)
            if child in self._entries:
                del self._entries[child]

    @_locked
    def _evict_entry(self, path: str):
        self._entries.pop(path, None)
        parent = self._parent(path)
        if parent in self._children:
            self._children[parent].discard(path)
            if not self._children[parent]:
                del self._children[parent]
        self._fully_cached_dirs.pop(parent, None)

    @_locked
    def _enforce_capacity(self):
        if self.max_paths is not None:
            while len(self._entries) > self.max_paths:
                oldest_path, _ = self._entries.popitem(last=False)
                self._evict_entry(oldest_path)

    @_locked
    def clear(self):
        self._entries.clear()
        self._children.clear()
        self._fully_cached_dirs.clear()

    @_locked
    def __len__(self):
        return len(self._entries)

    @_locked
    def __contains__(self, item):
        path = item.rstrip("/")
        if path in self._fully_cached_dirs:
            return True
        return self.get_info(path) is not None

    @_locked
    def __iter__(self):
        keys = list(self._fully_cached_dirs) + [
            k for k in self._entries if k not in self._fully_cached_dirs
        ]
        return iter(keys)

    def __reduce__(self):
        return (
            DirCache,
            (self.use_listings_cache, self.listings_expiry_time, self.max_paths),
        )
