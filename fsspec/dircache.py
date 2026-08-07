import time
from collections import OrderedDict
from collections.abc import MutableMapping


class DirCache(MutableMapping):
    """
    Unified Entry-Index Caching of directory listings and file metadata.

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

        # Primary metadata store: path -> (file_info_dict, expiry_timestamp)
        self._entries = OrderedDict()
        # Parent-child index: parent_path -> set(child_paths)
        self._children = {}
        # Directory listing completeness: dir_path -> expiry_timestamp
        self._fully_cached_dirs = {}

    @staticmethod
    def _parent(path: str) -> str:
        clean = path.rstrip("/")
        if "/" not in clean:
            return ""
        return clean.rsplit("/", 1)[0]

    def get_info(self, path: str):
        """
        O(1) lookup for single item metadata without requiring parent directory listing.
        """
        if not self.use_listings_cache:
            return None

        path = path.rstrip("/")
        if path not in self._entries:
            return None

        info, expiry = self._entries[path]
        if self.listings_expiry_time is not None and time.time() > expiry:
            self._evict_entry(path)
            return None

        self._entries.move_to_end(path)
        return info

    def save_info(self, path: str, info: dict):
        """
        Cache single item info without claiming full directory listing completeness.
        """
        if not self.use_listings_cache:
            return

        path = path.rstrip("/")
        parent = self._parent(path)
        expiry = (
            time.time() + self.listings_expiry_time
            if self.listings_expiry_time is not None
            else float("inf")
        )

        self._entries[path] = (info, expiry)
        self._entries.move_to_end(path)

        if parent not in self._children:
            self._children[parent] = set()
        self._children[parent].add(path)

        self._enforce_capacity()

    def __getitem__(self, item):
        if not self.use_listings_cache:
            raise KeyError(item)

        path = item.rstrip("/")

        # Check full directory listing
        if path in self._fully_cached_dirs:
            expiry = self._fully_cached_dirs[path]
            if self.listings_expiry_time is not None and time.time() > expiry:
                self._invalidate_dir(path)
                raise KeyError(item)

            children = self._children.get(path, set())
            res = []
            for child in list(children):
                info = self.get_info(child)
                if info is None:
                    # Child was evicted; directory listing is no longer complete
                    self._fully_cached_dirs.pop(path, None)
                    raise KeyError(item)
                res.append(info)
            return sorted(res, key=lambda x: x.get("name", ""))

        # Fallback to single item info lookup
        info = self.get_info(path)
        if info is not None:
            return [info]

        raise KeyError(item)

    def __setitem__(self, key, value):
        if not self.use_listings_cache:
            return

        dir_path = key.rstrip("/")
        expiry = (
            time.time() + self.listings_expiry_time
            if self.listings_expiry_time is not None
            else float("inf")
        )

        if isinstance(value, list):
            child_paths = set()
            for item in value:
                child_path = item.get("name", "").rstrip("/")
                if child_path:
                    self._entries[child_path] = (item, expiry)
                    self._entries.move_to_end(child_path)
                    child_paths.add(child_path)

                    parent = self._parent(child_path)
                    if parent not in self._children:
                        self._children[parent] = set()
                    self._children[parent].add(child_path)

            self._children[dir_path] = child_paths
            self._fully_cached_dirs[dir_path] = expiry
        elif isinstance(value, dict):
            self.save_info(dir_path, value)

        self._enforce_capacity()

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

    def _invalidate_dir(self, dir_path: str):
        self._fully_cached_dirs.pop(dir_path, None)
        children = self._children.pop(dir_path, set())
        for child in children:
            if child in self._fully_cached_dirs:
                self._invalidate_dir(child)
            if child in self._entries:
                del self._entries[child]

    def _evict_entry(self, path: str):
        self._entries.pop(path, None)
        parent = self._parent(path)
        if parent in self._children:
            self._children[parent].discard(path)
        self._fully_cached_dirs.pop(parent, None)

    def _enforce_capacity(self):
        if self.max_paths is not None:
            while len(self._entries) > self.max_paths:
                oldest_path, _ = self._entries.popitem(last=False)
                self._evict_entry(oldest_path)

    def clear(self):
        self._entries.clear()
        self._children.clear()
        self._fully_cached_dirs.clear()

    def __len__(self):
        return len(self._entries)

    def __contains__(self, item):
        path = item.rstrip("/")
        if path in self._fully_cached_dirs:
            return True
        return self.get_info(path) is not None

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
