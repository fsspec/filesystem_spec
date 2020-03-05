from functools import lru_cache
import time
from collections.abc import MutableMapping


class DirCache(MutableMapping):
    """
    Caching of directory listings, in a structure like

    {"path0": [
        {"name": "path0/file0",
         "size": 123,
         "type": "file",
         ...
        },
        {"name": "path0/file1",
        },
        ...
        ],
     "path1": [...]
    }

    Parameters to this class control listing expiry or indeed turn
    caching off
    """

    def __init__(self, use_cache=True, expiry_time=None, max_paths=None, **kwargs):
        """

        Parameters
        ----------
        use_cache: bool
            If False, this cache never returns items, but always reports KeyError,
            and setting items has no effect
        expiry_time: int (optional)
            Time in seconds that a listing is considered valid. If None,
            listings do not expire.
        max_paths: int (optional)
            The number of most recent listings that are considered valid; 'recent'
            refers to when the entry was set.
        """
        self._cache = {}
        self._times = {}
        if max_paths:
            self._q = lru_cache(max_paths + 1)(lambda key: self._cache.pop(key, None))
        self.use_cache = use_cache
        self.expiry_time = expiry_time
        self.max_paths = max_paths

    def __getitem__(self, item):
        if self.expiry_time:
            if self._times.get(item, 0) - time.time() < -self.expiry_time:
                del self._cache[item]
        if self.max_paths:
            self._q(item)
        return self._cache[item]  # maybe raises KeyError

    def clear(self):
        self._cache.clear()

    def __len__(self):
        return len(self._cache)

    def __contains__(self, item):
        try:
            self[item]
            return True
        except KeyError:
            return False

    def __setitem__(self, key, value):
        if not self.use_cache:
            return
        if self.max_paths:
            self._q(key)
        self._cache[key] = value
        if self.expiry_time:
            self._times[key] = time.time()

    def __delitem__(self, key):
        del self._cache[key]

    def __iter__(self):
        return (k for k in self._cache if k in self)

    def __reduce__(self):
        return DirCache, (self.use_cache, self.expiry_time, self.max_paths)
