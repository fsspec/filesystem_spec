from functools import lru_cache
import time


class DirCache(dict):
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
    def __init__(self, usecache=True, expiry_time=False, max_size=None):
        self._cache = {}
        self._times = {}
        if max_size:
            self.q = lru_cache(max_size + 1)(lambda key: self._cache.pop(key, None))
        self.use = usecache
        self.exp = -expiry_time
        self.max = max_size

    def __getitem__(self, item):
        if self.exp:
            if self._times.get(item, 0) - time.time() < self.exp:
                del self._cache[item]
        if self.max:
            self.q(item)
        return self._cache[item]  # maybe raises KeyError

    def __setitem__(self, key, value):
        if not self.use:
            return
        if self.max:
            self.q(key)
        self._cache[key] = value
        if self.exp:
            self._times[key] = time.time()

