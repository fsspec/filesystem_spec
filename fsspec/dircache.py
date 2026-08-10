import time
from collections import OrderedDict
from collections.abc import MutableMapping


class DirCache(MutableMapping):
    """
    Caching of directory listings, in a structure like::

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

    def __new__(
        cls,
        use_listings_cache=True,
        listings_expiry_time=None,
        max_paths=None,
        **kwargs,
    ):
        if not use_listings_cache:
            return super().__new__(NullDirCache)
        return super().__new__(cls)

    def __init__(
        self,
        use_listings_cache=True,
        listings_expiry_time=None,
        max_paths=None,
        **kwargs,
    ):
        self._cache = OrderedDict()
        self._times = {}
        self.use_listings_cache = use_listings_cache
        self.listings_expiry_time = listings_expiry_time
        self.max_paths = max_paths

    def __getitem__(self, item):
        if self.listings_expiry_time is not None:
            if self._times.get(item, 0) - time.time() < -self.listings_expiry_time:
                del self[item]
                raise KeyError(item)

        val = self._cache[item]  # maybe raises KeyError
        self._cache.move_to_end(item)
        return val

    def clear(self):
        self._cache.clear()
        self._times.clear()

    def __len__(self):
        return len(self._cache)

    def __contains__(self, item):
        if self.listings_expiry_time is not None:
            if self._times.get(item, 0) - time.time() < -self.listings_expiry_time:
                del self[item]
                return False
        return item in self._cache

    def __setitem__(self, key, value):
        self._cache[key] = value
        self._cache.move_to_end(key)
        if self.listings_expiry_time is not None:
            self._times[key] = time.time()

        if self.max_paths and len(self._cache) > self.max_paths:
            oldest, _ = self._cache.popitem(last=False)
            self._times.pop(oldest, None)

    def __delitem__(self, key):
        del self._cache[key]
        self._times.pop(key, None)

    def __iter__(self):
        now = time.time()
        for key in list(self._cache):
            if self.listings_expiry_time is not None and (
                self._times.get(key, 0) - now < -self.listings_expiry_time
            ):
                del self[key]
            else:
                yield key

    def __reduce__(self):
        return (
            DirCache,
            (self.use_listings_cache, self.listings_expiry_time, self.max_paths),
        )


class NullDirCache(DirCache):
    """No-op directory listing cache used when use_listings_cache=False"""

    def __init__(
        self,
        use_listings_cache=False,
        listings_expiry_time=None,
        max_paths=None,
        **kwargs,
    ):
        super().__init__(
            use_listings_cache=False,
            listings_expiry_time=listings_expiry_time,
            max_paths=max_paths,
            **kwargs,
        )

    def __getitem__(self, item):
        raise KeyError(item)

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __contains__(self, item):
        return False

    def __len__(self):
        return 0

    def __iter__(self):
        return iter(())

    def clear(self):
        pass
