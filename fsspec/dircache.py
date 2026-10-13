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
    caching off.
    """

    def __init__(
        self,
        use_listings_cache=True,
        listings_expiry_time=None,
        max_paths=None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        use_listings_cache: bool
            If False, this cache never returns items, but always reports KeyError,
            and setting items has no effect.
        listings_expiry_time: int or float (optional)
            Time in seconds that a listing is considered valid. If None,
            listings do not expire. Time is measured from when the entry was set.
        max_paths: int (optional)
            The maximum number of directory listings to retain in the cache.
            When the cache exceeds this limit, the least recently used
            (set or accessed) listings are evicted. If None, 0, or negative,
            there is no limit.
        """
        # max_paths is normalized to be either positive or None.
        self.max_paths = max_paths if max_paths and max_paths > 0 else None
        self._cache = OrderedDict() if self.max_paths else {}
        self._times = {}
        self.use_listings_cache = use_listings_cache
        self.listings_expiry_time = listings_expiry_time

    def __getitem__(self, item):
        if not self.use_listings_cache:
            raise KeyError(item)

        if self.listings_expiry_time is not None and item in self._cache:
            if time.time() - self._times.get(item, 0) > self.listings_expiry_time:
                del self[item]
                raise KeyError(item)

        val = self._cache[item]
        if self.max_paths:
            self._cache.move_to_end(item)
        return val

    def clear(self):
        self._cache.clear()
        self._times.clear()

    def __len__(self):
        return len(self._cache)

    def __contains__(self, item):
        if not self.use_listings_cache or item not in self._cache:
            return False

        if self.listings_expiry_time is not None:
            if time.time() - self._times.get(item, 0) > self.listings_expiry_time:
                del self[item]
                return False

        return True

    def __setitem__(self, key, value):
        if not self.use_listings_cache:
            return

        self._cache[key] = value
        if self.listings_expiry_time is not None:
            self._times[key] = time.time()

        if self.max_paths:
            self._cache.move_to_end(key)
            if len(self._cache) > self.max_paths:
                oldest, _ = self._cache.popitem(last=False)
                self._times.pop(oldest, None)

    def __delitem__(self, key):
        del self._cache[key]
        self._times.pop(key, None)

    def __iter__(self):
        if not self.use_listings_cache:
            return

        if self.listings_expiry_time is None:
            yield from self._cache
            return

        now = time.time()
        for key in list(self._cache):
            if now - self._times.get(key, 0) > self.listings_expiry_time:
                del self[key]
            else:
                yield key

    def __reduce__(self):
        return (
            DirCache,
            (self.use_listings_cache, self.listings_expiry_time, self.max_paths),
        )
