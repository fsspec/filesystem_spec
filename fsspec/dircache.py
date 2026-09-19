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
            and setting items has no effect
        listings_expiry_time: int or float (optional)
            Time in seconds that a listing is considered valid. If None,
            listings do not expire.
        max_paths: int (optional)
            The number of most recent listings that are considered valid; 'recent'
            refers to when the entry was set.
        """
        self._cache = {}
        self._times = {}
        if max_paths:
            self._q = OrderedDict()
        self.use_listings_cache = use_listings_cache
        self.listings_expiry_time = listings_expiry_time
        self.max_paths = max_paths

    def __getitem__(self, item):
        if self.listings_expiry_time is not None:
            if self._times.get(item, 0) - time.time() < -self.listings_expiry_time:
                del self._cache[item]
        if self.max_paths:
            # Refresh the recency order; the item may already be evicted.
            self._q.pop(item, None)
            self._q[item] = None
        return self._cache[item]  # maybe raises KeyError

    def clear(self):
        self._cache.clear()
        if self.max_paths:
            self._q.clear()

    def __len__(self):
        return len(self._cache)

    def __contains__(self, item):
        try:
            self[item]
            return True
        except KeyError:
            return False

    def __setitem__(self, key, value):
        if not self.use_listings_cache:
            return
        if self.max_paths:
            self._q.pop(key, None)
            self._q[key] = None
            while len(self._q) > self.max_paths:
                oldest, _ = self._q.popitem(last=False)
                self._cache.pop(oldest, None)
                self._times.pop(oldest, None)
        self._cache[key] = value
        if self.listings_expiry_time is not None:
            self._times[key] = time.time()

    def __delitem__(self, key):
        del self._cache[key]
        if self.max_paths:
            self._q.pop(key, None)

    def __iter__(self):
        entries = list(self._cache)

        return (k for k in entries if k in self)

    def __reduce__(self):
        return (
            DirCache,
            (self.use_listings_cache, self.listings_expiry_time, self.max_paths),
        )
