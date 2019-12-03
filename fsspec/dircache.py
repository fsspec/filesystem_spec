import time
from collections.abc import MutableMapping
from typing import Iterator, Any, Tuple, Dict


class StaleKeyError(KeyError):
    """
    Subclass of KeyError raised when a stale key was found.
    """

    pass


class DirCache(MutableMapping):
    # Is the age / timeout per key or ...?
    # gcsfs may be per directory?

    def __init__(self, cache_timeout=None) -> None:
        # Cache is (birthday, value)
        self._cache = {}  # type: Dict[Any, Tuple[float, Any]]
        self.cache_timeout = cache_timeout

    def __repr__(self):
        return "DirCache<{!r}>".format(self._cache)

    def _maybe_invalidate_cache(self, key):
        now = time.time()
        age = now - self._cache[key][0]

        if self.cache_timeout is not None and age >= self.cache_timeout:
            del self[key]

    def _check_age(self, key, result):
        now = time.time()
        born, value = result
        age = now - born
        if self.cache_timeout is not None and age >= self.cache_timeout:
            raise StaleKeyError(key)
        return value

    def __getitem__(self, k):
        result = self._cache[k]
        return self._check_age(k, result)

    def __setitem__(self, k, v) -> None:
        age = time.time()
        self._cache[k] = (age, v)

    def __delitem__(self, v) -> None:
        del self._cache[v]

    def __iter__(self) -> Iterator:
        keys = list(self._cache.keys())
        for k in keys:
            self._maybe_invalidate_cache(k)

        return iter(self._cache)

    def __len__(self) -> int:
        keys = list(self._cache.keys())
        for k in keys:
            self._maybe_invalidate_cache(k)
        return len(self._cache)
