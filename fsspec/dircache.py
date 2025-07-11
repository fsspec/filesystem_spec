from __future__ import annotations

import time
from functools import lru_cache
from typing import Any, Iterator, MutableMapping, TypedDict, TypeAlias, TYPE_CHECKING

if TYPE_CHECKING:
    # TODO: consider a more-precise type using TypedDict
    DirEntry: TypeAlias = dict[str, Any]


class DirCache(MutableMapping[str, list[DirEntry]]):
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
        use_listings_cache: bool = True,
        listings_expiry_time: float | None = None,
        max_paths: int | None = None,
        **kwargs: Any,
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
        self._cache: dict[str, list[DirEntry]] = {}
        self._times: dict[str, float] = {}
        if max_paths:
            self._q = lru_cache(max_paths + 1)(lambda key: self._cache.pop(key, None))
        self.use_listings_cache = use_listings_cache
        self.listings_expiry_time = listings_expiry_time
        self.max_paths = max_paths

    def __getitem__(self, item: str) -> list[DirEntry]:
        if self.listings_expiry_time is not None:
            if self._times.get(item, 0) - time.time() < -self.listings_expiry_time:
                del self._cache[item]
        if self.max_paths:
            self._q(item)
        return self._cache[item]  # maybe raises KeyError

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, item: object) -> bool:
        try:
            self[item]  # type: ignore[index]
            return True
        except KeyError:
            return False

    def __setitem__(self, key: str, value: List[DirEntry]) -> None:
        if not self.use_listings_cache:
            return
        if self.max_paths:
            self._q(key)
        self._cache[key] = value
        if self.listings_expiry_time is not None:
            self._times[key] = time.time()

    def __delitem__(self, key: str) -> None:
        del self._cache[key]

    def __iter__(self) -> Iterator[str]:
        entries = list(self._cache)

        return (k for k in entries if k in self)

    def __reduce__(self):
        return (
            DirCache,
            (self.use_listings_cache, self.listings_expiry_time, self.max_paths),
        )
