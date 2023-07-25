from __future__ import annotations

import abc
import hashlib
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


class AbstractCacheMapper(abc.ABC):
    """Abstract super-class for mappers from remote URLs to local cached
    basenames.
    """

    @abc.abstractmethod
    def __call__(self, path: str) -> str:
        ...

    def __eq__(self, other: Any) -> bool:
        # Identity only depends on class. When derived classes have attributes
        # they will need to be included.
        return isinstance(other, type(self))

    def __hash__(self) -> int:
        # Identity only depends on class. When derived classes have attributes
        # they will need to be included.
        return hash(type(self))


class BasenameCacheMapper(AbstractCacheMapper):
    """Cache mapper that uses the basename of the remote URL.

    Different paths with the same basename will therefore have the same cached
    basename.
    """

    def __call__(self, path: str) -> str:
        return os.path.basename(path)


class HashCacheMapper(AbstractCacheMapper):
    """Cache mapper that uses a hash of the remote URL."""

    def __call__(self, path: str) -> str:
        return hashlib.sha256(path.encode()).hexdigest()


def create_cache_mapper(same_names: bool) -> AbstractCacheMapper:
    """Factory method to create cache mapper for backward compatibility with
    ``CachingFileSystem`` constructor using ``same_names`` kwarg.
    """
    if same_names:
        return BasenameCacheMapper()
    else:
        return HashCacheMapper()
