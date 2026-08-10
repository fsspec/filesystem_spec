import time

import pytest

from fsspec.dircache import DirCache


def test_dircache_lru_eviction():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2
    dc["c"] = 3  # Should evict "a"
    assert "a" not in dc
    assert dc["b"] == 2
    assert dc["c"] == 3
    assert len(dc) == 2

    # Refresh "b" so "c" becomes LRU
    _ = dc["b"]
    dc["d"] = 4  # Should evict "c"
    assert "c" not in dc
    assert dc["b"] == 2
    assert dc["d"] == 4
    assert len(dc) == 2


def test_dircache_expiry():
    dc = DirCache(listings_expiry_time=0.1)
    dc["x"] = 100
    assert dc["x"] == 100
    time.sleep(0.15)
    assert "x" not in dc
    with pytest.raises(KeyError):
        _ = dc["x"]


def test_dircache_iter():
    dc = DirCache(max_paths=2, listings_expiry_time=0.1)
    dc["m"] = 1
    dc["n"] = 2
    assert list(dc) == ["m", "n"]
    assert len(dc) == 2

    time.sleep(0.15)
    assert list(dc) == []
    assert len(dc) == 0


def test_nulldircache():
    from fsspec.dircache import DirCache, NullDirCache

    dc = DirCache(use_listings_cache=False)
    assert isinstance(dc, DirCache)
    assert isinstance(dc, NullDirCache)

    dc["a"] = 1
    assert "a" not in dc
    assert len(dc) == 0
    assert list(dc) == []
    with pytest.raises(KeyError):
        _ = dc["a"]
