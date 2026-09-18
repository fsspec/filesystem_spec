import pickle
import time
from collections import OrderedDict

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


def test_dircache_missing_key_with_expiry():
    dc = DirCache(listings_expiry_time=10)
    assert "nonexistent" not in dc
    with pytest.raises(KeyError):
        _ = dc["nonexistent"]


def test_dircache_iter():
    dc = DirCache(max_paths=2, listings_expiry_time=0.1)
    dc["m"] = 1
    dc["n"] = 2
    assert list(dc) == ["m", "n"]
    assert len(dc) == 2

    time.sleep(0.15)
    assert list(dc) == []
    assert len(dc) == 0


def test_dircache_disabled():
    dc = DirCache(use_listings_cache=False)
    assert isinstance(dc, DirCache)

    dc["a"] = 1
    assert "a" not in dc
    assert len(dc) == 0
    assert list(dc) == []
    with pytest.raises(KeyError):
        _ = dc["a"]


def test_dircache_no_max_paths():
    dc_no_max = DirCache(max_paths=None)
    assert type(dc_no_max._cache) is dict

    dc_max = DirCache(max_paths=10)
    assert type(dc_max._cache) is OrderedDict

    dc_no_max["a"] = 1
    dc_no_max["b"] = 2
    assert dc_no_max["a"] == 1
    assert dc_no_max["b"] == 2
    assert len(dc_no_max) == 2
    assert list(dc_no_max) == ["a", "b"]


def test_dircache_max_paths_zero():
    dc = DirCache(max_paths=0)
    assert type(dc._cache) is OrderedDict
    dc["a"] = 1
    assert len(dc) == 0
    assert "a" not in dc


def test_dircache_clear_and_del():
    dc = DirCache(listings_expiry_time=60, max_paths=10)
    dc["a"] = 1
    dc["b"] = 2
    assert "a" in dc._times
    del dc["a"]
    assert "a" not in dc
    assert "a" not in dc._times

    dc.clear()
    assert len(dc) == 0
    assert len(dc._times) == 0


def test_dircache_pickle():
    dc = DirCache(use_listings_cache=True, listings_expiry_time=10, max_paths=5)
    data = pickle.dumps(dc)
    dc2 = pickle.loads(data)
    assert dc2.use_listings_cache is True
    assert dc2.listings_expiry_time == 10
    assert dc2.max_paths == 5
