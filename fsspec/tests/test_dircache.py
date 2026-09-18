import pickle
import time

import pytest

from fsspec.dircache import DirCache


def test_dircache_lru():
    # Unlimited cache
    dc_unlimited = DirCache(max_paths=None)
    dc_unlimited["x"] = 1
    assert list(dc_unlimited) == ["x"]

    # max_paths=0 retains nothing
    dc_zero = DirCache(max_paths=0)
    dc_zero["a"] = 1
    assert len(dc_zero) == 0 and "a" not in dc_zero

    # Bounded cache with LRU eviction
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2
    dc["c"] = 3  # Evicts oldest: "a"
    assert "a" not in dc and dc["b"] == 2 and dc["c"] == 3
    assert len(dc) == 2

    # Accessing "b" marks it recent, so "c" becomes LRU
    _ = dc["b"]
    dc["d"] = 4  # Evicts "c"
    assert "c" not in dc and dc["b"] == 2 and dc["d"] == 4
    assert list(dc) == ["b", "d"]


def test_dircache_expiry():
    dc = DirCache(max_paths=2, listings_expiry_time=0.1)
    dc["a"] = 100
    dc["b"] = 200

    # Accessing "a" moves it to the most recent position
    assert dc["a"] == 100
    assert list(dc) == ["b", "a"]
    assert "nonexistent" not in dc  # Missing key must not raise KeyError
    with pytest.raises(KeyError):
        _ = dc["nonexistent"]

    time.sleep(0.12)

    # Expired items raise KeyError on get and return False on membership
    with pytest.raises(KeyError):
        _ = dc["a"]
    assert "b" not in dc
    assert list(dc) == []
    assert len(dc) == 0


def test_dircache_disabled():
    dc = DirCache(use_listings_cache=False)
    dc["a"] = 1
    assert "a" not in dc
    assert len(dc) == 0
    assert list(dc) == []
    with pytest.raises(KeyError):
        _ = dc["a"]


def test_dircache_mapping_and_pickle():
    dc = DirCache(use_listings_cache=True, listings_expiry_time=60, max_paths=5)
    dc.update({"a": 1, "b": 2})
    assert dc.get("a") == 1
    assert dc.get("c", 42) == 42
    assert dc.setdefault("c", 3) == 3
    assert dict(dc) == {"a": 1, "b": 2, "c": 3}

    # del removes item
    del dc["a"]
    assert "a" not in dc

    # pop
    assert dc.pop("b") == 2 and "b" not in dc
    assert dc.pop("missing", 99) == 99

    # clear
    dc.clear()
    assert len(dc) == 0
    assert list(dc) == []

    # pickle roundtrip
    data = pickle.dumps(DirCache(listings_expiry_time=10, max_paths=5))
    dc2 = pickle.loads(data)
    assert dc2.listings_expiry_time == 10 and dc2.max_paths == 5
