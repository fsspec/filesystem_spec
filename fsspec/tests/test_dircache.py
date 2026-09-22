import pickle
import time

import pytest

from fsspec.dircache import DirCache


def test_dircache_evicts_oldest_when_capacity_exceeded():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2

    dc["c"] = 3

    assert "a" not in dc
    assert dc["b"] == 2
    assert dc["c"] == 3
    assert len(dc) == 2


def test_dircache_read_promotes_key_to_most_recent():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2

    _ = dc["a"]
    dc["c"] = 3

    assert "b" not in dc
    assert dc["a"] == 1
    assert dc["c"] == 3


def test_dircache_negative_max_paths_retains_all_items():
    dc = DirCache(max_paths=-1)

    for i in range(10):
        dc[f"k{i}"] = i

    assert len(dc) == 10
    assert dc["k0"] == 0
    assert dc["k9"] == 9
    assert len(list(dc)) == 10


def test_dircache_zero_max_paths_retains_all_items():
    dc = DirCache(max_paths=0)

    for i in range(10):
        dc[f"k{i}"] = i

    assert len(dc) == 10
    assert dc["k0"] == 0
    assert dc["k9"] == 9
    assert len(list(dc)) == 10


def test_dircache_unlimited_paths_retains_all_items():
    dc = DirCache(max_paths=None)

    for i in range(10):
        dc[f"k{i}"] = i

    assert len(dc) == 10
    assert dc["k0"] == 0
    assert dc["k9"] == 9
    assert len(list(dc)) == 10


def test_dircache_expired_entry_raises_key_error_on_get():
    dc = DirCache(listings_expiry_time=0.05)
    dc["a"] = 100

    time.sleep(0.08)

    with pytest.raises(KeyError):
        _ = dc["a"]


def test_dircache_expired_entry_not_in_cache():
    dc = DirCache(listings_expiry_time=0.05)
    dc["a"] = 100

    time.sleep(0.08)

    assert "a" not in dc


def test_dircache_missing_key_not_in_cache_with_expiry():
    dc = DirCache(listings_expiry_time=10)

    is_present = "missing" in dc

    assert is_present is False


def test_dircache_iter_excludes_expired_entries():
    dc = DirCache(listings_expiry_time=0.05)
    dc["a"] = 1
    dc["b"] = 2

    time.sleep(0.08)
    active_keys = list(dc)

    assert active_keys == []
    assert len(dc) == 0


def test_dircache_disabled_ignores_writes():
    dc = DirCache(use_listings_cache=False)

    dc["a"] = 1

    assert "a" not in dc
    assert len(dc) == 0
    assert list(dc) == []


def test_dircache_disabled_raises_key_error_on_read():
    dc = DirCache(use_listings_cache=False)

    with pytest.raises(KeyError):
        _ = dc["a"]


def test_dircache_del_removes_entry():
    dc = DirCache(listings_expiry_time=60, max_paths=10)
    dc["a"] = 1

    del dc["a"]

    assert "a" not in dc
    assert len(dc) == 0


def test_dircache_clear_removes_all_entries():
    dc = DirCache(listings_expiry_time=60, max_paths=10)
    dc["a"] = 1
    dc["b"] = 2

    dc.clear()

    assert len(dc) == 0
    assert "a" not in dc
    assert "b" not in dc


def test_dircache_pickle_roundtrip():
    dc = DirCache(use_listings_cache=True, listings_expiry_time=10, max_paths=5)

    dc2 = pickle.loads(pickle.dumps(dc))

    assert dc2.use_listings_cache is True
    assert dc2.listings_expiry_time == 10
    assert dc2.max_paths == 5
