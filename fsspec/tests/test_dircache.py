import time

from fsspec.dircache import DirCache


def test_max_paths_evicts_oldest_on_write():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2
    dc["c"] = 3
    dc["d"] = 4
    # Only the two most recent entries are retained.
    assert len(dc) == 2
    assert dict(dc._cache) == {"c": 3, "d": 4}


def test_max_paths_iteration_does_not_destroy_cache():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2
    assert sorted(dc) == ["a", "b"]
    # Iteration must not evict entries (regression: __iter__ used __getitem__,
    # which popped entries out of the cache).
    assert sorted(dc) == ["a", "b"]
    assert len(dc) == 2


def test_max_paths_access_refreshes_recency():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2
    assert dc["a"] == 1  # refresh 'a'
    dc["c"] = 3
    # 'b' was the least recently used and should be evicted, not 'a'.
    assert dict(dc._cache) == {"a": 1, "c": 3}


def test_no_max_paths_keeps_all():
    dc = DirCache()
    for i in range(10):
        dc[f"p{i}"] = i
    assert len(dc) == 10
    assert sorted(dc) == [f"p{i}" for i in range(10)]


def test_deleted_path_removed_from_recency_tracking():
    dc = DirCache(max_paths=2)
    dc["a"] = 1
    dc["b"] = 2
    del dc["a"]
    dc["c"] = 3
    assert dict(dc._cache) == {"b": 2, "c": 3}


def test_expiry_still_applies():
    dc = DirCache(listings_expiry_time=0.1)
    dc["a"] = 1
    time.sleep(0.2)
    try:
        dc["a"]
    except KeyError:
        pass
    else:
        raise AssertionError("expected expired entry to raise KeyError")
