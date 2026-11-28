import pickle
import time
from types import SimpleNamespace

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


@pytest.mark.parametrize("max_paths", [-1, 0, None])
def test_dircache_unlimited_paths_retains_all_items(max_paths):
    dc = DirCache(max_paths=max_paths)
    for i in range(10):
        dc[f"k{i}"] = i

    assert len(dc) == 10
    assert dc["k0"] == 0
    assert dc["k9"] == 9
    assert len(list(dc)) == 10


@pytest.mark.parametrize("operation", ["get", "contains", "iter"])
def test_dircache_expired_entry(operation):
    dc = DirCache(listings_expiry_time=0.05)
    dc["a"] = 100
    dc["b"] = 200
    time.sleep(0.08)

    # Use a fresh cache for each operation: checking expiry can evict entries.
    if operation == "get":
        with pytest.raises(KeyError):
            dc["a"]
    elif operation == "contains":
        assert "a" not in dc
    else:
        assert list(dc) == []
        assert len(dc) == 0


def test_dircache_missing_key_not_in_cache_with_expiry():
    dc = DirCache(listings_expiry_time=10)

    is_present = "missing" in dc

    assert is_present is False


def test_dircache_disabled():
    dc = DirCache(use_listings_cache=False)
    dc["a"] = 1
    assert "a" not in dc
    assert len(dc) == 0
    assert list(dc) == []
    with pytest.raises(KeyError):
        dc["a"]


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


@pytest.mark.parametrize("max_paths", [None, 0, 1, 2, 3])
@pytest.mark.parametrize("listings_expiry_time", [None, 60])
@pytest.mark.parametrize("view", ["items", "values"])
def test_dircache_views_after_reads(max_paths, listings_expiry_time, view):
    dc = DirCache(max_paths=max_paths, listings_expiry_time=listings_expiry_time)
    entries = {"a": 1, "b": 2, "c": 3}
    for key, value in entries.items():
        dc[key] = value

    # Start with a non-default LRU order when the cache is bounded.
    dc[next(iter(dc))]
    keys = list(dc)
    expected = [(key, entries[key]) for key in keys]
    if view == "values":
        expected = [value for _, value in expected]

    assert list(getattr(dc, view)()) == expected
    assert list(dc) == keys


@pytest.mark.parametrize("operation", ["get", "contains", "iter"])
@pytest.mark.parametrize("max_paths", [None, 2])
@pytest.mark.parametrize(
    "elapsed, wall_jump, expired",
    [(1, 1000, False), (6, -1000, True), (5, 1000, False), (5.001, -1000, True)],
)
def test_dircache_expiry_ignores_wall_clock_changes(
    monkeypatch, operation, max_paths, elapsed, wall_jump, expired
):
    clock = {"wall": 10000, "elapsed": 100}
    monkeypatch.setattr(
        "fsspec.dircache.time",
        SimpleNamespace(
            time=lambda: clock["wall"],
            monotonic=lambda: clock["elapsed"],
        ),
    )
    dc = DirCache(listings_expiry_time=5, max_paths=max_paths)
    dc["a"] = 100

    clock["wall"] += elapsed + wall_jump
    clock["elapsed"] += elapsed

    if operation == "get":
        if expired:
            with pytest.raises(KeyError):
                dc["a"]
        else:
            assert dc["a"] == 100
    elif operation == "contains":
        assert ("a" in dc) is not expired
    else:
        assert list(dc) == ([] if expired else ["a"])

    assert len(dc) == (0 if expired else 1)
