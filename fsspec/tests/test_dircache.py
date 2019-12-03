from unittest.mock import patch

import pytest

from fsspec.dircache import DirCache, StaleKeyError


def test_init():
    assert DirCache().cache_timeout is None
    assert DirCache(cache_timeout=10).cache_timeout == 10


@patch("fsspec.dircache.time.time")
def test_get_set_basic(mock_time):
    mock_time.return_value = 10.0
    cache = DirCache(cache_timeout=1)

    # setitem
    mock_time.return_value = 10.5
    cache["a"] = 10

    assert cache._cache["a"] == (10.5, 10)

    # getitem
    mock_time.return_value = 10.75
    result = cache["a"]
    assert result == 10

    # getitem expired
    mock_time.return_value = 11.75
    with pytest.raises(StaleKeyError):
        cache["a"]


def test_delitem():
    cache = DirCache()
    cache["a"] = 10

    assert "a" in cache._cache
    del cache["a"]
    assert "a" not in cache._cache


@patch("fsspec.dircache.time.time")
def test_len_iter_expires_old(mock_time):
    mock_time.return_value = 10
    cache = DirCache(cache_timeout=1)

    cache["a"] = 1

    mock_time.return_value = 10.5
    cache["b"] = 2

    # a expires, b hasn't
    mock_time.return_value = 11.1
    assert len(cache) == 1
    assert list(iter(cache)) == ["b"]
