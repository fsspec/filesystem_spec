import pickle
import string

import pytest
from fsspec.caching import BlockCache, caches


def test_cache_getitem(Cache_imp):
    cacher = Cache_imp(4, letters_fetcher, len(string.ascii_letters))
    assert cacher._fetch(0, 4) == b"abcd"
    assert cacher._fetch(None, 4) == b"abcd"
    assert cacher._fetch(2, 4) == b"cd"


def test_block_cache_lru():
    cache = BlockCache(4, letters_fetcher, len(string.ascii_letters), maxblocks=2)
    # miss
    cache._fetch(0, 2)
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1

    # hit
    cache._fetch(0, 2)
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1

    # miss
    cache._fetch(4, 6)
    assert cache.cache_info().misses == 2
    assert cache.cache_info().currsize == 2

    # miss & evict
    cache._fetch(12, 13)
    assert cache.cache_info().misses == 3
    assert cache.cache_info().currsize == 2


def _fetcher(start, end):
    return b"0" * (end - start)


def letters_fetcher(start, end):
    return string.ascii_letters[start:end].encode()


@pytest.fixture(params=caches.values(), ids=list(caches.keys()))
def Cache_imp(request):
    return request.param


def test_cache_empty_file(Cache_imp):
    blocksize = 5
    size = 0
    cache = Cache_imp(blocksize, _fetcher, size)
    assert cache._fetch(0, 0) == b""


def test_cache_pickleable(Cache_imp):
    blocksize = 5
    size = 100
    cache = Cache_imp(blocksize, _fetcher, size)
    cache._fetch(0, 5)  # fill in cache
    unpickled = pickle.loads(pickle.dumps(cache))
    assert isinstance(unpickled, Cache_imp)
    assert unpickled.blocksize == blocksize
    assert unpickled.size == size
    assert unpickled._fetch(0, 10) == b"0" * 10


@pytest.mark.parametrize(
    "size_requests",
    [[(0, 30), (0, 35), (51, 52)], [(0, 1), (1, 11), (1, 52)], [(0, 52), (11, 15)]],
)
@pytest.mark.parametrize("blocksize", [1, 10, 52, 100])
def test_cache_basic(Cache_imp, blocksize, size_requests):
    cache = Cache_imp(blocksize, letters_fetcher, len(string.ascii_letters))

    for start, end in size_requests:
        result = cache._fetch(start, end)
        expected = string.ascii_letters[start:end].encode()
        assert result == expected
