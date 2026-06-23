import pickle
import string

import pytest

from fsspec.caching import (
    BlockCache,
    FirstChunkCache,
    MMapCache,
    ReadAheadCache,
    caches,
    register_cache,
)
from fsspec.implementations.cached import WholeFileCacheFileSystem


def test_cache_getitem(Cache_imp):
    cacher = Cache_imp(4, letters_fetcher, len(string.ascii_letters))
    assert cacher._fetch(0, 4) == b"abcd"
    assert cacher._fetch(None, 4) == b"abcd"
    assert cacher._fetch(2, 4) == b"cd"


def test_block_cache_lru():
    # BlockCache is a cache that stores blocks of data and uses LRU to evict
    block_size = 4
    cache = BlockCache(
        block_size, letters_fetcher, len(string.ascii_letters), maxblocks=2
    )
    # miss
    cache._fetch(0, 2)
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1
    assert cache.total_requested_bytes == block_size * cache.miss_count
    assert cache.size == 52

    # hit
    cache._fetch(0, 2)
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1
    assert cache.total_requested_bytes == block_size * cache.miss_count

    # hit
    cache._fetch(0, 2)
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1
    # this works as a counter since all the reads are from the cache
    assert cache.hit_count == 3
    assert cache.miss_count == 1
    # so far only 4 bytes have been read using range requests
    assert cache.total_requested_bytes == block_size * cache.miss_count

    # miss
    cache._fetch(4, 6)
    assert cache.cache_info().misses == 2
    assert cache.cache_info().currsize == 2
    assert cache.total_requested_bytes == block_size * cache.miss_count

    # miss & evict
    cache._fetch(12, 13)
    assert cache.cache_info().misses == 3
    assert cache.cache_info().currsize == 2
    assert cache.hit_count == 5
    assert cache.miss_count == 3
    assert cache.total_requested_bytes == block_size * cache.miss_count


def test_block_cache_lru_no_redundant_reads():
    block_size = 4
    maxblocks = 2
    cache = BlockCache(
        block_size, letters_fetcher, len(string.ascii_letters), maxblocks=maxblocks
    )
    cache._fetch(0, block_size * (maxblocks + 1))
    assert cache.cache_info().misses == 3


def test_first_cache():
    """
    FirstChunkCache is a cache that only caches the first chunk of data
    when some of that first block is requested.
    """
    block_size = 5
    cache = FirstChunkCache(block_size, letters_fetcher, len(string.ascii_letters))
    assert cache.cache is None
    assert cache._fetch(12, 15) == letters_fetcher(12, 15)
    assert cache.miss_count == 1
    assert cache.hit_count == 0
    assert cache.cache is None
    total_requested_bytes = 15 - 12
    assert cache.total_requested_bytes == total_requested_bytes

    # because we overlap with the cache range, it will be cached
    assert cache._fetch(3, 10) == letters_fetcher(3, 10)
    assert cache.miss_count == 2
    assert cache.hit_count == 0
    # we'll read the first 5 and then the rest
    total_requested_bytes += block_size + 5
    assert cache.total_requested_bytes == total_requested_bytes

    # partial hit again
    assert cache._fetch(3, 10) == letters_fetcher(3, 10)
    assert cache.miss_count == 2
    assert cache.hit_count == 1
    # we have the first 5 bytes cached
    total_requested_bytes += 10 - 5
    assert cache.total_requested_bytes == total_requested_bytes

    assert cache.cache == letters_fetcher(0, 5)
    assert cache._fetch(0, 4) == letters_fetcher(0, 4)
    assert cache.hit_count == 2
    assert cache.miss_count == 2
    assert cache.total_requested_bytes == 18


def test_readahead_cache():
    """
    ReadAheadCache is a cache that reads ahead of the requested range.
    If the access pattern is not sequential it will be very inefficient.
    """
    block_size = 5
    cache = ReadAheadCache(block_size, letters_fetcher, len(string.ascii_letters))
    assert cache._fetch(12, 15) == letters_fetcher(12, 15)
    assert cache.miss_count == 1
    assert cache.hit_count == 0
    total_requested_bytes = 15 - 12 + block_size
    assert cache.total_requested_bytes == total_requested_bytes

    assert cache._fetch(3, 10) == letters_fetcher(3, 10)
    assert cache.miss_count == 2
    assert cache.hit_count == 0
    assert len(cache.cache) == 12
    total_requested_bytes += (10 - 3) + block_size
    assert cache.total_requested_bytes == total_requested_bytes

    # caache hit again
    assert cache._fetch(3, 10) == letters_fetcher(3, 10)
    assert cache.miss_count == 2
    assert cache.hit_count == 1
    assert len(cache.cache) == 12
    assert cache.total_requested_bytes == total_requested_bytes
    assert cache.cache == letters_fetcher(3, 15)

    # cache miss
    assert cache._fetch(0, 4) == letters_fetcher(0, 4)
    assert cache.hit_count == 1
    assert cache.miss_count == 3
    assert len(cache.cache) == 9
    total_requested_bytes += (4 - 0) + block_size
    assert cache.total_requested_bytes == total_requested_bytes


def _fetcher(start, end):
    return b"0" * (end - start)


def letters_fetcher(start, end):
    return string.ascii_letters[start:end].encode()


def multi_letters_fetcher(ranges):
    return [string.ascii_letters[start:end].encode() for start, end in ranges]


not_parts_caches = {k: v for k, v in caches.items() if k != "parts"}


@pytest.fixture(params=not_parts_caches.values(), ids=list(not_parts_caches))
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


def test_first_cache():
    c = FirstChunkCache(5, letters_fetcher, 52)
    assert c.cache is None
    assert c._fetch(12, 15) == letters_fetcher(12, 15)
    assert c.cache is None
    assert c._fetch(3, 10) == letters_fetcher(3, 10)
    assert c.cache == letters_fetcher(0, 5)
    c.fetcher = None
    assert c._fetch(1, 4) == letters_fetcher(1, 4)


def test_mmap_cache(mocker):
    fetcher = mocker.Mock(wraps=letters_fetcher)
    c = MMapCache(5, fetcher, 52)
    assert c._fetch(6, 8) == letters_fetcher(6, 8)
    assert fetcher.call_count == 1
    assert c._fetch(17, 22) == letters_fetcher(17, 22)
    assert fetcher.call_count == 2
    assert c._fetch(1, 38) == letters_fetcher(1, 38)
    assert fetcher.call_count == 5

    multi_fetcher = mocker.Mock(wraps=multi_letters_fetcher)
    m = MMapCache(5, fetcher, size=52, multi_fetcher=multi_fetcher)
    assert m._fetch(6, 8) == letters_fetcher(6, 8)
    assert multi_fetcher.call_count == 1
    assert m._fetch(17, 22) == letters_fetcher(17, 22)
    assert multi_fetcher.call_count == 2
    assert m._fetch(1, 38) == letters_fetcher(1, 38)
    assert multi_fetcher.call_count == 3
    assert fetcher.call_count == 5


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


@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("sort", [True, False])
def test_known(strict, sort):
    parts = {
        (10, 20): b"1" * 10,
        (20, 30): b"2" * 10,
        (0, 10): b"0" * 10,
        (40, 50): b"3" * 10,
    }
    if sort:
        parts = dict(sorted(parts.items()))
    c = caches["parts"](None, None, 100, parts, strict=strict)
    assert c.size == 40
    assert (0, 30) in c.data  # got consolidated
    assert c.nblocks == 2

    assert c._fetch(5, 15) == b"0" * 5 + b"1" * 5
    assert c._fetch(15, 25) == b"1" * 5 + b"2" * 5
    assert c.hit_count
    assert not c.miss_count

    if strict:
        # Over-read will raise error
        with pytest.raises(ValueError):
            c._fetch(25, 35)
        with pytest.raises(ValueError):
            c._fetch(25, 45)
    else:
        assert c._fetch(25, 35) == b"2" * 5 + b"\x00" * 5
        assert c._fetch(25, 45) == b"2" * 5 + b"\x00" * 10 + b"3" * 5
    assert c.miss_count


def test_background(server, monkeypatch):
    import threading
    import time

    import fsspec

    head = {"head_ok": "true", "head_give_length": "true"}
    urla = server.realfile
    h = fsspec.filesystem("http", headers=head)
    thread_ids = {threading.current_thread().ident}
    f = h.open(urla, block_size=5, cache_type="background")
    orig = f.cache._fetch_block

    def wrapped(*a, **kw):
        thread_ids.add(threading.current_thread().ident)
        return orig(*a, **kw)

    f.cache._fetch_block = wrapped
    assert len(thread_ids) == 1
    f.read(1)
    time.sleep(0.1)  # second block is loading
    assert len(thread_ids) == 2


def test_register_cache():
    # just test that we have them populated and fail to re-add again unless overload
    with pytest.raises(ValueError):
        register_cache(BlockCache)
    register_cache(BlockCache, clobber=True)


def test_cache_kwargs(mocker):
    # test that kwargs are passed to the underlying filesystem after cache commit

    fs = WholeFileCacheFileSystem(target_protocol="memory")
    fs.touch("test")
    fs.fs.put = mocker.MagicMock()

    with fs.open("test", "wb", overwrite=True) as file_handle:
        file_handle.write(b"foo")

    # We don't care about the first parameter, just retrieve its expected value.
    # It is a random location that cannot be predicted.
    # The important thing is the 'overwrite' kwarg
    fs.fs.put.assert_called_with(fs.fs.put.call_args[0][0], ["/test"], overwrite=True)
