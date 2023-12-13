import pickle
import string

import pytest

from fsspec.caching import BlockCache, FirstChunkCache, caches, register_cache
from fsspec.implementations.cached import WholeFileCacheFileSystem


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
def test_known(sort, strict):
    parts = {(10, 20): b"1" * 10, (20, 30): b"2" * 10, (0, 10): b"0" * 10}
    if sort:
        parts = dict(sorted(parts.items()))
    c = caches["parts"](None, None, 100, parts, strict=strict)
    assert (0, 30) in c.data  # got consolidated
    assert c._fetch(5, 15) == b"0" * 5 + b"1" * 5
    assert c._fetch(15, 25) == b"1" * 5 + b"2" * 5
    if strict:
        # Over-read will raise error
        with pytest.raises(ValueError):
            # tries to call None fetcher
            c._fetch(25, 35)
    else:
        # Over-read will be zero-padded
        assert c._fetch(25, 35) == b"2" * 5 + b"\x00" * 5


def test_background(server, monkeypatch):
    import threading
    import time

    import fsspec

    head = {"head_ok": "true", "head_give_length": "true"}
    urla = server + "/index/realfile"
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
    fs.fs.put.assert_called_with(fs.fs.put.call_args[0][0], "/test", overwrite=True)
