import os
import pytest
import pickle
import string
import tempfile

from fsspec.core import (
    _expand_paths,
    OpenFile,
    open_local,
    caches,
    get_compression,
    BaseCache,
    BlockCache,
)


@pytest.mark.parametrize(
    "path, name_function, num, out",
    [
        [["apath"], None, 1, ["apath"]],
        ["apath.*.csv", None, 1, ["apath.0.csv"]],
        ["apath.*.csv", None, 2, ["apath.0.csv", "apath.1.csv"]],
        ["a*", lambda x: "abc"[x], 2, ["aa", "ab"]],
    ],
)
def test_expand_paths(path, name_function, num, out):
    assert _expand_paths(path, name_function, num) == out


def test_expand_error():
    with pytest.raises(ValueError):
        _expand_paths("*.*", None, 1)


def test_openfile_api(m):
    m.open("somepath", "wb").write(b"data")
    of = OpenFile(m, "somepath")
    assert str(of) == "<OpenFile 'somepath'>"
    f = of.open()
    assert f.read() == b"data"
    f.close()
    with OpenFile(m, "somepath", mode="rt") as f:
        f.read() == "data"


def test_open_local():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    d2 = str(tempfile.mkdtemp())
    fn = open_local("simplecache://" + f1, cache_storage=d2, target_protocol="file")
    assert isinstance(fn, str)
    assert open(fn).read() == "test1"
    assert d2 in fn


# For test_cache_pickleable(). Functions are only picklable if they are defined
# at the top-level of a module
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
        result = cache[start:end]
        expected = string.ascii_letters[start:end].encode()
        assert result == expected


def test_xz_lzma_compressions():
    pytest.importorskip("lzma")
    # Ensure that both 'xz' and 'lzma' compression names can be parsed
    assert get_compression("some_file.xz", "infer") == "xz"
    assert get_compression("some_file.xz", "xz") == "xz"
    assert get_compression("some_file.xz", "lzma") == "lzma"


def test_cache_getitem(Cache_imp):
    cacher = Cache_imp(4, letters_fetcher, len(string.ascii_letters))
    assert cacher[0:4] == b"abcd"
    assert cacher[:4] == b"abcd"
    assert cacher[-3:] == b"XYZ"
    assert cacher[-3:-1] == b"XY"
    assert cacher[2:4] == b"cd"


def test_cache_getitem_raises():
    cacher = BaseCache(4, letters_fetcher, len(string.ascii_letters))
    with pytest.raises(TypeError, match="int"):
        cacher[5]

    with pytest.raises(ValueError, match="contiguous"):
        cacher[::4]


def test_block_cache_lru():
    cache = BlockCache(4, letters_fetcher, len(string.ascii_letters), maxblocks=2)
    # miss
    cache[0:2]
    assert cache.cache_info().hits == 0
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1

    # hit
    cache[0:2]
    assert cache.cache_info().hits == 1
    assert cache.cache_info().misses == 1
    assert cache.cache_info().currsize == 1

    # miss
    cache[4:6]
    assert cache.cache_info().hits == 1
    assert cache.cache_info().misses == 2
    assert cache.cache_info().currsize == 2

    # miss & evict
    cache[12:13]
    assert cache.cache_info().hits == 1
    assert cache.cache_info().misses == 3
    assert cache.cache_info().currsize == 2
