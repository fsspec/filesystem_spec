"""Tests the spec, using memoryfs"""

import pickle
from fsspec.implementations.memory import MemoryFileSystem


def test_idempotent():
    MemoryFileSystem.clear_instance_cache()
    fs = MemoryFileSystem()
    fs2 = MemoryFileSystem()
    assert fs is fs2
    assert MemoryFileSystem.current() is fs2
    fs2 = MemoryFileSystem(do_cache=False)
    assert fs is not fs2

    assert hash(fs) == hash(fs2)
    assert fs == fs2

    MemoryFileSystem.clear_instance_cache()
    assert not MemoryFileSystem._cache

    fs2 = MemoryFileSystem().current()
    assert fs == fs2


def test_pickle():
    fs = MemoryFileSystem()
    fs2 = pickle.loads(pickle.dumps(fs))
    assert fs == fs2


def test_class_methods():
    assert MemoryFileSystem._strip_protocol('memory:stuff') == "stuff"
    assert MemoryFileSystem._strip_protocol('memory://stuff') == "stuff"
    assert MemoryFileSystem._strip_protocol('stuff') == "stuff"
    assert MemoryFileSystem._strip_protocol('other://stuff') == "other://stuff"

    assert MemoryFileSystem._get_kwargs_from_urls("memory://user@thing") == {}
