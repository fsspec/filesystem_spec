import threading

import pytest

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.registry import register_implementation
from fsspec.spec import AbstractFileSystem


class SyncDummyFS(AbstractFileSystem):
    cachable = True
    async_impl = False
    protocol = "syncdummy"


class AsyncDummyFS(AsyncFileSystem):
    cachable = True
    protocol = "asyncdummy"


@pytest.fixture(autouse=True)
def dummy_fs_setup():
    register_implementation("syncdummy", SyncDummyFS)
    register_implementation("asyncdummy", AsyncDummyFS)
    SyncDummyFS.clear_instance_cache()
    AsyncDummyFS.clear_instance_cache()
    yield
    SyncDummyFS.clear_instance_cache()
    AsyncDummyFS.clear_instance_cache()


def test_async_fs_sync_mode_shares_instance():
    results = {}
    lock = threading.Lock()

    def worker(thread_id):
        with lock:
            fs = fsspec.filesystem("asyncdummy", asynchronous=False)
        results[thread_id] = id(fs)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results[1] == results[2]


def test_async_fs_async_mode_does_not_share():
    results = {}
    lock = threading.Lock()

    def worker(thread_id):
        with lock:
            fs = fsspec.filesystem("asyncdummy", asynchronous=True)
        results[thread_id] = id(fs)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results[1] != results[2]


def test_sync_fs_does_not_share():
    results = {}
    lock = threading.Lock()

    def worker(thread_id):
        with lock:
            fs = fsspec.filesystem("syncdummy")
        results[thread_id] = id(fs)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results[1] != results[2]
