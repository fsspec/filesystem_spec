import copy
import pickle
from concurrent.futures import ThreadPoolExecutor

import pytest

from fsspec.implementations._memory_index import _MemoryDirectories, _MemoryStore


def check_directories(container, *, include_self):
    paths = ["", "/a", "/a/b", "/a/bc", "/other", "remote://bucket"]
    keys = list(container)
    for path in paths:
        expected = (include_self and path in keys) or any(
            key.startswith(path + "/") for key in keys
        )
        assert container.has_directory(path) == expected


def test_store_direct_mutations():
    store = _MemoryStore()
    store["/a/b/file"] = 1
    store["/a/b/second"] = 2
    store["/a/b/file"] = 3
    store.update({"/other/file": 4}, **{"remote://bucket/file": 5})
    store.setdefault("/a/b/file", 99)
    store.setdefault("/a/bc/file", 6)
    store |= {"/a/b/third": 7}
    check_directories(store, include_self=False)

    del store["/a/b/file"]
    assert store.pop("/a/b/second") == 2
    check_directories(store, include_self=False)
    assert store.popitem() == ("/a/b/third", 7)
    assert not store.has_directory("/a/b")
    assert store.has_directory("/a")
    assert store.pop("/missing", None) is None
    with pytest.raises(KeyError):
        store.pop("/missing")
    with pytest.raises(TypeError):
        store.pop("/other/file", 1, 2)
    check_directories(store, include_self=False)
    store.update(store)
    check_directories(store, include_self=False)
    store.clear()
    check_directories(store, include_self=False)


def test_store_same_size_replacement():
    store = _MemoryStore({"/a/b/file": 1})
    del store["/a/b/file"]
    store["/other/file"] = 2
    assert len(store) == 1
    assert not store.has_directory("/a")
    assert store.has_directory("/other")


def test_store_fromkeys():
    store = _MemoryStore.fromkeys(["/a/b/file", "/other/file"], 1)
    check_directories(store, include_self=False)
    store.__init__({"/a/bc/file": 2})
    assert store["/a/b/file"] == 1
    check_directories(store, include_self=False)


def test_partial_store_update():
    def entries():
        yield "/a/b/file", 1
        raise RuntimeError("input failed")

    store = _MemoryStore()
    with pytest.raises(RuntimeError, match="input failed"):
        store.update(entries())
    assert store == {"/a/b/file": 1}
    check_directories(store, include_self=False)


def test_directory_list_direct_mutations():
    dirs = _MemoryDirectories(["", "/a/b"])
    dirs.append("/a/b")
    dirs.extend(["/a/bc", "remote://bucket"])
    dirs.insert(0, "/other/nested")
    dirs.remove("/a/b")
    assert dirs.has_directory("/a/b")  # duplicate still present
    check_directories(dirs, include_self=True)
    dirs[0] = "/other/changed"
    dirs[1:3] = ["/a/deep/child", "/other/again"]
    check_directories(dirs, include_self=True)
    dirs += ["/a/b"]
    dirs *= 2
    check_directories(dirs, include_self=True)
    dirs.pop()
    del dirs[::2]
    check_directories(dirs, include_self=True)
    dirs.reverse()
    dirs.sort()
    check_directories(dirs, include_self=True)
    dirs *= 0
    check_directories(dirs, include_self=True)
    dirs.append("/a/b")
    dirs.clear()
    check_directories(dirs, include_self=True)


def test_partial_directory_extend():
    def entries():
        yield "/a/b"
        raise RuntimeError("input failed")

    dirs = _MemoryDirectories([""])
    with pytest.raises(RuntimeError, match="input failed"):
        dirs.extend(entries())
    assert dirs == ["", "/a/b"]
    check_directories(dirs, include_self=True)


def test_directory_sort_failed_mutation():
    dirs = _MemoryDirectories(["/a/b", "/other/nested"])

    def key(path):
        dirs.append("/a/bc")
        return path

    with pytest.raises(ValueError, match="modified during sort"):
        dirs.sort(key=key)
    check_directories(dirs, include_self=True)


@pytest.mark.parametrize("protocol", range(pickle.HIGHEST_PROTOCOL + 1))
@pytest.mark.parametrize(
    "container", [_MemoryStore({"/a/b/file": 1}), _MemoryDirectories(["", "/a/b"])]
)
def test_index_pickle(container, protocol):
    restored = pickle.loads(pickle.dumps(container, protocol=protocol))
    assert restored == container
    assert type(restored) is type(container)
    check_directories(restored, include_self=isinstance(restored, list))
    restored.clear()
    assert not restored.has_directory("/a")
    assert container.has_directory("/a")


@pytest.mark.parametrize("copier", [copy.copy, copy.deepcopy])
def test_index_copy(copier):
    for container in [
        _MemoryStore({"/a/b/file": 1}),
        _MemoryDirectories(["", "/a/b"]),
    ]:
        restored = copier(container)
        assert type(restored) is type(container)
        check_directories(restored, include_self=isinstance(restored, list))
        restored.clear()
        assert container.has_directory("/a")


def test_shared_index_concurrent_mutations():
    store = _MemoryStore()
    dirs = _MemoryDirectories([""])

    def worker(i):
        for j in range(100):
            path = f"/a/{i}/{j}/file"
            store[path] = j
            dirs.append(path)
            assert store.has_directory(f"/a/{i}/{j}")
            assert dirs.has_directory(path)
            del store[path]
            dirs.remove(path)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(worker, range(4)))
    assert store == {}
    assert dirs == [""]
    check_directories(store, include_self=False)
    check_directories(dirs, include_self=True)
