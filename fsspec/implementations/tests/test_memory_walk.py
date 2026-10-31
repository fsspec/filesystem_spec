"""Memory walk's ordered, mutable traversal and extension contracts."""

import copy
import weakref
from datetime import UTC, datetime
from types import MethodType

import pytest

from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile, MemoryFileSystem


class GenericMemory(MemoryFileSystem):
    walk = AbstractFileSystem.walk


class InheritedMemory(MemoryFileSystem):
    pass


def populate(fs):
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]
    for i in range(80):
        name = f"/tree/d{i:03}/file"
        fs.store[name] = MemoryFile(fs, name, b"data")
        fs.store[name].created = datetime(2020, 1, 1, tzinfo=UTC)
    fs.pseudo_dirs.extend(["/tree/d079/empty", "/tree/d079/deeper/empty"])


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("topdown", [False, True])
@pytest.mark.parametrize("global_store", [False, True])
@pytest.mark.parametrize(
    "change",
    [
        "add",
        "delete",
        "rename",
        "reinsert",
        "reorder",
        "replace",
        "write",
        "created",
        "clear",
        "rebind",
        "add-dir",
        "replace-dir",
        "reorder-dirs",
        "clear-dirs",
        "rebind-dirs",
    ],
)
def test_walk_live_changes(detail, topdown, global_store, change):
    def run(cls):
        fs = cls(global_store=global_store, skip_instance_cache=True)
        populate(fs)
        other = MemoryFileSystem(skip_instance_cache=True) if global_store else fs
        key = "/tree/d079/file"
        out = []
        for row in fs.walk("/tree", detail=detail, topdown=topdown):
            out.append(copy.deepcopy(row))
            if row[0] != "/tree/d040":
                continue
            if change == "add":
                other.pipe_file("/tree/d079/new", b"new")
                fs.store["/tree/d079/new"].created = datetime(2020, 1, 1, tzinfo=UTC)
            elif change == "delete":
                other.store.pop(key)
            elif change == "rename":
                other.store[key + "-renamed"] = other.store.pop(key)
            elif change == "reinsert":
                other.store[key] = other.store.pop(key)
            elif change == "reorder":
                fs.store = dict(reversed(list(fs.store.items())))
            elif change == "replace":
                other.store[key] = MemoryFile(data=b"replacement")
                other.store[key].created = datetime(2021, 1, 1, tzinfo=UTC)
            elif change == "write":
                other.store[key].write(b"longer-content")
            elif change == "created":
                other.store[key].created = datetime(2024, 1, 1, tzinfo=UTC)
            elif change == "clear":
                other.store.clear()
            elif change == "rebind":
                fs.store = {k: v for k, v in fs.store.items() if k != key}
            elif change == "add-dir":
                other.pseudo_dirs.append("/tree/d079/new-dir")
            elif change == "replace-dir":
                other.pseudo_dirs[-1] = "/tree/d079/replaced-dir"
            elif change == "reorder-dirs":
                other.pseudo_dirs.reverse()
            elif change == "clear-dirs":
                other.pseudo_dirs.clear()
            elif change == "rebind-dirs":
                fs.pseudo_dirs = ["", "/tree/d079/rebound-dir"]
        fs.store.clear()
        fs.pseudo_dirs[:] = [""]
        return out

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("topdown", [False, True])
@pytest.mark.parametrize("keep", [0, 1, 25, 31, 32, 33, 40, 80])
def test_walk_pruning_and_order(detail, topdown, keep):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        out = []
        for path, dirs, files in fs.walk(
            "memory://tree/", detail=detail, topdown=topdown
        ):
            if path == "/tree":
                selected = list(dirs)[::-1][:keep]
                if detail:
                    saved = [(k, dirs[k]) for k in selected]
                    dirs.clear()
                    dirs.update(saved)
                else:
                    dirs[:] = selected
            out.append(copy.deepcopy((path, dirs, files)))
        return out

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("method", ["ls", "walk"])
@pytest.mark.parametrize("level", ["class", "instance", "subclass"])
@pytest.mark.parametrize("during", [False, True])
def test_walk_public_dispatch(monkeypatch, method, level, during):
    class ExtendedMemory(MemoryFileSystem):
        pass

    cls = ExtendedMemory if level == "subclass" else MemoryFileSystem
    fs = cls(global_store=False, skip_instance_cache=True)
    populate(fs)
    calls = []
    original = getattr(cls, method)

    def wrapped(self, path, **kwargs):
        assert kwargs.get("sentinel") == "preserved"
        calls.append(path)
        return original(self, path, **kwargs)

    def install():
        if level == "instance":
            monkeypatch.setattr(fs, method, MethodType(wrapped, fs))
        else:
            monkeypatch.setattr(cls, method, wrapped)

    if not during:
        install()
    result = []
    for row in fs.walk("/tree", sentinel="preserved"):
        result.append(row)
        if during and row[0] == "/tree/d040":
            install()
    assert len(result) == 84
    expected = [row[0] for row in result]
    assert calls == (expected[42:] if during else expected)


@pytest.mark.parametrize("on_error", ["omit", "raise", "callback"])
@pytest.mark.parametrize("topdown", [False, True])
def test_walk_reads_metadata_only_when_visited(on_error, topdown):
    class BrokenFile:
        @property
        def size(self):
            raise PermissionError("unreadable metadata")

    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        fs.store["/unrelated/bad"] = BrokenFile()
        fs.store["/tree/d079/file"] = BrokenFile()
        errors = []
        mode = errors.append if on_error == "callback" else on_error
        rows = []
        iterator = fs.walk("/tree", on_error=mode, topdown=topdown)
        if on_error == "raise":
            with pytest.raises(PermissionError, match="unreadable metadata"):
                rows.extend(iterator)
        else:
            rows = list(iterator)
        assert len(rows) >= 79
        return rows, [(type(e), str(e)) for e in errors]

    assert run(MemoryFileSystem) == run(GenericMemory)


def test_walk_lazy_and_independent_lifetime(monkeypatch):
    # Check lifetime without depending on the index activation threshold.
    import fsspec.implementations.memory as memory

    refs = []
    original = memory._MemoryWalk.__init__

    def track(self, fs):
        original(self, fs)
        refs.append(weakref.ref(self))

    monkeypatch.setattr(memory._MemoryWalk, "__init__", track)
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)
    populate(fs)
    it = fs.walk("/tree")
    assert refs == []
    it.close()
    assert refs == []
    for stop in (1, 30, 33, 45):
        it = fs.walk("/tree")
        for _ in range(stop):
            next(it)
        assert refs[-1]() is not None
        it.close()
        assert refs[-1]() is None
    first, second = fs.walk("/tree"), fs.walk("/tree", detail=True)
    next(first)
    next(second)
    assert refs[-1]() is not refs[-2]()
    first.close()
    second.close()
    assert all(ref() is None for ref in refs)


@pytest.mark.parametrize("topdown", [True, False])
@pytest.mark.parametrize("cls", [MemoryFileSystem, GenericMemory, InheritedMemory])
def test_walk_deep_chain(topdown, cls):
    fs = cls(global_store=False, skip_instance_cache=True)
    fs.pipe_file("/tree" + "/d" * 500 + "/file", b"data")
    rows = list(fs.walk("/tree", topdown=topdown))
    assert len(rows) == 501
    assert rows[-1 if topdown else 0][2] == ["file"]


def test_walk_invalid_depth_does_not_resolve_custom_ls(monkeypatch):
    fs = MemoryFileSystem(global_store=False, skip_instance_cache=True)

    def inaccessible(self):
        raise AssertionError("ls was accessed before validating maxdepth")

    monkeypatch.setattr(MemoryFileSystem, "ls", property(inaccessible))
    with pytest.raises(ValueError, match="maxdepth"):
        next(fs.walk("/tree", maxdepth=0))


@pytest.mark.parametrize("topdown", [True, False])
@pytest.mark.parametrize("detail", [True, False])
def test_walk_conflicting_and_unusual_names(topdown, detail):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        for name in (
            "/tree/d079/both",
            "/tree/d079/both/child",
            "/tree/d079//double",
            "/tree/d079/trailing/",
            "/tree/d079/memory://literal/file",
        ):
            fs.store[name] = MemoryFile(data=b"data")
            fs.store[name].created = datetime(2020, 1, 1, tzinfo=UTC)
        fs.pseudo_dirs.extend(
            [
                "/tree/d079/both",
                "/tree/d079/trailing/",
                "/tree/d079/empty",
                "/tree/d079//double-dir",
            ]
        )
        return list(fs.walk("/tree", topdown=topdown, detail=detail))

    assert run(MemoryFileSystem) == run(GenericMemory)
