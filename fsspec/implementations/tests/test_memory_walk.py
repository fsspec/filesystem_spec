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


@pytest.mark.parametrize("path", ["memory://tree", "memory://memory://tree"])
@pytest.mark.parametrize("detail", [True, False])
@pytest.mark.parametrize("topdown", [True, False])
@pytest.mark.parametrize("on_error", ["omit", "raise", "callback"])
@pytest.mark.parametrize("keep", [31, 32, 40])
def test_walk_repeated_protocol(path, detail, topdown, on_error, keep):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        errors = []
        mode = errors.append if on_error == "callback" else on_error
        rows = []
        for row in fs.walk(path, detail=detail, topdown=topdown, on_error=mode):
            if row[0] in ("/tree", "memory://tree") and topdown:
                if detail:
                    for name in list(row[1])[keep:]:
                        del row[1][name]
                else:
                    row[1][:] = row[1][:keep]
            rows.append(copy.deepcopy(row))
        assert errors == []
        root = "/tree" if path == "memory://tree" else "memory://tree"
        assert rows[0 if topdown else -1][0] == root
        return rows

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("detail", [True, False])
@pytest.mark.parametrize("topdown", [True, False])
def test_walk_containers_with_mutation_hooks(monkeypatch, detail, topdown):
    import fsspec.implementations.memory as memory

    class Store(dict):
        def __setitem__(self, key, value):
            super().__setitem__(key, value)

    class Directories(list):
        def append(self, value):
            super().append(value)

    rebuilds = []
    original = memory._MemoryWalk._rebuild

    def rebuild(self, *args):
        rebuilds.append(self.fs)
        return original(self, *args)

    monkeypatch.setattr(memory._MemoryWalk, "_rebuild", rebuild)

    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        fs.store = Store(fs.store)
        fs.pseudo_dirs = Directories(fs.pseudo_dirs)
        rows = []
        for row in fs.walk("/tree", detail=detail, topdown=topdown):
            rows.append(copy.deepcopy(row))
            if row[0] == "/tree/d040":
                fs.store["/tree/d079/new"] = MemoryFile(data=b"new")
                fs.store["/tree/d079/new"].created = datetime(2020, 1, 1, tzinfo=UTC)
                fs.pseudo_dirs.append("/tree/d079/new-dir")
        return rows, fs

    actual, fs = run(MemoryFileSystem)
    expected, _ = run(GenericMemory)
    assert actual == expected
    assert fs in rebuilds


@pytest.mark.parametrize("container", ["store", "pseudo_dirs"])
@pytest.mark.parametrize(
    "method", ["__iter__", "__len__", "__contains__", "__getitem__"]
)
@pytest.mark.parametrize("during", [False, True])
def test_walk_custom_container_reads(monkeypatch, container, method, during):
    import fsspec.implementations.memory as memory

    rebuilds = []
    original = memory._MemoryWalk._rebuild

    def rebuild(self, *args):
        rebuilds.append(self.fs)
        return original(self, *args)

    monkeypatch.setattr(memory._MemoryWalk, "_rebuild", rebuild)

    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        builtin = dict if container == "store" else list
        calls = []

        def read(self, *args):
            calls.append((method, args))
            return getattr(builtin, method)(self, *args)

        custom = type("CustomReads", (builtin,), {method: read})

        def install():
            setattr(fs, container, custom(getattr(fs, container)))

        if not during:
            install()
        rows = []
        for row in fs.walk("/tree", detail=True):
            rows.append(copy.deepcopy(row))
            if during and row[0] == "/tree/d040":
                install()
                rebuilds.clear()
        return rows, calls, fs

    actual, calls, fs = run(MemoryFileSystem)
    assert fs not in rebuilds
    expected, expected_calls, _ = run(GenericMemory)
    assert actual == expected
    assert calls == expected_calls


@pytest.mark.parametrize("container", ["store", "pseudo_dirs"])
@pytest.mark.parametrize("during", [False, True])
def test_walk_custom_container_error_timing(container, during):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        builtin = dict if container == "store" else list

        class Unreadable(builtin):
            def __iter__(self):
                raise PermissionError("custom iteration")

        if not during:
            setattr(fs, container, Unreadable(getattr(fs, container)))
        rows, errors = [], []
        for row in fs.walk("/tree", on_error=errors.append):
            rows.append(copy.deepcopy(row))
            if during and row[0] == "/tree/d040":
                setattr(fs, container, Unreadable(getattr(fs, container)))
        return rows, [(type(e), str(e)) for e in errors]

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("container", ["store", "pseudo_dirs"])
def test_walk_container_read_descriptor(container):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        builtin = dict if container == "store" else list
        calls = []

        class Iteration:
            def __get__(self, instance, owner):
                if instance is None:
                    return builtin.__iter__

                def read():
                    calls.append("iterated")
                    return builtin.__iter__(instance)

                return read

        class Custom(builtin):
            __iter__ = Iteration()

        setattr(fs, container, Custom(getattr(fs, container)))
        rows = list(fs.walk("/tree"))
        return rows, calls

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("container", ["store", "pseudo_dirs"])
def test_walk_container_class_property(container):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        calls = []

        class Custom:
            def __init__(self, values):
                self.values = values

            def __iter__(self):
                return iter(self.values)

            def __len__(self):
                return len(self.values)

            def __contains__(self, value):
                return value in self.values

            def __getitem__(self, key):
                return self.values[key]

            @property
            def __class__(self):
                calls.append("class property read")
                return type(self)

        setattr(fs, container, Custom(getattr(fs, container)))
        rows = list(fs.walk("/tree"))
        assert calls == []
        return rows

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("container", ["store", "pseudo_dirs"])
@pytest.mark.parametrize("kind", ["property", "nondata", "shadowed", "binding"])
@pytest.mark.parametrize("mode", ["reads", "omit", "callback", "raise", "depth"])
def test_walk_filesystem_container_descriptor(monkeypatch, container, kind, mode):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        values = getattr(fs, container)
        calls, errors = [], []

        def read(instance):
            calls.append(container)
            if mode != "reads":
                raise PermissionError("container access")
            return values

        class Nondata:
            def __get__(self, instance, owner):
                assert instance is not None
                return read(instance)

        class Binding(dict if container == "store" else list):
            # A container can itself be a descriptor, even though all its read
            # operations inherit builtin semantics.
            def __get__(self, instance, owner):
                assert instance is not None
                return read(instance)

        descriptor = (
            property(read)
            if kind == "property"
            else Binding(values)
            if kind == "binding"
            else Nondata()
        )
        if kind != "shadowed":
            del fs.__dict__[container]
        with monkeypatch.context() as patch:
            patch.setattr(MemoryFileSystem, container, descriptor)
            kwargs = {"maxdepth": 0 if mode == "depth" else 1}
            if mode == "callback":
                kwargs["on_error"] = errors.append
            elif mode == "raise":
                kwargs["on_error"] = "raise"
            if mode == "depth":
                with pytest.raises(ValueError, match="maxdepth"):
                    list(fs.walk("", **kwargs))
                result = "invalid depth"
            elif mode == "raise" and kind != "shadowed":
                with pytest.raises(PermissionError, match="container access"):
                    list(fs.walk("", **kwargs))
                result = "container error"
            else:
                result = list(fs.walk("", **kwargs))
        return result, calls, [(type(e), str(e)) for e in errors]

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("container", ["store", "pseudo_dirs"])
@pytest.mark.parametrize("after", [0, 35])
@pytest.mark.parametrize("raises", [False, True])
def test_walk_filesystem_descriptor_replaced_during_iteration(
    monkeypatch, container, after, raises
):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        values = getattr(fs, container)
        calls, errors, out = [], [], []

        def read(instance):
            calls.append(container)
            if raises:
                raise PermissionError("container access")
            return values

        with monkeypatch.context() as patch:
            for pos, row in enumerate(fs.walk("/tree", on_error=errors.append)):
                out.append(copy.deepcopy(row))
                if pos == after:
                    patch.setattr(MemoryFileSystem, container, property(read))
        return out, calls, [(type(e), str(e)) for e in errors]

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("attribute", ["__getattribute__", "__getattr__"])
@pytest.mark.parametrize("after", [None, 35])
def test_walk_filesystem_attribute_protocol(monkeypatch, attribute, after):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        calls, out = [], []
        instance_dict = fs.__dict__

        def getattribute(instance, name):
            if name in ("store", "pseudo_dirs", "__dict__"):
                calls.append(name)
            return object.__getattribute__(instance, name)

        def getattr_missing(instance, name):
            calls.append(name)
            if name == "store":
                return values
            raise AttributeError(name)

        with monkeypatch.context() as patch:

            def replace():
                if attribute == "__getattr__":
                    # Force the missing-attribute path without changing the
                    # store used by ls().
                    instance_dict.pop("store")
                    patch.delattr(MemoryFileSystem, "store")
                patch.setattr(
                    MemoryFileSystem,
                    attribute,
                    getattribute
                    if attribute == "__getattribute__"
                    else getattr_missing,
                    raising=False,
                )

            values = fs.store
            if after is None:
                replace()
            for pos, row in enumerate(fs.walk("/tree")):
                out.append(copy.deepcopy(row))
                if pos == after:
                    replace()
        return out, calls

    assert run(MemoryFileSystem) == run(GenericMemory)


@pytest.mark.parametrize("after", [None, 35])
def test_walk_filesystem_instance_namespace(after):
    def run(cls):
        fs = cls(global_store=False, skip_instance_cache=True)
        populate(fs)
        calls, out = [], []

        class Namespace(dict):
            def __contains__(self, name):
                calls.append(name)
                return super().__contains__(name)

        def replace():
            fs.__dict__ = Namespace(fs.__dict__)

        if after is None:
            replace()
        for pos, row in enumerate(fs.walk("/tree")):
            out.append(copy.deepcopy(row))
            if pos == after:
                replace()
        return out, calls

    assert run(MemoryFileSystem) == run(GenericMemory)
