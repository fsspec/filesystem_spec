import pytest

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.memory import MemoryFileSystem


class ObjectStoreMemoryFS(MemoryFileSystem):
    """Model a store whose virtual directories are not copyable objects."""

    def cp_file(self, path1, path2, **kwargs):
        if self.isdir(path1) or path1.endswith("broken"):
            raise FileNotFoundError(path1)
        super().cp_file(path1, path2, **kwargs)


class AsyncObjectStoreFS(AsyncFileSystem):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.store = ObjectStoreMemoryFS()

    async def _ls(self, path, detail=True, **kwargs):
        return self.store.ls(path, detail=detail, **kwargs)

    async def _info(self, path, **kwargs):
        return self.store.info(path, **kwargs)

    async def _cp_file(self, path1, path2, **kwargs):
        return self.store.cp_file(path1, path2, **kwargs)

    async def _makedirs(self, path, exist_ok=False):
        return self.store.makedirs(path, exist_ok=exist_ok)

    async def _rm_file(self, path, **kwargs):
        if self.store.isfile(path):
            self.store.rm_file(path)


@pytest.fixture(params=[ObjectStoreMemoryFS, AsyncObjectStoreFS])
def object_store(request):
    fs = request.param(skip_instance_cache=True)
    store = fs.store if isinstance(fs, AsyncObjectStoreFS) else fs
    store.store.clear()
    store.pseudo_dirs[:] = [""]
    yield fs, store
    store.store.clear()
    store.pseudo_dirs[:] = [""]


@pytest.mark.parametrize("operation", ["copy", "mv"])
def test_recursive_object_copy(object_store, operation):
    fs, store = object_store
    store.pipe({"/src/a": b"a", "/src/sub/b": b"b"})
    getattr(fs, operation)(
        "/src",
        "/dst",
        recursive=True,
        **({"on_error": "raise"} if operation == "copy" else {}),
    )
    assert store.cat("/dst/a") == b"a"
    assert store.cat("/dst/sub/b") == b"b"
    assert store.exists("/src/a") == (operation == "copy")


def test_recursive_move_retains_source_on_copy_failure(object_store):
    fs, store = object_store
    store.pipe({"/src/a": b"a", "/src/broken": b"b"})
    with pytest.raises(FileNotFoundError, match="broken"):
        fs.mv("/src", "/dst", recursive=True)
    assert store.cat("/src/a") == b"a"
    assert store.cat("/src/broken") == b"b"


def test_recursive_copy_empty_directory(object_store):
    fs, store = object_store
    store.makedirs("/src/empty")
    fs.copy("/src", "/dst", recursive=True, on_error="raise")
    assert store.isdir("/dst/empty")


class MarkerAwareMemoryFS(ObjectStoreMemoryFS):
    """Simulate a backend where a directory marker is also a file."""

    def isdir(self, path):
        if path.rstrip("/") == "/src/marker":
            return True
        return super().isdir(path)

    def isfile(self, path):
        if path.rstrip("/") == "/src/marker":
            return True
        return super().isfile(path)

    def cp_file(self, path1, path2, **kwargs):
        if path1.rstrip("/") == "/src/marker":
            return MemoryFileSystem.cp_file(self, path1, path2, **kwargs)
        return super().cp_file(path1, path2, **kwargs)


def test_recursive_copy_preserves_directory_marker():
    fs = MarkerAwareMemoryFS(skip_instance_cache=True)
    fs.store.clear()
    fs.pseudo_dirs[:] = [""]

    try:
        fs.pipe("/src/data.txt", b"data")
        fs.pipe("/src/marker/", b"marker-content")

        fs.copy("/src", "/dst", recursive=True, on_error="raise")

        assert fs.cat("/dst/data.txt") == b"data"
        assert fs.cat("/dst/marker") == b"marker-content"
    finally:
        fs.store.clear()
        fs.pseudo_dirs[:] = [""]


class AsyncMarkerAwareFS(AsyncObjectStoreFS):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.store = MarkerAwareMemoryFS(skip_instance_cache=True)

    async def _info(self, path, **kwargs):
        if path.rstrip("/") == "/src/marker":
            return {
                "name": "/src/marker",
                "size": len(self.store.cat("/src/marker")),
                "type": "file",
            }
        return await super()._info(path, **kwargs)


def test_async_recursive_copy_preserves_directory_marker():
    fs = AsyncMarkerAwareFS(skip_instance_cache=True)
    store = fs.store
    store.store.clear()
    store.pseudo_dirs[:] = [""]

    try:
        store.pipe("/src/data.txt", b"data")
        store.pipe("/src/marker/", b"marker-content")

        fs.copy("/src", "/dst", recursive=True, on_error="raise")

        assert store.cat("/dst/data.txt") == b"data"
        assert store.cat("/dst/marker") == b"marker-content"
    finally:
        store.store.clear()
        store.pseudo_dirs[:] = [""]


def test_explicit_recursive_copy_avoids_metadata_lookup():
    from unittest.mock import Mock

    from fsspec.spec import AbstractFileSystem

    fs = AbstractFileSystem(skip_instance_cache=True)
    fs.info = Mock(side_effect=AssertionError("Unexpected metadata lookup"))
    fs.cp_file = Mock()

    fs.copy(
        ["bucket/source/file"],
        ["bucket/target/file"],
        recursive=True,
        on_error="raise",
    )

    fs.info.assert_not_called()
    fs.cp_file.assert_called_once_with("bucket/source/file", "bucket/target/file")


def test_explicit_recursive_copy_preserves_slash_object():
    from unittest.mock import Mock

    from fsspec.spec import AbstractFileSystem

    fs = AbstractFileSystem(skip_instance_cache=True)
    fs.info = Mock(return_value={"type": "directory"})
    fs.cp_file = Mock()
    fs.makedirs = Mock()

    fs.copy(
        ["bucket/source/marker/"],
        ["bucket/target/marker/"],
        recursive=True,
        on_error="raise",
    )

    fs.cp_file.assert_called_once_with("bucket/source/marker/", "bucket/target/marker/")
    fs.makedirs.assert_not_called()


@pytest.mark.parametrize("payload", [b"", b"marker-content"])
@pytest.mark.parametrize("maxdepth", [None, 2])
def test_recursive_copy_flat_store_marker(payload, maxdepth):
    from unittest.mock import Mock

    from fsspec.spec import AbstractFileSystem

    fs = AbstractFileSystem(skip_instance_cache=True)

    objects = {
        "bucket/src/marker/": payload,
        "bucket/src/data": b"data",
    }

    entries = {
        "bucket/src": {
            "name": "bucket/src",
            "type": "directory",
            "size": 0,
        },
        **{
            name: {
                "name": name,
                "type": "file",
                "size": len(data),
            }
            for name, data in objects.items()
        },
    }

    fs.find = Mock(
        side_effect=lambda path, detail=False, **kwargs: (
            entries if detail else sorted(entries)
        )
    )

    def info(path, **kwargs):
        if path in ("bucket/src", "bucket/src/marker/"):
            return {"name": path.rstrip("/"), "type": "directory"}
        if path in entries:
            return entries[path]
        raise FileNotFoundError(path)

    def cp_file(source, target, **kwargs):
        objects[target] = objects[source]

    fs.info = Mock(side_effect=info)
    fs.cp_file = Mock(side_effect=cp_file)
    fs.makedirs = Mock()

    fs.copy(
        "bucket/src",
        "bucket/dst",
        recursive=True,
        maxdepth=maxdepth,
        on_error="raise",
    )

    assert objects["bucket/dst/marker/"] == payload
    assert objects["bucket/dst/data"] == b"data"
    if maxdepth is None:
        fs.makedirs.assert_called_once_with("bucket/dst", exist_ok=True)
    else:
        fs.makedirs.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"", b"marker-content"])
@pytest.mark.parametrize("maxdepth", [None, 2])
async def test_async_flat_store_marker(payload, maxdepth):
    from unittest.mock import AsyncMock

    fs = AsyncFileSystem(
        asynchronous=True,
        skip_instance_cache=True,
    )

    objects = {
        "bucket/src/marker/": payload,
        "bucket/src/data": b"data",
    }

    entries = {
        "bucket/src": {"type": "directory"},
        "bucket/src/marker/": {"type": "file"},
        "bucket/src/data": {"type": "file"},
    }

    async def copy_file(source, target, **kwargs):
        objects[target] = objects[source]

    fs._find = AsyncMock(return_value=entries)
    fs._isdir = AsyncMock(return_value=False)
    fs._info = AsyncMock(side_effect=AssertionError("Unexpected metadata lookup"))
    fs._cp_file = AsyncMock(side_effect=copy_file)
    fs._makedirs = AsyncMock()

    await fs._copy(
        "bucket/src",
        "bucket/dst",
        recursive=True,
        maxdepth=maxdepth,
        on_error="raise",
    )

    assert objects["bucket/dst/marker/"] == payload
    assert objects["bucket/dst/data"] == b"data"
    fs._info.assert_not_awaited()
    if maxdepth is None:
        fs._makedirs.assert_awaited_once_with("bucket/dst", exist_ok=True)
    else:
        fs._makedirs.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_explicit_copy_avoids_metadata_lookup():
    from unittest.mock import AsyncMock

    fs = AsyncFileSystem(
        asynchronous=True,
        skip_instance_cache=True,
    )

    fs._info = AsyncMock(side_effect=AssertionError("Unexpected metadata lookup"))
    fs._cp_file = AsyncMock()
    fs._makedirs = AsyncMock()

    await fs._copy(
        ["bucket/src/marker/"],
        ["bucket/dst/marker/"],
        recursive=True,
        on_error="raise",
    )

    fs._info.assert_not_awaited()
    fs._makedirs.assert_not_awaited()
    fs._cp_file.assert_awaited_once_with("bucket/src/marker/", "bucket/dst/marker/")


def test_recursive_copy_glob(object_store):
    fs, store = object_store
    store.pipe(
        {
            "/src/sub/a": b"a",
            "/src/sub/b": b"b",
        }
    )

    fs.copy(
        "/src/*",
        "/dst",
        recursive=True,
        on_error="raise",
    )

    assert store.cat("/dst/a") == b"a"
    assert store.cat("/dst/b") == b"b"


def test_recursive_copy_invalid_maxdepth(object_store):
    fs, store = object_store
    store.pipe("/src/a", b"a")

    with pytest.raises(ValueError, match="maxdepth"):
        fs.copy("/src", "/dst", recursive=True, maxdepth=0)


@pytest.mark.parametrize("maxdepth", [None, 2])
@pytest.mark.parametrize("fail_copy", [False, True])
def test_flat_store_marker_move(maxdepth, fail_copy):
    from unittest.mock import Mock

    from fsspec.spec import AbstractFileSystem

    fs = AbstractFileSystem(skip_instance_cache=True)

    source = "bucket/src/marker/"
    target = "bucket/dst/marker/"
    payload = b"marker-content"

    objects = {source: payload}

    entries = {
        "bucket/src": {"type": "directory"},
        source: {"type": "file", "size": len(payload)},
    }

    fs.find = Mock(
        side_effect=lambda path, detail=False, **kwargs: (
            entries if detail else sorted(entries)
        )
    )

    fs.isdir = Mock(side_effect=lambda path: path in ("bucket/src", source))
    fs.makedirs = Mock()
    fs.exists = Mock(return_value=False)

    def copy_file(src, dst, **kwargs):
        if fail_copy:
            raise FileNotFoundError(src)
        objects[dst] = objects[src]

    def remove(paths, recursive=False, **kwargs):
        if isinstance(paths, str):
            paths = [p for p in objects if p.startswith(paths.rstrip("/") + "/")]
        for path in paths:
            objects.pop(path, None)

    fs.cp_file = Mock(side_effect=copy_file)
    fs.rm = Mock(side_effect=remove)

    if fail_copy:
        with pytest.raises(FileNotFoundError):
            fs.mv(
                "bucket/src",
                "bucket/dst",
                recursive=True,
                maxdepth=maxdepth,
            )

        assert objects[source] == payload
        assert target not in objects
        fs.rm.assert_not_called()
    else:
        fs.mv(
            "bucket/src",
            "bucket/dst",
            recursive=True,
            maxdepth=maxdepth,
        )

        assert objects[target] == payload
        assert source not in objects


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_single_marker_destination(existing, asynchronous):
    from unittest.mock import AsyncMock, Mock

    from fsspec.spec import AbstractFileSystem

    source = "bucket/src/marker/"
    destination = "bucket/dst"
    expected = "bucket/dst/src/marker/" if existing else "bucket/dst/marker/"

    entries = {
        "bucket/src": {"type": "directory"},
        source: {"type": "file", "size": 14},
    }

    if asynchronous:
        fs = AsyncFileSystem(
            asynchronous=True,
            skip_instance_cache=True,
        )
        fs._find = AsyncMock(return_value=entries)
        fs._isdir = AsyncMock(return_value=existing)
        fs._cp_file = AsyncMock()

        await fs._copy(
            "bucket/src",
            destination,
            recursive=True,
            maxdepth=2,
            on_error="raise",
        )

        fs._cp_file.assert_awaited_once_with(source, expected)
    else:
        fs = AbstractFileSystem(skip_instance_cache=True)
        fs.find = Mock(return_value=entries)
        fs.isdir = Mock(return_value=existing)
        fs.cp_file = Mock()

        fs.copy(
            "bucket/src",
            destination,
            recursive=True,
            maxdepth=2,
            on_error="raise",
        )

        fs.cp_file.assert_called_once_with(source, expected)
