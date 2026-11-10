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
