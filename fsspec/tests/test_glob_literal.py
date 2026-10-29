import asyncio

import pytest

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem


@pytest.mark.parametrize("backend", ["local", "memory", "async"])
@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("kind", ["file", "directory", "missing"])
@pytest.mark.parametrize("slash", [False, True])
def test_literal_glob(tmp_path, backend, detail, kind, slash):
    fs = (
        LocalFileSystem(skip_instance_cache=True)
        if backend != "memory"
        else MemoryFileSystem(skip_instance_cache=True)
    )
    root = fs._strip_protocol(tmp_path.as_posix())
    fs.makedirs(root, exist_ok=True)
    path = root + "/" + kind
    if kind == "file":
        fs.pipe_file(path, b"payload")
    elif kind == "directory":
        fs.mkdir(path)
    query = path + ("/" if slash else "")
    if backend == "async":

        class BackedAsync(AsyncFileSystem):
            @classmethod
            def _strip_protocol(cls, path):
                return LocalFileSystem._strip_protocol(path)

            async def _info(self, path, **kwargs):
                return fs.info(path, **kwargs)

        actual = asyncio.run(
            BackedAsync(asynchronous=True, skip_instance_cache=True)._glob(
                query, detail=detail
            )
        )
    else:
        actual = fs.glob(query, detail=detail)
    expected = kind != "missing" and (not slash or kind == "directory")
    assert list(actual) == ([path] if expected else [])
    if detail and expected:
        assert actual[path]["type"] == kind


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("kind", ["file", "directory"])
def test_literal_glob_metadata_kwargs(asynchronous, detail, kind):
    from fsspec.spec import AbstractFileSystem

    calls = []

    def get_info(path, **kwargs):
        assert kwargs == {"version_id": "old"}
        calls.append(path)
        return {"name": path, "type": kind, "size": 0}

    if asynchronous:

        class Backend(AsyncFileSystem):
            async def _info(self, path, **kwargs):
                return get_info(path, **kwargs)

        actual = asyncio.run(
            Backend(asynchronous=True, skip_instance_cache=True)._glob(
                "/item/", detail=detail, version_id="old"
            )
        )
    else:

        class Backend(AbstractFileSystem):
            def info(self, path, **kwargs):
                return get_info(path, **kwargs)

        actual = Backend(skip_instance_cache=True).glob(
            "/item/", detail=detail, version_id="old"
        )
    assert list(actual) == (["/item"] if kind == "directory" else [])
    assert calls
