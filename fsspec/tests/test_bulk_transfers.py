import asyncio
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.callbacks import Callback
from fsspec.generic import copy_file_op


class AsyncLocalTransfers(AsyncFileSystem):
    cachable = False

    async def _cp_file(self, source, destination, **kwargs):
        await asyncio.to_thread(
            fsspec.filesystem("file").cp_file, source, destination, **kwargs
        )

    async def _get_file(self, source, destination, **kwargs):
        await asyncio.to_thread(
            fsspec.filesystem("file").get_file, source, destination, **kwargs
        )

    async def _put_file(self, source, destination, **kwargs):
        await asyncio.to_thread(
            fsspec.filesystem("file").put_file, source, destination, **kwargs
        )


@pytest.fixture
def local_batch(tmp_path):
    sources = [tmp_path / name for name in ("source-b", "source-a")]
    destinations = [tmp_path / name for name in ("destination-b", "destination-a")]
    for path, data in zip(sources, (b"first", b"second")):
        path.write_bytes(data)
    for path in destinations:
        path.write_bytes(b"keep existing destination")
    return sources, destinations


@pytest.mark.parametrize(
    "source_count,destination_count", [(2, 1), (1, 2), (2, 0), (0, 2)]
)
@pytest.mark.parametrize(
    "method,on_error",
    [("copy", "raise"), ("copy", "ignore"), ("get", None), ("put", None)],
)
@pytest.mark.parametrize("backend", ["local", "async-sync", "async-coroutine"])
def test_invalid_transfer_batches_leave_files_and_callbacks_unchanged(
    local_batch, source_count, destination_count, method, on_error, backend
):
    sources, destinations = local_batch
    before = {path: path.read_bytes() for path in sources + destinations}
    fs = fsspec.filesystem("file") if backend == "local" else AsyncLocalTransfers()
    source_paths = [str(path) for path in sources[:source_count]]
    destination_paths = [str(path) for path in destinations[:destination_count]]
    callback = Callback(size=99, value=17)
    kwargs = {"on_error": on_error} if method == "copy" else {"callback": callback}

    with pytest.raises(ValueError, match="same length"):
        if backend == "async-coroutine":
            asyncio.run(
                getattr(fs, "_" + method)(source_paths, destination_paths, **kwargs)
            )
        else:
            getattr(fs, method)(source_paths, destination_paths, **kwargs)

    assert {path: path.read_bytes() for path in before} == before
    assert callback.size == 99
    assert callback.value == 17


@pytest.mark.parametrize("method", ["copy", "get", "put"])
@pytest.mark.parametrize("backend", ["local", "async-sync", "async-coroutine"])
@pytest.mark.parametrize("path_type", [str, Path])
def test_equal_transfer_batches_preserve_pair_order(
    local_batch, method, backend, path_type
):
    sources, destinations = local_batch
    fs = fsspec.filesystem("file") if backend == "local" else AsyncLocalTransfers()
    source_paths = [path_type(path) for path in sources]
    destination_paths = [path_type(path) for path in destinations]

    if backend == "async-coroutine":
        asyncio.run(getattr(fs, "_" + method)(source_paths, destination_paths))
    else:
        getattr(fs, method)(source_paths, destination_paths)

    assert [path.read_bytes() for path in sources] == [b"first", b"second"]
    assert [path.read_bytes() for path in destinations] == [b"first", b"second"]


@pytest.mark.parametrize("method", ["copy", "get", "put"])
@pytest.mark.parametrize("backend", ["local", "async-sync", "async-coroutine"])
def test_empty_transfer_batches_are_noops(local_batch, method, backend):
    sources, destinations = local_batch
    before = {path: path.read_bytes() for path in sources + destinations}
    fs = fsspec.filesystem("file") if backend == "local" else AsyncLocalTransfers()

    if backend == "async-coroutine":
        asyncio.run(getattr(fs, "_" + method)([], []))
    else:
        getattr(fs, method)([], [])

    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize(
    "source_count,destination_count", [(2, 1), (1, 2), (2, 0), (0, 2)]
)
@pytest.mark.parametrize(
    "method,on_error",
    [("copy", "raise"), ("copy", "ignore"), ("copy", "return"), ("mv", None)],
)
@pytest.mark.parametrize("source_backend", ["file", "memory"])
def test_invalid_generic_batches_preserve_all_sources_and_destinations(
    local_batch, m, source_count, destination_count, method, on_error, source_backend
):
    sources, destinations = local_batch
    local_before = {path: path.read_bytes() for path in sources + destinations}
    memory_sources = ["memory:///source-b", "memory:///source-a"]
    m.pipe(dict(zip(memory_sources, (b"first", b"second"))))
    source_paths = (
        [path.as_uri() for path in sources]
        if source_backend == "file"
        else memory_sources
    )
    kwargs = {"on_error": on_error} if method == "copy" else {}
    fsspec.filesystem("file")
    fs = fsspec.filesystem("generic", default_method="current")

    with pytest.raises(ValueError, match="same length"):
        getattr(fs, method)(
            source_paths[:source_count],
            [path.as_uri() for path in destinations[:destination_count]],
            **kwargs,
        )

    assert {path: path.read_bytes() for path in local_before} == local_before
    assert [m.cat_file(path) for path in memory_sources] == [b"first", b"second"]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_empty_generic_copy_batches_are_noops(local_batch, m, asynchronous):
    sources, destinations = local_batch
    before = {path: path.read_bytes() for path in sources + destinations}
    m.pipe_file("source", b"keep memory file")
    fs = fsspec.filesystem("generic", asynchronous=asynchronous)

    if asynchronous:
        asyncio.run(fs._copy([], []))
    else:
        fs.copy([], [])

    assert {path: path.read_bytes() for path in before} == before
    assert m.cat_file("source") == b"keep memory file"


@pytest.mark.parametrize("method", ["copy", "mv"])
@pytest.mark.parametrize("source_backend", ["file", "memory"])
def test_equal_generic_batches_preserve_pair_order(
    local_batch, m, method, source_backend
):
    sources, destinations = local_batch
    source_paths = [path.as_uri() for path in sources]
    if source_backend == "memory":
        source_paths = ["memory:///source-b", "memory:///source-a"]
        m.pipe(dict(zip(source_paths, (b"first", b"second"))))
    fsspec.filesystem("file")
    fs = fsspec.filesystem("generic", default_method="current")

    getattr(fs, method)(source_paths, [path.as_uri() for path in destinations])

    assert [path.read_bytes() for path in destinations] == [b"first", b"second"]
    if method == "mv":
        assert not any(fs.exists(path) for path in source_paths)
    else:
        assert [fs.cat_file(path) for path in source_paths] == [b"first", b"second"]


@pytest.mark.parametrize(
    "source_count,destination_count", [(2, 1), (1, 2), (2, 0), (0, 2)]
)
@pytest.mark.parametrize("on_error", ["raise", "ignore", "return"])
def test_invalid_copy_file_op_batches_preserve_sources_and_destinations(
    local_batch, m, source_count, destination_count, on_error
):
    sources, _ = local_batch
    destinations = ["/destination-b", "/destination-a"]
    before = dict.fromkeys(destinations, b"keep existing destination")
    m.pipe(before)

    with pytest.raises(ValueError, match="same length"):
        asyncio.run(
            copy_file_op(
                fsspec.filesystem("file"),
                [str(path) for path in sources[:source_count]],
                m,
                destinations[:destination_count],
                on_error=on_error,
            )
        )

    assert [path.read_bytes() for path in sources] == [b"first", b"second"]
    assert m.cat(destinations) == before


@pytest.mark.parametrize("container", [list, tuple, iter])
def test_equal_copy_file_op_batches_preserve_pair_order(local_batch, m, container):
    sources, _ = local_batch
    destinations = ["/destination-b", "/destination-a"]

    asyncio.run(
        copy_file_op(
            fsspec.filesystem("file"),
            container(str(path) for path in sources),
            m,
            container(destinations),
            on_error="raise",
        )
    )

    assert [path.read_bytes() for path in sources] == [b"first", b"second"]
    assert [m.cat_file(path) for path in destinations] == [b"first", b"second"]


@pytest.mark.parametrize("glob_source", [False, True])
def test_optimized_python_rejects_unequal_move_batches(glob_source):
    script = textwrap.dedent(
        f"""
        import fsspec

        fs = fsspec.filesystem("memory", global_store=False, skip_instance_cache=True)
        before = {{"/source-b": b"first", "/source-a": b"second", "/destination": b"keep"}}
        fs.pipe(before)
        sources = "/source-*" if {glob_source!r} else ["/source-b", "/source-a"]
        try:
            fs.mv(sources, ["/destination"])
        except ValueError:
            pass
        else:
            raise RuntimeError("Unequal path lists were accepted")
        if fs.cat(list(before)) != before:
            raise RuntimeError("Invalid move changed files")
        """
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
