import asyncio
import inspect
import io
import time

import pytest

import fsspec
import fsspec.asyn
from fsspec.asyn import _run_coros_in_chunks


def test_sync_methods():
    inst = fsspec.asyn.AsyncFileSystem()
    assert inspect.iscoroutinefunction(inst._info)
    assert hasattr(inst, "info")
    assert inst.info.__qualname__ == "AsyncFileSystem._info"
    assert not inspect.iscoroutinefunction(inst.info)


def test_when_sync_methods_are_disabled():
    class TestFS(fsspec.asyn.AsyncFileSystem):
        mirror_sync_methods = False

    inst = TestFS()
    assert inspect.iscoroutinefunction(inst._info)
    assert not inspect.iscoroutinefunction(inst.info)
    assert inst.info.__qualname__ == "AbstractFileSystem.info"


def test_interrupt():
    loop = fsspec.asyn.get_loop()

    async def f():
        await asyncio.sleep(1000000)
        return True

    fut = asyncio.run_coroutine_threadsafe(f(), loop)
    time.sleep(0.01)  # task launches
    out = fsspec.asyn._dump_running_tasks(with_task=True)
    task = out[0]["task"]
    assert task.done() and fut.done()
    assert isinstance(fut.exception(), fsspec.asyn.FSSpecCoroutineCancel)


class _DummyAsyncKlass:
    def __init__(self):
        self.loop = fsspec.asyn.get_loop()

    async def _dummy_async_func(self):
        # Sleep 1 second function to test timeout
        await asyncio.sleep(1)
        return True

    async def _bad_multiple_sync(self):
        fsspec.asyn.sync_wrapper(_DummyAsyncKlass._dummy_async_func)(self)
        return True

    dummy_func = fsspec.asyn.sync_wrapper(_dummy_async_func)
    bad_multiple_sync_func = fsspec.asyn.sync_wrapper(_bad_multiple_sync)


def test_sync_wrapper_timeout_on_less_than_expected_wait_time_not_finish_function():
    test_obj = _DummyAsyncKlass()
    with pytest.raises(fsspec.FSTimeoutError):
        test_obj.dummy_func(timeout=0.1)


def test_sync_wrapper_timeout_on_more_than_expected_wait_time_will_finish_function():
    test_obj = _DummyAsyncKlass()
    assert test_obj.dummy_func(timeout=5)


def test_sync_wrapper_timeout_none_will_wait_func_finished():
    test_obj = _DummyAsyncKlass()
    assert test_obj.dummy_func(timeout=None)


def test_sync_wrapper_treat_timeout_0_as_none():
    test_obj = _DummyAsyncKlass()
    assert test_obj.dummy_func(timeout=0)


def test_sync_wrapper_bad_multiple_sync():
    test_obj = _DummyAsyncKlass()
    with pytest.raises(NotImplementedError):
        test_obj.bad_multiple_sync_func(timeout=5)


def test_run_coros_in_chunks(monkeypatch):
    total_running = 0

    async def runner():
        nonlocal total_running

        total_running += 1
        await asyncio.sleep(0)
        if total_running > 4:
            raise ValueError("More than 4 coroutines are running together")
        total_running -= 1
        return 1

    async def main(**kwargs):
        nonlocal total_running

        total_running = 0
        coros = [runner() for _ in range(32)]
        results = await _run_coros_in_chunks(coros, **kwargs)
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results

    assert sum(asyncio.run(main(batch_size=4))) == 32

    with pytest.raises(ValueError):
        asyncio.run(main(batch_size=5))

    with pytest.raises(ValueError):
        asyncio.run(main(batch_size=-1))

    assert sum(asyncio.run(main(batch_size=4))) == 32

    monkeypatch.setitem(fsspec.config.conf, "gather_batch_size", 5)
    with pytest.raises(ValueError):
        asyncio.run(main())
    assert sum(asyncio.run(main(batch_size=4))) == 32  # override

    monkeypatch.setitem(fsspec.config.conf, "gather_batch_size", 4)
    assert sum(asyncio.run(main())) == 32  # override


def test_running_async():
    assert not fsspec.asyn.running_async()

    async def go():
        assert fsspec.asyn.running_async()

    asyncio.run(go())


class DummyAsyncFS(fsspec.asyn.AsyncFileSystem):
    _file_class = fsspec.asyn.AbstractAsyncStreamedFile

    async def _info(self, path, **kwargs):
        return {"name": "misc/foo.txt", "type": "file", "size": 100}

    async def open_async(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return DummyAsyncStreamedFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )


class DummyAsyncStreamedFile(fsspec.asyn.AbstractAsyncStreamedFile):
    def __init__(self, fs, path, mode, block_size, autocommit, **kwargs):
        super().__init__(fs, path, mode, block_size, autocommit, **kwargs)
        self.temp_buffer = io.BytesIO(b"foo-bar" * 20)

    async def _fetch_range(self, start, end):
        return self.temp_buffer.read(end - start)

    async def _initiate_upload(self):
        # Reinitialize for new uploads.
        self.temp_buffer = io.BytesIO()

    async def _upload_chunk(self, final=False):
        self.temp_buffer.write(self.buffer.getbuffer())

    async def get_data(self):
        return self.temp_buffer.getbuffer().tobytes()


@pytest.mark.asyncio
async def test_async_streamed_file_write():
    test_fs = DummyAsyncFS()
    streamed_file = await test_fs.open_async("misc/foo.txt", mode="wb")
    inp_data = b"foo-bar" * streamed_file.blocksize * 2
    await streamed_file.write(inp_data)
    assert streamed_file.loc == len(inp_data)
    await streamed_file.close()
    out_data = await streamed_file.get_data()
    assert out_data.count(b"foo-bar") == streamed_file.blocksize * 2


@pytest.mark.asyncio
async def test_async_streamed_file_read():
    test_fs = DummyAsyncFS()
    streamed_file = await test_fs.open_async("misc/foo.txt", mode="rb")
    assert (
        await streamed_file.read(7 * 3) + await streamed_file.read(7 * 18)
        == b"foo-bar" * 20
    )
    await streamed_file.close()


def test_rm_file_with_rm_implementation():
    class AsyncFSWithRm(fsspec.asyn.AsyncFileSystem):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.removed_paths = []

        async def _rm(self, path, recursive=False, batch_size=None, **kwargs):
            if isinstance(path, str):
                path = [path]
            for p in path:
                self.removed_paths.append(p)
            return None

    fs = AsyncFSWithRm()
    fs.rm_file("test/file.txt")
    assert "test/file.txt" in fs.removed_paths


def test_rm_file_with_rm_file_implementation():
    class AsyncFSWithRmFile(fsspec.asyn.AsyncFileSystem):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.removed_paths = []

        async def _rm_file(self, path, **kwargs):
            self.removed_paths.append(path)
            return None

    fs = AsyncFSWithRmFile()
    fs.rm_file("test/file.txt")
    assert "test/file.txt" in fs.removed_paths


def test_rm_file_without_implementation():
    fs = fsspec.asyn.AsyncFileSystem()
    with pytest.raises(NotImplementedError):
        fs.rm_file("test/file.txt")


# ---------------------------------------------------------------------------
# Tests for the prefix= hint that _glob passes to _find
# ---------------------------------------------------------------------------

_GLOB_PREFIX_FILES = [
    "data/2024/results.csv",
    "data/2024/report.txt",
    "data/2023/results.csv",
    "top_results.csv",
    "top_other.txt",
    "other/results.csv",
]


class _PrefixCapturingFS(fsspec.asyn.AsyncFileSystem):
    """Minimal AsyncFileSystem that records every _find call's kwargs.

    _find ignores the prefix hint and returns all files under *root* so that
    the client-side glob pattern-matching in _glob still works correctly.
    This simulates a "naive" backend that silently absorbs unknown kwargs.
    """

    protocol = "prefixmock"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.find_calls = []

    async def _find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        self.find_calls.append({"path": path, "kwargs": dict(kwargs)})
        root = (path.rstrip("/") + "/") if path else ""
        results = {
            f: {"name": f, "type": "file", "size": 0}
            for f in _GLOB_PREFIX_FILES
            if f.startswith(root)
        }
        return results if detail else list(results)

    async def _info(self, path, **kwargs):
        for f in _GLOB_PREFIX_FILES:
            if f == path:
                return {"name": f, "type": "file", "size": 0}
        raise FileNotFoundError(path)


@pytest.fixture
def prefix_fs():
    return _PrefixCapturingFS(skip_instance_cache=True)


@pytest.mark.parametrize(
    "pattern, expected_results, expected_prefix",
    [
        # root/stem* -> prefix="stem"
        (
            "data/2024/res*",
            ["data/2024/results.csv"],
            "res",
        ),
        # root/stem*.ext -> prefix="stem"
        (
            "data/2024/re*.txt",
            ["data/2024/report.txt"],
            "re",
        ),
        # stem* (no slash) -> prefix="stem"
        (
            "top_*",
            ["top_other.txt", "top_results.csv"],
            "top_",
        ),
        # ? wildcard: prefix is everything before the first ?
        (
            "top_r?sults.csv",
            ["top_results.csv"],
            "top_r",
        ),
        # [ wildcard: prefix is everything before the first [
        # re[rp]* translates to re[rp][^/]* so only report.txt matches (not results.csv)
        (
            "data/2024/re[rp]*",
            ["data/2024/report.txt"],
            "re",
        ),
        # root/* (wildcard immediately after /) -> empty prefix, NOT forwarded
        (
            "data/2024/*",
            ["data/2024/report.txt", "data/2024/results.csv"],
            None,
        ),
        # bare * (no prefix at all) -> NOT forwarded
        # * translates to [^/]+ so paths containing / are excluded
        (
            "*",
            ["top_other.txt", "top_results.csv"],
            None,
        ),
    ],
)
def test_glob_prefix_hint(prefix_fs, pattern, expected_results, expected_prefix):
    """_glob should extract the literal stem before the first wildcard and
    forward it as ``prefix=`` to ``_find``.  When the stem is empty the kwarg
    must not be forwarded at all so that backends that reject unknown kwargs
    are not broken.  The glob results must be correct regardless."""
    results = prefix_fs.glob(pattern)
    assert sorted(results) == sorted(expected_results)

    assert len(prefix_fs.find_calls) == 1
    forwarded = prefix_fs.find_calls[0]["kwargs"]

    if expected_prefix is None:
        assert "prefix" not in forwarded, (
            f"prefix= should not be forwarded for pattern {pattern!r}, "
            f"but got prefix={forwarded.get('prefix')!r}"
        )
    else:
        assert forwarded.get("prefix") == expected_prefix, (
            f"expected prefix={expected_prefix!r} for pattern {pattern!r}, "
            f"got {forwarded.get('prefix')!r}"
        )
