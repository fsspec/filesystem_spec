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
