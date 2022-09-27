import asyncio
import inspect
import os
import time

import pytest

import fsspec
import fsspec.asyn
from fsspec.asyn import _run_coros_in_chunks, get_running_loop


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

    dummy_func = fsspec.asyn.sync_wrapper(_dummy_async_func)


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


@pytest.mark.skipif(os.name != "nt", reason="only for windows")
def test_windows_policy():
    from asyncio.windows_events import SelectorEventLoop

    loop = fsspec.asyn.get_loop()
    policy = asyncio.get_event_loop_policy()

    # Ensure that the created loop always uses selector policy
    assert isinstance(loop, SelectorEventLoop)

    # Ensure that the global policy is not changed and it is
    # set to the default one. This is important since the
    # get_loop() method will temporarily override the policy
    # with the one which uses selectors on windows, so this
    # check ensures that we are restoring the old policy back
    # after our change.
    assert isinstance(policy, asyncio.DefaultEventLoopPolicy)


def test_fsspec_loop():
    asyncio._set_running_loop(None)

    with fsspec.asyn.fsspec_loop() as loop:
        assert get_running_loop() is loop
        assert get_running_loop() is fsspec.asyn.get_loop()

    with pytest.raises(RuntimeError):
        get_running_loop()

    original_loop = asyncio.new_event_loop()
    asyncio._set_running_loop(original_loop)

    with fsspec.asyn.fsspec_loop() as loop:
        assert get_running_loop() is loop
        assert get_running_loop() is fsspec.asyn.get_loop()

    assert get_running_loop() is original_loop
