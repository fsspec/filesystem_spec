import asyncio
import inspect
import time

import fsspec.asyn


def test_sync_methods():
    inst = fsspec.asyn.AsyncFileSystem()
    assert inspect.iscoroutinefunction(inst._info)
    assert hasattr(inst, "info")
    assert not inspect.iscoroutinefunction(inst.info)


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
