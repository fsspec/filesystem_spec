import asyncio
import inspect
import sys

import pytest

import fsspec.asyn
from fsspec.asyn import _throttled_gather


def test_sync_methods():
    inst = fsspec.asyn.AsyncFileSystem()
    assert inspect.iscoroutinefunction(inst._info)
    assert hasattr(inst, "info")
    assert not inspect.iscoroutinefunction(inst.info)


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in <3.7")
def test_throttled_gather(monkeypatch):
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
        coros = [runner() for _ in range(32)]
        results = await _throttled_gather(coros, **kwargs)
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results

    assert sum(asyncio.run(main(batch_size=4))) == 32

    with pytest.raises(ValueError):
        asyncio.run(main(batch_size=5, return_exceptions=True))

    with pytest.raises(ValueError):
        asyncio.run(main(batch_size=-1, return_exceptions=True))
