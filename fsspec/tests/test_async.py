import asyncio
import inspect
import sys

import pytest

import fsspec.asyn
from fsspec.asyn import _throttled_gather

try:
    import resource
except ImportError:
    resource = None


def test_sync_methods():
    inst = fsspec.asyn.AsyncFileSystem()
    assert inspect.iscoroutinefunction(inst._info)
    assert hasattr(inst, "info")
    assert not inspect.iscoroutinefunction(inst.info)


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in <3.7")
@pytest.mark.skipif(
    resource is None, reason="resource module is not available on this operating system"
)
def test_throttled_gather(monkeypatch):
    monkeypatch.setattr(resource, "getrlimit", lambda something: (32, 64))

    total_running = 0

    async def runner():
        nonlocal total_running

        total_running += 1
        await asyncio.sleep(0)
        if total_running > 4:
            raise ValueError("More than 4 coroutines are running together")
        total_running -= 1
        return 1

    async def main(disable=False):
        coros = [runner() for _ in range(32)]
        results = await _throttled_gather(
            coros, disable=disable, return_exceptions=True
        )
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results

    assert sum(asyncio.run(main())) == 32

    monkeypatch.setattr(resource, "getrlimit", lambda something: (64, 64))
    with pytest.raises(ValueError):
        asyncio.run(main())

    with pytest.raises(ValueError):
        asyncio.run(main(disable=True))
