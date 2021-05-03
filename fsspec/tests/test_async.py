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


# After the except is returned, the other coroutines gets automatically
# ignored and they raise RuntimeWarnings. We could overcome this by cancelling
# all the futures in gather, though that require return_exceptions to be
# set True on gather. Since we don't need it that much, we simply ignore the
# exceptions in this test case.
@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in <3.7")
@pytest.mark.skipif(
    resource is None, reason="resource module is not available on this operating system"
)
@pytest.mark.filterwarnings("ignore: coroutine")
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
        return await _throttled_gather(coros, disable=disable)

    assert sum(asyncio.run(main())) == 32

    monkeypatch.setattr(resource, "getrlimit", lambda something: (64, 64))
    with pytest.raises(ValueError):
        asyncio.run(main())

    with pytest.raises(ValueError):
        asyncio.run(main(disable=True))
