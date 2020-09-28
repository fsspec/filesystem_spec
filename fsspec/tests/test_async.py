import asyncio
from fsspec.asyn import _run_until_done


async def inner():
    await asyncio.sleep(1)
    return True


async def outer():
    await asyncio.sleep(1)
    return _run_until_done(inner())


def test_runtildone():
    loop = asyncio.get_event_loop()
    assert loop.run_until_complete(outer())
    loop.close()
