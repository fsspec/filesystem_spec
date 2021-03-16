import asyncio
import sys

import pytest

from fsspec.asyn import _run_until_done


async def inner():
    await asyncio.sleep(1)
    return True


async def outer(loop):
    await asyncio.sleep(1)
    return _run_until_done(loop, inner())


@pytest.mark.skipif(sys.version_info < (3, 7), reason="Async fails on py36")
def test_runtildone():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    assert loop.run_until_complete(outer(loop))
    loop.close()
