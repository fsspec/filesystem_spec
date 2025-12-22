import asyncio
import concurrent.interpreters
from collections.abc import Callable
from typing import Any


async def one():
    # test function
    print("one")
    return 1


class SubinterpreterAsyncWorker:
    def __init__(
        self, qin: concurrent.interpreters.Queue, qout: concurrent.interpreters.Queue
    ):
        self.qin = qin
        self.qout = qout
        self.loop = None
        try:
            # creates in-thread-interpreter loop
            asyncio.run(self.wait())
        finally:
            self.loop.close()
            qout.put(None)

    async def wait(self):
        self.loop = asyncio.get_running_loop()
        # simulate run_forever and allow debug/liveness
        while True:
            batch = self.qin.get()
            if batch is None:
                # poison
                break
            assert isinstance(batch, list)
            self.qout.put(await self.run_batch(batch))

    async def run_batch(self, batch: list[tuple[Callable, Any]]) -> list:
        return await asyncio.gather(
            *[_[0](*_[1:]) for _ in batch], return_exceptions=True
        )


def run_worker(qin, qout):
    SubinterpreterAsyncWorker(qin, qout)
    return True


def spawn():
    inter = concurrent.interpreters.create()
    qin = concurrent.interpreters.create_queue()
    qout = concurrent.interpreters.create_queue()
    inter.call_in_thread(run_worker, qin, qout)
    return qin, qout
