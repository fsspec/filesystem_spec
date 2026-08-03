from __future__ import annotations

import asyncio
import logging
import weakref
from collections import deque

import fsspec.asyn

logger = logging.getLogger(__name__)

# Kept for compatibility with existing tests/consumers.
HAS_CPYTHON_API = False


def _fast_slice(src_bytes: bytes, offset: int, read_size: int) -> bytes:
    if read_size == 0:
        return b""
    if offset < 0 or offset + read_size > len(src_bytes):
        raise ValueError("Slice indices out of bounds")
    return src_bytes[offset : offset + read_size]


class RunningAverageTracker:
    """Tracks a running average of values over a sliding window."""

    def __init__(self, maxlen: int = 10):
        self._history: deque[int] = deque(maxlen=maxlen)
        self._sum = 0

    def add(self, value: int) -> None:
        if value <= 0:
            raise ValueError(
                "Internal error, RunningAverageTracker tried inserting negative value"
            )
        if len(self._history) == self._history.maxlen:
            self._sum -= self._history[0]

        self._history.append(value)
        self._sum += value

    @property
    def average(self) -> int:
        count = len(self._history)
        if count == 0:
            return 1024 * 1024
        return self._sum // count

    @property
    def is_variable(self) -> bool:
        count = len(self._history)
        if count < 2:
            return False
        return len(set(self._history)) > 1

    @property
    def last_value(self) -> int:
        if not self._history:
            raise RuntimeError("No entry found in history")
        return self._history[-1]

    def clear(self) -> None:
        self._history.clear()
        self._sum = 0


class PrefetchProducer:
    """Background worker that fetches sequential blocks of data."""

    MIN_CHUNK_SIZE = 5 * 1024 * 1024
    MIN_PREFETCH_SIZE = 128 * 1024 * 1024
    MIN_STREAKS_FOR_PREFETCHING = 3
    VARIABLE_IO_THRESHOLD = 64 * 1024 * 1024

    def __init__(
        self,
        fetcher,
        size: int,
        concurrency: int,
        queue: asyncio.Queue,
        wakeup_event: asyncio.Event,
        consumer: "PrefetchConsumer",
        tracker: RunningAverageTracker,
        orchestrator: "BackgroundPrefetcher",
        user_max_prefetch_size=None,
    ):
        self.fetcher = fetcher
        self.size = size
        self.concurrency = concurrency
        self.queue = queue
        self.wakeup_event = wakeup_event
        self.consumer = consumer
        self.tracker = tracker
        self.orchestrator = weakref.proxy(orchestrator)
        self._user_max_prefetch_size = user_max_prefetch_size

        self.current_offset = 0
        self.is_stopped = False
        self._active_tasks = set()
        self._producer_task = None

    @property
    def max_prefetch_size(self) -> int:
        if self._user_max_prefetch_size is not None:
            return min(
                self._user_max_prefetch_size,
                max(2 * self.tracker.average, self.MIN_PREFETCH_SIZE),
            )
        return max(2 * self.tracker.average, self.MIN_PREFETCH_SIZE)

    def start(self) -> None:
        self.is_stopped = False
        self.wakeup_event.clear()
        self._producer_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self.is_stopped = True
        self.wakeup_event.set()

        tasks_to_wait = []
        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()
            tasks_to_wait.append(self._producer_task)

        for task in list(self._active_tasks):
            if not task.done():
                tasks_to_wait.append(task)

        self._active_tasks.clear()

        while not self.queue.empty():
            try:
                item = self.queue.get_nowait()
                if (
                    isinstance(item, asyncio.Task)
                    and item.done()
                    and not item.cancelled()
                ):
                    item.exception()
            except asyncio.QueueEmpty:
                break

        if tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)

        self.wakeup_event.clear()

    async def restart(self, new_offset: int) -> None:
        await self.stop()
        self.current_offset = new_offset
        self.start()

    async def _loop(self) -> None:
        try:
            while not self.is_stopped:
                await self.wakeup_event.wait()
                self.wakeup_event.clear()

                if self.is_stopped:
                    break

                await self._process_prefetch_cycle()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.is_stopped = True
            self.orchestrator.set_error(e)
            await self.queue.put(e)

    def _calculate_prefetch_params(self) -> tuple[int, int, int]:
        avg_io_size = self.tracker.average
        streak = self.consumer.sequential_streak
        is_variable = self.tracker.is_variable
        last_read_size = self.tracker.last_value

        exceeds_user_max = (
            self._user_max_prefetch_size is not None
            and avg_io_size > self._user_max_prefetch_size
        )

        if (
            is_variable and avg_io_size > self.VARIABLE_IO_THRESHOLD
        ) or exceeds_user_max:
            prefetch_multiplier = 1
        elif streak < self.MIN_STREAKS_FOR_PREFETCHING:
            prefetch_multiplier = 1
        else:
            prefetch_multiplier = streak - self.MIN_STREAKS_FOR_PREFETCHING + 1

        if self.queue.empty() or prefetch_multiplier == 1:
            io_size = last_read_size
        else:
            io_size = avg_io_size

        prefetch_size = min(prefetch_multiplier * io_size, self.max_prefetch_size)
        if self.consumer.offset + prefetch_size < self.consumer.target_offset:
            prefetch_size = self.consumer.target_offset - self.consumer.offset

        if is_variable:
            effective_prefetch_size = prefetch_size
        else:
            effective_prefetch_size = (prefetch_size // io_size) * io_size
            if effective_prefetch_size == 0:
                effective_prefetch_size = prefetch_size

        return prefetch_size, io_size, effective_prefetch_size

    async def _process_prefetch_cycle(self) -> None:
        prefetch_size, io_size, effective_prefetch_size = (
            self._calculate_prefetch_params()
        )

        while (
            not self.is_stopped
            and (self.current_offset - self.consumer.offset) < prefetch_size
            and self.current_offset < self.size
        ):
            user_offset = self.consumer.offset
            space_remaining = self.size - self.current_offset
            prefetch_space_available = prefetch_size - (
                self.current_offset - user_offset
            )

            if prefetch_size >= self.MIN_CHUNK_SIZE:
                if prefetch_space_available >= self.MIN_CHUNK_SIZE:
                    actual_size = min(max(self.MIN_CHUNK_SIZE, io_size), space_remaining)
                else:
                    break
            else:
                actual_size = min(io_size, space_remaining)

            if prefetch_space_available < actual_size:
                if self.tracker.is_variable or prefetch_space_available == prefetch_size:
                    actual_size = prefetch_space_available
                else:
                    break

            streak = self.consumer.sequential_streak
            if streak < self.MIN_STREAKS_FOR_PREFETCHING:
                sfactor = self.concurrency
            else:
                sfactor = min(
                    self.concurrency,
                    max(1, actual_size * self.concurrency // effective_prefetch_size),
                )

            download_task = asyncio.create_task(
                self.fetcher(self.current_offset, actual_size, split_factor=sfactor)
            )
            self._active_tasks.add(download_task)
            download_task.add_done_callback(self._active_tasks.discard)

            await self.queue.put(download_task)
            self.current_offset += actual_size

        if self.current_offset >= self.size:
            self.is_stopped = True


class PrefetchConsumer:
    """Consumes prefetched chunks and manages byte slicing."""

    def __init__(
        self,
        queue: asyncio.Queue,
        wakeup_event: asyncio.Event,
        tracker: RunningAverageTracker,
        orchestrator: "BackgroundPrefetcher",
    ):
        self.queue = queue
        self.wakeup_event = wakeup_event
        self.tracker = tracker
        self.orchestrator = weakref.proxy(orchestrator)
        self.sequential_streak = 0
        self.offset = 0
        self.target_offset = 0
        self._current_block = b""
        self._current_block_idx = 0

    def seek(self, new_offset: int) -> None:
        self.offset = new_offset
        self.target_offset = new_offset
        self.sequential_streak = 0
        self._current_block = b""
        self._current_block_idx = 0

    def clear_buffer(self) -> None:
        self._current_block = b""
        self._current_block_idx = 0

    async def _advance(self, size: int, save_data: bool) -> list[bytes]:
        if size <= 0:
            return []

        chunks = []
        processed = 0
        self.target_offset = self.offset + size

        while processed < size:
            available = len(self._current_block) - self._current_block_idx
            trigger_wakeup = False

            if not available:
                is_producer_stopped = (
                    self.orchestrator.producer is None
                    or self.orchestrator.producer.is_stopped
                )
                if is_producer_stopped and self.queue.empty():
                    break

                if self.queue.empty():
                    self.wakeup_event.set()

                task = await self.queue.get()

                if isinstance(task, Exception):
                    self.orchestrator.set_error(task)
                    raise task

                try:
                    block = await task
                    self.sequential_streak += 1
                    if self.sequential_streak >= PrefetchProducer.MIN_STREAKS_FOR_PREFETCHING:
                        exceeds_user_max = (
                            self.orchestrator.max_prefetch_size is not None
                            and self.tracker.average > self.orchestrator.max_prefetch_size
                        )
                        is_massive_variable = (
                            self.tracker.is_variable
                            and self.tracker.average > PrefetchProducer.VARIABLE_IO_THRESHOLD
                        )
                        if not (is_massive_variable or exceeds_user_max):
                            trigger_wakeup = True

                    self._current_block = block
                    self._current_block_idx = 0
                    available = len(self._current_block)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.orchestrator.set_error(e)
                    raise e

            if not self._current_block:
                break

            needed = size - processed
            take = min(needed, available)

            if save_data:
                if take == len(self._current_block) and self._current_block_idx == 0:
                    chunk = self._current_block
                else:
                    chunk = await asyncio.to_thread(
                        _fast_slice,
                        self._current_block,
                        self._current_block_idx,
                        take,
                    )
                chunks.append(chunk)

            self._current_block_idx += take
            processed += take
            self.offset += take
            if trigger_wakeup:
                self.wakeup_event.set()

        return chunks

    async def consume(self, size: int) -> bytes:
        if size <= 0:
            return b""

        chunks = await self._advance(size, save_data=True)

        if not chunks:
            return b""

        if len(chunks) == 1:
            return chunks[0]

        return await asyncio.to_thread(b"".join, chunks)

    async def skip(self, size: int) -> None:
        await self._advance(size, save_data=False)


class BackgroundPrefetcher:
    """Coordinates read behavior and background prefetch work."""

    producer = None

    def __init__(
        self,
        fetcher,
        size: int,
        concurrency: int,
        max_prefetch_size=None,
        loop=None,
    ):
        self.size = size
        self.concurrency = concurrency
        self.max_prefetch_size = max_prefetch_size

        if max_prefetch_size is not None and max_prefetch_size <= 0:
            raise ValueError(
                "max_prefetch_size should be a positive integer to use adaptive prefetching!"
            )

        self.loop = loop
        self._error = None
        self.is_stopped = False
        self.user_offset = 0
        self.read_tracker = RunningAverageTracker(maxlen=10)

        self.queue = None
        self.wakeup_event = None
        self._async_lock = None
        self.consumer = None
        self.producer = None

        def _start():
            self.queue = asyncio.Queue()
            self.wakeup_event = asyncio.Event()
            self._async_lock = asyncio.Lock()

            self.consumer = PrefetchConsumer(
                queue=self.queue,
                wakeup_event=self.wakeup_event,
                tracker=self.read_tracker,
                orchestrator=self,
            )

            self.producer = PrefetchProducer(
                fetcher=fetcher,
                size=self.size,
                concurrency=self.concurrency,
                queue=self.queue,
                wakeup_event=self.wakeup_event,
                consumer=self.consumer,
                tracker=self.read_tracker,
                orchestrator=self,
                user_max_prefetch_size=max_prefetch_size,
            )
            self.producer.start()

        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is self.loop and self.loop is not None:
            _start()
        elif self.loop is not None:

            async def _start_wrapper():
                _start()

            fsspec.asyn.sync(self.loop, _start_wrapper)
        elif current_loop is not None:
            self.loop = current_loop
            _start()
        else:
            raise RuntimeError("No event loop found")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    def set_error(self, e: Exception) -> None:
        self._error = e

    async def _restart_producer(self, new_offset: int) -> None:
        self._error = None
        await self.producer.restart(new_offset)
        self.consumer.seek(new_offset)
        self.read_tracker.clear()

    async def _async_fetch(self, start: int, end: int) -> bytes:
        async with self._async_lock:
            try:
                if self.is_stopped:
                    raise RuntimeError("The file instance has been closed.")

                if self._error:
                    self.user_offset = start
                    await self._restart_producer(start)
                elif start != self.user_offset:
                    block_offset = self.consumer.offset - self.consumer._current_block_idx
                    if self.user_offset < start <= self.producer.current_offset:
                        skip_amount = start - self.user_offset
                        await self.consumer.skip(skip_amount)
                        self.user_offset = start
                    elif block_offset <= start < self.consumer.offset:
                        self.consumer._current_block_idx = start - block_offset
                        self.consumer.offset = start
                        self.consumer.target_offset = start
                        self.user_offset = start
                    else:
                        self.user_offset = start
                        await self._restart_producer(start)

                requested_size = end - start
                self.read_tracker.add(requested_size)

                chunk = await self.consumer.consume(requested_size)
                self.user_offset += len(chunk)
                return chunk
            except asyncio.CancelledError as e:
                self._error = e
                raise
            except Exception as e:
                self._error = e
                if self.producer and not self.producer.is_stopped:
                    await self.producer.stop()
                raise

    async def _async_close(self) -> None:
        async with self._async_lock:
            if self.is_stopped:
                return

            self.is_stopped = True
            if self.producer:
                await self.producer.stop()

            self.consumer.clear_buffer()

    async def afetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None:
            end = self.size

        end = min(end, self.size)

        if start >= self.size or start >= end:
            return b""

        if self.is_stopped:
            raise RuntimeError(
                "The file instance has been closed. This can occur if a close operation "
                "is executed concurrently while a read operation is still in progress."
            )

        return await self._async_fetch(start, end)

    def fetch(self, start: int | None, end: int | None) -> bytes:
        return fsspec.asyn.sync(self.loop, self.afetch, start, end)

    async def aclose(self) -> None:
        await self._async_close()

    def close(self) -> None:
        fsspec.asyn.sync(self.loop, self._async_close)
