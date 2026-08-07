import asyncio
from unittest import mock

import fsspec.asyn
import pytest

from fsspec.prefetcher import BackgroundPrefetcher, RunningAverageTracker, _fast_slice


@pytest.fixture
def prefetcher_factory():
    prefetchers = []

    def _make_prefetcher(**kwargs):
        if "loop" not in kwargs:
            kwargs["loop"] = fsspec.asyn.get_loop()

        bp = BackgroundPrefetcher(**kwargs)
        prefetchers.append(bp)
        return bp

    yield _make_prefetcher

    for bp in prefetchers:
        bp.is_stopped = False
        bp.close()


class MockFetcher:
    def __init__(self, data, fail_at_call=None, hang_at_call=None):
        self.data = data
        self.calls = []
        self.fail_at_call = fail_at_call
        self.hang_at_call = hang_at_call
        self.call_count = 0

    async def __call__(self, start, size, split_factor=1):
        self.call_count += 1
        self.calls.append({"start": start, "size": size, "split_factor": split_factor})

        await asyncio.sleep(0.001)

        if self.hang_at_call is not None and self.call_count >= self.hang_at_call:
            await asyncio.sleep(1000)

        if self.fail_at_call is not None and self.call_count >= self.fail_at_call:
            raise OSError("Simulated Network Timeout")

        return self.data[start : start + size]


def test_fast_slice_direct():
    src = b"0123456789"
    assert _fast_slice(src, 2, 4) == b"2345"
    assert _fast_slice(src, 5, 0) == b""
    assert _fast_slice(src, 0, 10) == b"0123456789"


def test_running_average_tracker():
    tracker = RunningAverageTracker(maxlen=3)
    assert tracker.average == 1024 * 1024  # Default 1MB fallback

    tracker.add(512)
    tracker.add(512)
    assert tracker.average == 512

    tracker.add(2048)
    assert tracker.average == 1024  # (512 + 512 + 2048) // 3

    tracker.clear()
    assert tracker.average == 1024 * 1024


def test_max_prefetch_size_property(prefetcher_factory):
    bp1 = prefetcher_factory(fetcher=MockFetcher(b""), size=10000, concurrency=4)
    assert bp1.producer.max_prefetch_size == bp1.producer.MIN_PREFETCH_SIZE

    bp2 = prefetcher_factory(fetcher=MockFetcher(b""), size=1000000000, concurrency=4)
    # Give it a history so it calculates 2x the io_size
    bp2.read_tracker.add(100 * 1024 * 1024)
    assert bp2.producer.max_prefetch_size == 200 * 1024 * 1024


def test_sequential_read_spanning_blocks(prefetcher_factory):
    data = b"A" * 100 + b"B" * 100 + b"C" * 100
    fetcher = MockFetcher(data)
    bp = prefetcher_factory(fetcher=fetcher, size=300, concurrency=4)
    bp.read_tracker.add(100)  # Seed the adaptive tracker

    assert bp.fetch(0, 100) == b"A" * 100
    assert bp.fetch(100, 150) == b"B" * 50
    assert bp.consumer._current_block_idx == 50
    assert bp.fetch(150, 250) == b"B" * 50 + b"C" * 50
    assert bp.fetch(250, 300) == b"C" * 50
    assert bp.fetch(300, 310) == b""


def test_fetch_default_args_and_out_of_bounds(prefetcher_factory):
    fetcher = MockFetcher(b"12345")
    bp = prefetcher_factory(fetcher=fetcher, size=5, concurrency=4)

    assert bp.fetch(None, None) == b"12345"
    assert bp.fetch(None, 2) == b"12"
    assert bp.fetch(5, 10) == b""
    assert bp.fetch(10, 20) == b""
    assert bp.fetch(2, 2) == b""
    assert bp.fetch(4, 2) == b""


def test_seek_logic(prefetcher_factory):
    data = b"0123456789" * 10
    fetcher = MockFetcher(data)
    bp = prefetcher_factory(fetcher=fetcher, size=100, concurrency=4)

    assert bp.fetch(0, 10) == data[0:10]
    assert bp.fetch(10, 20) == data[10:20]
    assert bp.user_offset == 20
    assert bp.fetch(50, 60) == data[50:60]
    assert bp.user_offset == 60
    assert bp.fetch(10, 20) == data[10:20]
    assert bp.user_offset == 20


def test_exception_placed_in_queue(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    async def inject_error():
        await bp.queue.put(ValueError("Injected Producer Error"))

    fsspec.asyn.sync(bp.loop, inject_error)

    with pytest.raises(ValueError, match="Injected Producer Error"):
        bp.fetch(0, 50)

    assert isinstance(bp._error, ValueError)


def test_producer_concurrency_streak_and_min_chunk(prefetcher_factory):
    data = b"X" * 1000
    fetcher = MockFetcher(data)

    bp = prefetcher_factory(fetcher=fetcher, size=1000, concurrency=4)
    bp.read_tracker.add(50)

    # Temporarily lower chunk limit for test
    original_min_chunk = bp.producer.MIN_CHUNK_SIZE
    bp.producer.MIN_CHUNK_SIZE = 10

    # Do 6 reads to push the streak well past the MIN_STREAKS threshold
    # Update these values as BackgroundPrefetcher constant changes.
    target_streak = bp.producer.MIN_STREAKS_FOR_PREFETCHING + 3
    for i in range(target_streak):
        bp.fetch(i * 50, (i + 1) * 50)

    fsspec.asyn.sync(bp.loop, asyncio.sleep, 0.1)

    split_factors = [call["split_factor"] for call in fetcher.calls]
    assert split_factors[0] == 4
    assert max(split_factors) > 1
    assert max(split_factors) <= 4

    bp.producer.MIN_CHUNK_SIZE = original_min_chunk


def test_producer_loop_space_constraints(prefetcher_factory):
    data = b"Y" * 100
    fetcher = MockFetcher(data)

    bp = prefetcher_factory(fetcher=fetcher, size=100, concurrency=4)
    bp.read_tracker.add(60)

    original_min_chunk = bp.producer.MIN_CHUNK_SIZE
    bp.producer.MIN_CHUNK_SIZE = 200

    assert bp.fetch(0, 10) == b"Y" * 10

    fsspec.asyn.sync(bp.loop, asyncio.sleep, 0.1)
    sizes = [call["size"] for call in fetcher.calls]
    assert all(s <= 100 for s in sizes)

    bp.producer.MIN_CHUNK_SIZE = original_min_chunk


def test_producer_error_propagation_and_recovery(prefetcher_factory):
    fetcher = MockFetcher(b"A" * 2000, fail_at_call=3)
    bp = prefetcher_factory(fetcher=fetcher, size=2000, concurrency=4)

    for i in range(2):
        bp.fetch(i * 100, (i + 1) * 100)

    # 3rd read triggers the network timeout
    with pytest.raises(OSError, match="Simulated Network Timeout"):
        bp.fetch(400, 500)

    # The prefetcher is now in an error state
    assert isinstance(bp._error, OSError)

    # Disable the mock failure so it can succeed on retry
    fetcher.fail_at_call = None

    # The next fetch should seamlessly recover, wiping the error and returning data
    data = bp.fetch(400, 500)
    assert data == b"A" * 100
    assert bp._error is None


def test_read_after_close(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)
    bp.close()

    assert bp.is_stopped is True
    with pytest.raises(RuntimeError, match="The file instance has been closed"):
        bp.fetch(0, 10)


def test_read_recovers_after_error(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    # Simulate an error state from a previous failed read
    bp._error = ValueError("Pre-existing error")

    # The new error-recovery logic allows a subsequent read to clear the error and succeed
    assert bp.fetch(0, 10) == b"X" * 10
    assert bp._error is None


def test_empty_queue_when_stopped(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 500), size=500, concurrency=4)
    bp.is_stopped = True

    with pytest.raises(RuntimeError, match="The file instance has been closed"):
        bp.fetch(0, 100)


def test_cancel_all_tasks_cleans_queue_with_exceptions(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    async def inject_task():
        async def dummy_exception_task():
            raise ValueError("Hidden error")

        task = asyncio.create_task(dummy_exception_task())
        await bp.queue.put(task)
        await asyncio.sleep(0.05)

    fsspec.asyn.sync(bp.loop, inject_task)
    bp.close()
    assert bp.queue.empty()


def test_cleanup_cancels_active_tasks(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"Z" * 1000), size=1000, concurrency=4)

    async def inject_task():
        async def dummy_task():
            await asyncio.sleep(3)

        task = asyncio.create_task(dummy_task())
        bp.producer._active_tasks.add(task)

    fsspec.asyn.sync(bp.loop, inject_task)

    assert len(bp.producer._active_tasks) > 0
    assert bp.is_stopped is False

    bp.close()

    assert bp.is_stopped is True
    assert len(bp.producer._active_tasks) == 0


def test_read_task_cancellation(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 1000), size=1000, concurrency=4)

    async def inject_and_read():
        bp.is_stopped = True
        while not bp.queue.empty():
            bp.queue.get_nowait()

        cancel_task = asyncio.create_task(asyncio.sleep(10))
        cancel_task.cancel()
        await bp.queue.put(cancel_task)

        with pytest.raises(asyncio.CancelledError):
            await bp.consumer.consume(10)

    fsspec.asyn.sync(bp.loop, inject_and_read)


def test_async_fetch_exception_trapping(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    async def bad_consume(*args, **kwargs):
        raise RuntimeError("Simulated async crash")

    bp.consumer.consume = bad_consume

    with pytest.raises(RuntimeError, match="Simulated async crash"):
        bp.fetch(0, 10)

    # Orchestrator should capture the error internally and halt producer processing correctly
    assert isinstance(bp._error, RuntimeError)
    assert bp.producer.is_stopped is True


def test_read_past_eof_internal(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 50), size=50, concurrency=4)
    bp.user_offset = 50
    res = bp.fetch(50, 60)
    assert res == b""


def test_fetch_with_exact_block_matches(prefetcher_factory):
    data = b"X" * 100
    bp = prefetcher_factory(fetcher=MockFetcher(data), size=100, concurrency=4)
    bp.read_tracker.add(50)

    assert bp.fetch(0, 50) == b"X" * 50
    assert bp.consumer._current_block_idx == 50
    assert bp.fetch(50, 100) == b"X" * 50


def test_queue_empty_race_condition(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    async def inject():
        bp.queue.put_nowait(asyncio.create_task(asyncio.sleep(0)))
        with mock.patch.object(bp.queue, "get_nowait", side_effect=asyncio.QueueEmpty):
            await bp.producer.stop()

    fsspec.asyn.sync(bp.loop, inject)


def test_producer_space_remaining_break(prefetcher_factory):
    bp = prefetcher_factory(
        fetcher=MockFetcher(b"X" * 1000),
        size=1000,
        concurrency=4,
        max_prefetch_size=150,
    )
    bp.fetch(0, 10)
    fsspec.asyn.sync(bp.loop, asyncio.sleep, 0.1)


def test_producer_min_chunk_logic(prefetcher_factory):
    bp1 = prefetcher_factory(
        fetcher=MockFetcher(b"X" * 1000),
        size=1000,
        concurrency=4,
        max_prefetch_size=300,
    )
    bp1.producer.MIN_CHUNK_SIZE = 100

    fsspec.asyn.sync(bp1.loop, asyncio.sleep, 0.1)

    bp2 = prefetcher_factory(
        fetcher=MockFetcher(b"X" * 1000),
        size=1000,
        concurrency=4,
        max_prefetch_size=150,
    )
    bp2.producer.MIN_CHUNK_SIZE = 100
    fsspec.asyn.sync(bp2.loop, asyncio.sleep, 0.1)


def test_producer_loop_exception(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"A" * 100), size=100, concurrency=4)
    error_object = ValueError("Producer crash")

    with mock.patch(
        "fsspec.prefetcher.RunningAverageTracker.average", new_callable=mock.PropertyMock
    ) as mocked_avg:
        mocked_avg.side_effect = error_object
        with pytest.raises(ValueError, match="Producer crash"):
            bp.fetch(0, 10)

    assert bp.is_stopped is False
    assert bp._error == error_object


def test_seek_same_offset(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b""), size=100, concurrency=4)
    bp.fetch(0, 10)


def test_read_history_maxlen(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 2000), size=2000, concurrency=4)
    for i in range(12):
        bp.fetch(i * 10, (i + 1) * 10)
    assert len(bp.read_tracker._history) == 10


def test_fast_slice_branch(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 200), size=200, concurrency=4)
    assert bp.fetch(0, 10) == b"X" * 10
    assert bp.fetch(10, 20) == b"X" * 10


def test_async_fetch_not_block_break(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b""), size=100, concurrency=4)

    async def fake_consume(size):
        return b""

    bp.consumer.consume = fake_consume
    bp.user_offset = 0

    res = bp.fetch(0, 50)
    assert res == b""


def test_fetch_stopped_before_execution(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)
    bp.is_stopped = True
    bp._error = None

    with pytest.raises(RuntimeError, match="The file instance has been closed"):
        bp.fetch(0, 10)


def test_async_fetch_zero_copy_remainder(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X"), size=100, concurrency=4)
    bp.consumer._current_block = b"ABCDE"
    bp.consumer._current_block_idx = 0
    bp.user_offset = 0
    res = bp.fetch(0, 5)
    assert res == b"ABCDE"
    assert bp.consumer._current_block_idx == 5


def test_read_runtime_error_on_stopped_empty(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X"), size=100, concurrency=4)
    bp.is_stopped = True
    bp.producer.is_stopped = True

    while not bp.queue.empty():
        bp.queue.get_nowait()

    res = fsspec.asyn.sync(bp.loop, bp.consumer.consume, 10)
    assert res == b""


def test_init_invalid_max_prefetch_size():
    with pytest.raises(
        ValueError,
        match=r"max_prefetch_size should be a positive integer",
    ):
        BackgroundPrefetcher(
            fetcher=MockFetcher(b""), size=1000, concurrency=4, max_prefetch_size=0
        )


def test_init_valid_max_prefetch_size_edge_case(prefetcher_factory):
    bp = prefetcher_factory(
        fetcher=MockFetcher(b""), size=1000, concurrency=4, max_prefetch_size=100
    )
    assert bp.producer._user_max_prefetch_size == 100


def test_consumer_zero_size_checks(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    # 1. Test consume size <= 0
    res_consume_zero = fsspec.asyn.sync(bp.loop, bp.consumer.consume, 0)
    assert res_consume_zero == b""
    res_consume_neg = fsspec.asyn.sync(bp.loop, bp.consumer.consume, -5)
    assert res_consume_neg == b""

    # 2. Test _advance size <= 0 directly
    # (consume catches it early, so we call _advance directly to hit its internal check)
    res_advance_zero = fsspec.asyn.sync(
        bp.loop, bp.consumer._advance, 0, save_data=True
    )
    assert res_advance_zero == []
    res_advance_neg = fsspec.asyn.sync(
        bp.loop, bp.consumer._advance, -10, save_data=False
    )
    assert res_advance_neg == []


def test_producer_min_chunk_inner_break(prefetcher_factory):
    fetcher = MockFetcher(b"X" * 1000)
    bp = prefetcher_factory(
        fetcher=fetcher, size=1000, concurrency=4, max_prefetch_size=400
    )

    bp.read_tracker.add(100)

    original_min_chunk = bp.producer.MIN_CHUNK_SIZE
    bp.producer.MIN_CHUNK_SIZE = 200

    async def trigger_loop():
        bp.producer.current_offset = 250
        bp.consumer.offset = 0
        bp.consumer.target_offset = 0
        # streak=6 makes prefetch_multiplier = 4 (6 - 3 + 1)
        # prefetch_size = 4 * 100 = 400
        bp.consumer.sequential_streak = 6
        bp.wakeup_event.set()
        await asyncio.sleep(0.05)

    fsspec.asyn.sync(bp.loop, trigger_loop)

    assert fetcher.call_count == 0

    bp.producer.MIN_CHUNK_SIZE = original_min_chunk


def test_producer_loop_break_on_stopped_after_wakeup(prefetcher_factory):
    fetcher = MockFetcher(b"X" * 1000)
    bp = prefetcher_factory(fetcher=fetcher, size=1000, concurrency=4)

    async def trigger_stop_and_wake():
        bp.producer.is_stopped = True
        bp.wakeup_event.set()
        await asyncio.sleep(0.05)

    fsspec.asyn.sync(bp.loop, trigger_stop_and_wake)

    # Verify the producer gracefully exited without doing work
    assert fetcher.call_count == 0


def test_massive_read_disables_proactive_prefetching(prefetcher_factory):
    fetcher = MockFetcher(b"X" * 1000)

    # max_prefetch_size = 40
    bp = prefetcher_factory(
        fetcher=fetcher, size=1000, concurrency=4, max_prefetch_size=40
    )

    # Do enough reads to build a sequential streak and trigger large averages
    # Reading 60 bytes at a time. Average = 60. Threshold = 50.
    for i in range(4):
        bp.fetch(i * 60, (i + 1) * 60)

    fsspec.asyn.sync(bp.loop, asyncio.sleep, 0.1)

    # Because average (60) > threshold (40), prefetch_multiplier is pinned to 1.
    # The producer should only fetch what the user specifically read (4 * 60 = 240)
    # and should NOT have pre-fetched any additional data ahead into the queue.
    assert bp.producer.current_offset == 240


def test_normal_read_allows_proactive_prefetching(prefetcher_factory):
    fetcher = MockFetcher(b"X" * 1000)

    # max_prefetch_size = 200 makes dynamic threshold = 100
    bp = prefetcher_factory(
        fetcher=fetcher, size=1000, concurrency=4, max_prefetch_size=200
    )

    # Reading 60 bytes at a time. Average = 60. Threshold = 100.
    for i in range(4):
        bp.fetch(i * 60, (i + 1) * 60)

    fsspec.asyn.sync(bp.loop, asyncio.sleep, 0.1)

    # Because average (60) <= threshold (100), the producer allows prefetching.
    # It calculates a normal prefetch_multiplier > 1 and pre-fetches data ahead.
    assert bp.producer.current_offset > 240


def test_target_offset_expands_prefetch(prefetcher_factory):
    fetcher = MockFetcher(b"X" * 1000)
    bp = prefetcher_factory(fetcher=fetcher, size=1000, concurrency=4)

    # Seed tracker to keep the default `max_prefetch_size` calculation small
    bp.read_tracker.add(10)

    # The consumer requests a massive chunk (500 bytes), far exceeding normal prefetch windows
    bp.fetch(0, 500)

    fsspec.asyn.sync(bp.loop, asyncio.sleep, 0.1)

    # The new target_offset logic should explicitly tell the producer to expand its
    # boundary to cover the requested 500 bytes, overriding the tiny multiplier logic.
    assert bp.consumer.target_offset == 500
    assert bp.producer.current_offset >= 500


def test_producer_min_chunk_inner_empty_queue_shrink(prefetcher_factory):
    fetcher = MockFetcher(b"X" * 1000)
    bp = prefetcher_factory(
        fetcher=fetcher, size=1000, concurrency=4, max_prefetch_size=400
    )

    bp.read_tracker.add(100)

    original_min_chunk = bp.producer.MIN_CHUNK_SIZE
    bp.producer.MIN_CHUNK_SIZE = 200

    async def trigger_loop():
        # Setup conditions where the queue is empty and the user is waiting
        # This makes prefetch_space_available exactly equal to prefetch_size
        bp.producer.current_offset = 0
        bp.consumer.offset = 0
        bp.consumer.target_offset = 0
        bp.consumer.sequential_streak = 6
        bp.wakeup_event.set()
        await asyncio.sleep(0.05)

    fsspec.asyn.sync(bp.loop, trigger_loop)

    # Because space_available == prefetch_size, it triggers the shrink condition
    # instead of breaking, ensuring the blocked consumer gets its data.
    assert fetcher.call_count > 0

    bp.producer.MIN_CHUNK_SIZE = original_min_chunk


def test_async_context_manager_and_afetch(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)

    async def run_async():
        async with bp as ctx:
            res = await ctx.afetch(0, 10)
            assert res == b"X" * 10
            # Test default bounds -> (0, 100). Validates a backwards hard seek internally.
            res_all = await ctx.afetch(None, None)
            assert len(res_all) == 100

        assert bp.is_stopped is True
        assert bp.consumer._current_block == b""  # Buffer cleanly cleared on async exit

    fsspec.asyn.sync(bp.loop, run_async)


def test_init_with_explicit_loop(prefetcher_factory):
    """Verify that passing an explicit loop assigns it correctly."""
    loop = fsspec.asyn.get_loop()
    bp = prefetcher_factory(
        fetcher=MockFetcher(b"X"), size=100, concurrency=1, loop=loop
    )
    assert bp.loop is loop


@pytest.mark.asyncio
async def test_init_with_running_loop():
    """Verify asynchronous=True behavior where it inherits the user's running loop."""
    current = asyncio.get_running_loop()
    # explicitly passing loop=None simulates asynchronous=True behavior
    bp = BackgroundPrefetcher(
        fetcher=MockFetcher(b"X"), size=100, concurrency=1, loop=None
    )
    assert bp.loop is current
    await bp.aclose()


def test_init_within_fsspec_loop(prefetcher_factory):
    """Verify the edge case where the prefetcher is initialized while already running inside the target loop."""
    loop = fsspec.asyn.get_loop()

    async def init_inside_loop():
        bp = BackgroundPrefetcher(
            fetcher=MockFetcher(b"X"), size=100, concurrency=1, loop=loop
        )
        assert bp.loop is loop
        await bp.aclose()

    # Submit the initialization directly onto the background thread
    fsspec.asyn.sync(loop, init_inside_loop)


def test_init_no_loop_raises_error():
    """Verify synchronous execution strictly fails if no explicit loop or active loop is found."""
    with pytest.raises(RuntimeError, match="No event loop found"):
        BackgroundPrefetcher(
            fetcher=MockFetcher(b"X"), size=100, concurrency=1, loop=None
        )


def test_local_seek_optimization(prefetcher_factory):
    data = b"0123456789" * 10
    fetcher = MockFetcher(data)
    bp = prefetcher_factory(fetcher=fetcher, size=100, concurrency=4)

    # First fetch: read first 50 bytes (0-50)
    # This will trigger a fetch. The block size fetched will be 50 bytes (covering [0, 50]).
    assert bp.fetch(0, 50) == data[0:50]
    assert len(bp.consumer._current_block) == 50
    initial_fetch_calls = fetcher.call_count
    assert initial_fetch_calls > 0

    # 1. Perform a backward seek *within* the currently buffered block [0, 50].
    # Seek back to 5 and read 10 bytes (5 to 15).
    # This should be a zero-cost local seek and NOT increment the fetcher call count.
    assert bp.fetch(5, 15) == data[5:15]
    assert fetcher.call_count == initial_fetch_calls
    assert bp.user_offset == 15

    # 2. Perform a forward seek *within* the currently buffered block [0, 50].
    # Seek forward to 30 and read 10 bytes (30 to 40).
    # This should also be resolved locally and NOT increment the fetcher call count.
    assert bp.fetch(30, 40) == data[30:40]
    assert fetcher.call_count == initial_fetch_calls
    assert bp.user_offset == 40

    # 3. Read further to trigger a hard seek and load the next block [60, 100].
    assert bp.fetch(60, 80) == data[60:80]
    calls_after_next = fetcher.call_count
    assert calls_after_next > initial_fetch_calls

    # The currently buffered block in the consumer is now [60, 100].
    # Seek backward to 10 (which is outside [60, 100]).
    # This should trigger a hard seek and increment the fetcher call count.
    assert bp.fetch(10, 20) == data[10:20]
    assert fetcher.call_count > calls_after_next


@mock.patch("fsspec.prefetcher.HAS_CPYTHON_API", False)
def test_fast_slice_pypy_fallback():
    """
    Tests that when HAS_CPYTHON_API is False (e.g., on PyPy), _fast_slice
    correctly falls back to standard Python slicing and respects boundaries.
    """
    src = b"0123456789_pypy_fallback_test"

    # 1. Verify standard extraction works perfectly
    assert _fast_slice(src, 11, 13) == b"pypy_fallback"

    # 2. Verify zero-length read
    assert _fast_slice(src, 5, 0) == b""

    # 3. Verify exact bounds
    assert _fast_slice(src, 0, len(src)) == src