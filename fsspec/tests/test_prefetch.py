import asyncio
from unittest import mock

import fsspec.asyn
import pytest

from fsspec.prefetch import BackgroundPrefetcher, RunningAverageTracker, _fast_slice


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
    def __init__(self, data, fail_at_call=None):
        self.data = data
        self.calls = []
        self.fail_at_call = fail_at_call
        self.call_count = 0

    async def __call__(self, start, size, split_factor=1):
        self.call_count += 1
        self.calls.append({"start": start, "size": size, "split_factor": split_factor})
        await asyncio.sleep(0.001)

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
    assert tracker.average == 1024 * 1024

    tracker.add(512)
    tracker.add(512)
    assert tracker.average == 512

    tracker.add(2048)
    assert tracker.average == 1024


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


def test_producer_error_propagation_and_recovery(prefetcher_factory):
    fetcher = MockFetcher(b"A" * 2000, fail_at_call=3)
    bp = prefetcher_factory(fetcher=fetcher, size=2000, concurrency=4)

    for i in range(2):
        bp.fetch(i * 100, (i + 1) * 100)

    with pytest.raises(OSError, match="Simulated Network Timeout"):
        bp.fetch(400, 500)

    assert isinstance(bp._error, OSError)

    fetcher.fail_at_call = None
    data = bp.fetch(400, 500)
    assert data == b"A" * 100
    assert bp._error is None


def test_read_after_close(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)
    bp.close()

    assert bp.is_stopped is True
    with pytest.raises(RuntimeError, match="The file instance has been closed"):
        bp.fetch(0, 10)


def test_init_invalid_max_prefetch_size():
    with pytest.raises(ValueError, match=r"max_prefetch_size should be a positive integer"):
        BackgroundPrefetcher(
            fetcher=MockFetcher(b""),
            size=1000,
            concurrency=4,
            max_prefetch_size=0,
            loop=fsspec.asyn.get_loop(),
        )


def test_fast_slice_pypy_fallback():
    src = b"0123456789_pypy_fallback_test"
    with mock.patch("fsspec.prefetch.HAS_CPYTHON_API", False):
        assert _fast_slice(src, 11, 13) == b"pypy_fallback"
        assert _fast_slice(src, 5, 0) == b""
        assert _fast_slice(src, 0, len(src)) == src
