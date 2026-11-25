import asyncio

import pytest

from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem


class RangeFS(AbstractFileSystem):
    cachable = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.calls = []
        self.store = {"a": b"0123456789abcdef", "b": b"ABCDEFGHIJKLMNOP"}

    def cat_file(self, path, start=None, end=None, **kwargs):
        self.calls.append((path, start, end, kwargs))
        if path not in self.store:
            raise FileNotFoundError(path)
        return self.store[path][start:end]


class AsyncRangeFS(AsyncFileSystem):
    cachable = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.calls = []
        self.store = {"a": b"0123456789abcdef", "b": b"ABCDEFGHIJKLMNOP"}

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        await asyncio.sleep(0)
        self.calls.append((path, start, end, kwargs))
        if path not in self.store:
            raise FileNotFoundError(path)
        return self.store[path][start:end]


@pytest.fixture(params=[RangeFS, AsyncRangeFS])
def fs(request):
    return request.param()


@pytest.mark.parametrize("gap,requests", [(None, 6), (0, 4), (2, 2)])
def test_coalesced_ranges_order_duplicates_and_kwargs(fs, gap, requests):
    paths = ["a", "b", "a", "a", "a", "b"]
    starts = [6, 4, 0, 2, 2, 0]
    ends = [8, 6, 2, 4, 4, 2]
    expected = [fs.store[p][s:e] for p, s, e in zip(paths, starts, ends)]
    assert (
        fs.cat_ranges(paths, starts, ends, max_gap=gap, marker="forwarded") == expected
    )
    assert len(fs.calls) == requests
    assert all(call[3] == {"marker": "forwarded"} for call in fs.calls)


@pytest.mark.parametrize("gap", [0, 3])
def test_coalesced_ranges_preserve_special_bounds(fs, gap):
    starts = [None, -4, 8, 5, 0, 2, 14, 30]
    ends = [3, None, 4, 5, 2, 4, 20, 32]
    expected = [fs.store["a"][s:e] for s, e in zip(starts, ends)]
    assert fs.cat_ranges(["a"] * len(starts), starts, ends, max_gap=gap) == expected
    for start, end in [(None, 3), (-4, None), (8, 4)]:
        assert ("a", start, end, {}) in fs.calls


def test_coalesced_ranges_scalar_and_empty(fs):
    assert fs.cat_ranges(["a", "a", "b"], 1, 4, max_gap=0) == [b"123", b"123", b"BCD"]
    assert len(fs.calls) == 2
    fs.calls.clear()
    assert fs.cat_ranges([], [], [], max_gap=0) == []
    assert fs.calls == []


def test_coalesced_ranges_errors_follow_input_order(fs):
    paths = ["z-missing", "a", "a-missing", "z-missing"]
    result = fs.cat_ranges(paths, [0, 0, 0, 2], [2, 2, 2, 4], max_gap=0)
    assert result[1] == b"01"
    assert all(isinstance(result[i], FileNotFoundError) for i in (0, 2, 3))
    assert result[0] is result[3]
    assert len(fs.calls) == 3
    with pytest.raises(FileNotFoundError, match="z-missing"):
        fs.cat_ranges(paths, [0, 0, 0, 2], [2, 2, 2, 4], max_gap=0, on_error="raise")


@pytest.mark.parametrize("gap", [-1, 0.5, "1"])
def test_coalesced_ranges_invalid_gap(fs, gap):
    with pytest.raises(ValueError, match="max_gap"):
        fs.cat_ranges(["a"], [0], [1], max_gap=gap)
    assert fs.calls == []


@pytest.mark.asyncio
async def test_coalesced_ranges_native_async():
    fs = AsyncRangeFS(asynchronous=True)
    assert await fs._cat_ranges(
        ["a"] * 3, [4, 0, 2], [6, 2, 4], max_gap=0, batch_size=1
    ) == [b"45", b"01", b"23"]
    assert fs.calls == [("a", 0, 6, {})]


@pytest.mark.parametrize("gap", [0, 5, 100])
def test_coalesced_ranges_nested_and_shuffled(fs, gap):
    import random

    rng = random.Random(42)
    starts = [rng.randrange(32) for _ in range(100)] + [0, 1, 2, 16]
    ends = [s + rng.randrange(20) for s in starts[:-4]] + [16, 15, 14, 16]
    paths = [rng.choice(["a", "b"]) for _ in starts]
    expected = [fs.store[p][s:e] for p, s, e in zip(paths, starts, ends)]
    assert fs.cat_ranges(paths, starts, ends, max_gap=gap) == expected


def test_coalesced_ranges_local_files(tmp_path):
    from fsspec.implementations.local import LocalFileSystem

    path = tmp_path / "data.bin"
    data = bytes(range(256)) * 16
    path.write_bytes(data)
    fs = LocalFileSystem()
    paths = [str(path)] * 5
    starts, ends = [1024, 0, 512, -8, 4000], [1536, 512, 1024, None, 5000]
    assert fs.cat_ranges(paths, starts, ends, max_gap=0) == [
        data[s:e] for s, e in zip(starts, ends)
    ]
