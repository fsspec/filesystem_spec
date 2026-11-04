import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from fsspec.implementations import cache_metadata, cached
from fsspec.implementations.cache_metadata import CacheMetadata
from fsspec.implementations.cached import CachingFileSystem
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem


class RangeFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        return self.fs.data[start:end]


class RangeFileSystem(AbstractFileSystem):
    protocol = "cache-expiry-test"
    cachable = False
    data = b"0123456789abcdef"

    def info(self, path, **kwargs):
        return {"name": path, "size": len(self.data), "type": "file"}

    def _open(self, path, mode="rb", **kwargs):
        return RangeFile(self, path, mode=mode, **kwargs)


@pytest.fixture
def block_cache(tmp_path, monkeypatch):
    now = [0]
    clock = SimpleNamespace(time=lambda: now[0])
    monkeypatch.setattr(cached, "time", clock)
    monkeypatch.setattr(cache_metadata, "time", clock)
    fs = CachingFileSystem(
        fs=RangeFileSystem(),
        cache_storage=str(tmp_path / "cache"),
        cache_check=False,
        skip_instance_cache=True,
    )
    with fs.open("old", block_size=4) as f:
        assert f.read(1) == b"0"
    now[0] = 1000
    with fs.open("live", block_size=4) as f:
        assert f.read(1) == b"0"
    with fs.open("whole", block_size=4) as f:
        assert f.read() == RangeFileSystem.data
    now[0] = 1001
    return fs


@pytest.mark.parametrize(
    "expiry, survivors",
    [
        (2000, {"old", "live", "whole"}),
        (60, {"live", "whole"}),
        (0, set()),
    ],
)
def test_clear_expired_partial_blocks(block_cache, expiry, survivors):
    fs = block_cache
    metadata = fs._metadata
    original = metadata.cached_files[-1].copy()
    blocks = {path: entry["blocks"] for path, entry in original.items()}
    assert blocks["old"] == blocks["live"] == {0}
    assert blocks["whole"] is True

    fs.clear_expired_cache(expiry)
    assert set(metadata.cached_files[-1]) == survivors
    for path, entry in original.items():
        filename = Path(fs.storage[-1]) / entry["fn"]
        assert filename.exists() == (path in survivors)
    for path in survivors:
        assert metadata.cached_files[-1][path]["blocks"] is blocks[path]

    # Saving again must not merge expired entries back from disk.
    fs.save_cache()
    cache_file = Path(fs.storage[-1]) / "cache"
    serialized = json.loads(cache_file.read_text())
    assert set(serialized) == survivors
    for path in survivors:
        if path == "whole":
            assert serialized[path]["blocks"] is True
        else:
            assert serialized[path]["blocks"] == [0]

    loaded = CacheMetadata(fs.storage)
    loaded.load()
    assert set(loaded.cached_files[-1]) == survivors
    for path in survivors:
        assert loaded.cached_files[-1][path]["blocks"] == blocks[path]
        if path != "whole":
            assert isinstance(loaded.cached_files[-1][path]["blocks"], set)
    loaded.save()
    loaded.load()
    assert set(loaded.cached_files[-1]) == survivors


def test_clear_expired_keeps_live_block_set(block_cache):
    fs = block_cache
    with fs.open("live", block_size=4) as stream:
        blocks = stream.cache.blocks
        assert fs._metadata.cached_files[-1]["live"]["blocks"] is blocks
        fs.clear_expired_cache(60)
        assert fs._metadata.cached_files[-1]["live"]["blocks"] is blocks
        assert stream.cache.blocks is blocks

        stream.seek(5)
        assert stream.read(1) == b"5"
        assert blocks == {0, 1}
        fs.save_cache()
        assert fs._metadata.cached_files[-1]["live"]["blocks"] is blocks
        loaded = CacheMetadata(fs.storage)
        loaded.load()
        assert loaded.cached_files[-1]["live"]["blocks"] == {0, 1}
        assert "old" not in loaded.cached_files[-1]


@pytest.mark.parametrize("operation", ["save", "clear_expired"])
def test_metadata_write_failure_preserves_state(block_cache, monkeypatch, operation):
    fs = block_cache
    metadata = fs._metadata
    original = metadata.cached_files[-1].copy()
    blocks = {path: entry["blocks"] for path, entry in original.items()}
    directory = Path(fs.storage[-1])
    cache_file = directory / "cache"
    original_bytes = cache_file.read_bytes()
    original_files = set(directory.iterdir())

    def fail_dump(value, stream):
        stream.write('{"incomplete":')
        raise OSError("simulated metadata write failure")

    with monkeypatch.context() as patch:
        patch.setattr(cache_metadata.json, "dump", fail_dump)
        with pytest.raises(OSError, match="simulated metadata write failure"):
            if operation == "save":
                fs.save_cache()
            else:
                fs.clear_expired_cache(60)

    assert cache_file.read_bytes() == original_bytes
    assert set(directory.iterdir()) == original_files
    assert metadata.cached_files[-1] == original
    for path in original:
        assert metadata.cached_files[-1][path]["blocks"] is blocks[path]

    # A failed atomic write must not prevent a later successful cleanup.
    fs.clear_expired_cache(60)
    loaded = CacheMetadata(fs.storage)
    loaded.load()
    assert set(loaded.cached_files[-1]) == {"live", "whole"}
    assert loaded.cached_files[-1]["live"]["blocks"] == {0}
    assert loaded.cached_files[-1]["whole"]["blocks"] is True


@pytest.mark.parametrize("blocks", [set(), {0, 2}, True])
def test_save_preserves_block_values(tmp_path, blocks):
    metadata = CacheMetadata([str(tmp_path)])
    entry = {"fn": "data", "time": 0, "uid": "uid", "blocks": blocks}
    metadata.cached_files[-1]["file"] = entry
    for _ in range(2):
        metadata.save()
        assert metadata.cached_files[-1]["file"]["blocks"] is blocks
        assert entry["blocks"] is blocks
        loaded = CacheMetadata([str(tmp_path)])
        loaded.load()
        assert loaded.cached_files[-1]["file"]["blocks"] == blocks
        if blocks is True:
            assert loaded.cached_files[-1]["file"]["blocks"] is True
        else:
            assert isinstance(loaded.cached_files[-1]["file"]["blocks"], set)
