from __future__ import annotations

import json
import os
import time

import pytest

import fsspec
from fsspec.caching import MMapCache
from fsspec.implementations.cache_metadata import CacheMetadata
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem


class _BufferedFile(AbstractBufferedFile):
    def _fetch_range(self, start: int, end: int) -> bytes:
        return self.fs.data[self.path][start:end]


class _BufferedMemoryFS(AbstractFileSystem):
    protocol = "bufmem"

    def __init__(self, data: dict[str, bytes], **kwargs):
        super().__init__(**kwargs)
        self.data = data

    def info(self, path: str, **kwargs) -> dict:
        path = self._strip_protocol(path)
        if path not in self.data:
            raise FileNotFoundError(path)
        return {"name": path, "size": len(self.data[path]), "type": "file"}

    def cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs
    ) -> bytes:
        path = self._strip_protocol(path)
        return self.data[path][start:end]

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict | None = None,
        **kwargs,
    ) -> _BufferedFile:
        path = self._strip_protocol(path)
        return _BufferedFile(
            self,
            path,
            mode=mode,
            block_size=block_size or 8,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )


def _symlink_or_skip(
    target: os.PathLike[str] | str, link: os.PathLike[str] | str
) -> None:
    try:
        os.symlink(target, link)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks unavailable: {exc}")


def _write_index(
    storage: os.PathLike[str] | str, entries: dict, name: str = "cache"
) -> None:
    os.makedirs(storage, exist_ok=True)
    with open(os.path.join(storage, name), "w") as f:
        json.dump(entries, f)


@pytest.mark.parametrize(
    "bad_fn_factory",
    [
        lambda victim, storage: str(victim),
        lambda victim, storage: os.path.relpath(victim, storage),
        lambda victim, storage: f"nested/../{os.path.relpath(victim, storage)}",
        lambda victim, storage: "..\\outside",
        lambda victim, storage: "sub\\outside",
        lambda victim, storage: "C:evil",
        lambda victim, storage: "C:\\evil",
        lambda victim, storage: ".",
        lambda victim, storage: "..",
        lambda victim, storage: "cache",
        lambda victim, storage: "nested",
        lambda victim, storage: "bad\x00name",
        lambda victim, storage: "",
        lambda victim, storage: None,
        lambda victim, storage: 123,
        lambda victim, storage: ["a"],
    ],
)
def test_check_file_rejects_uncontained_or_invalid_fn(tmp_path, bad_fn_factory):
    victim = tmp_path / "outside"
    victim.write_bytes(b"secret")
    storage = tmp_path / "cache"
    storage.mkdir()
    (storage / "nested").mkdir()
    (storage / "sub").mkdir()
    (storage / "sub" / "outside").write_bytes(b"secret")
    if os.sep != "\\":
        (storage / "..\\outside").write_bytes(b"secret")
        (storage / "sub\\outside").write_bytes(b"secret")
        (storage / "C:\\evil").write_bytes(b"secret")
        (storage / "C:evil").write_bytes(b"secret")

    bad_fn = bad_fn_factory(victim, storage)
    _write_index(
        storage,
        {
            "/remote": {
                "original": "/remote",
                "fn": bad_fn,
                "blocks": True,
                "time": time.time(),
                "uid": "u1",
            }
        },
    )

    meta = CacheMetadata([str(storage)])
    meta.load()
    assert meta._resolve_cached_file(str(storage), bad_fn) is None
    assert meta.check_file("/remote", None) is False


@pytest.mark.parametrize("kind", ["absolute", "relative", "metadata_name"])
@pytest.mark.parametrize("multi_layer", [False, True])
def test_filecache_ignores_poisoned_fn_and_fetches_remote(tmp_path, kind, multi_layer):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe_file("/remote.txt", b"remote-bytes")

    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"victim-secret")

    ro_dir = tmp_path / "shared_ro"
    rw_dir = tmp_path / "private_rw"
    poisoned_dir = ro_dir if multi_layer else rw_dir
    poisoned_dir.mkdir()
    rw_dir.mkdir(exist_ok=True)

    if kind == "absolute":
        bad_fn = str(victim)
    elif kind == "relative":
        bad_fn = os.path.relpath(victim, poisoned_dir)
    else:
        bad_fn = "cache"

    _write_index(
        poisoned_dir,
        {
            "/remote.txt": {
                "original": "/remote.txt",
                "fn": bad_fn,
                "blocks": True,
                "time": time.time(),
                "uid": mem.ukey("/remote.txt"),
            }
        },
    )

    cache_storage = [str(ro_dir), str(rw_dir)] if multi_layer else str(rw_dir)
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=cache_storage,
        skip_instance_cache=True,
    )

    assert fs.cat("/remote.txt") == b"remote-bytes"
    with fs.open("/remote.txt", "rb") as f:
        assert f.read() == b"remote-bytes"
    assert victim.read_bytes() == b"victim-secret"
    hit = fs._check_file("/remote.txt")
    assert hit
    with open(rw_dir / "cache") as f:
        saved = json.load(f)
    assert saved["/remote.txt"]["fn"] == fs._mapper("/remote.txt")


@pytest.mark.parametrize("kind", ["absolute", "relative"])
def test_blockcache_ignores_poisoned_fn_without_overwriting_victim(tmp_path, kind):
    remote_payload = b"REMOTE_OVERWRITE_DATA!"
    victim_payload = b"VICTIM_ORIGINAL_SECRET"
    assert len(remote_payload) == len(victim_payload)
    mem = _BufferedMemoryFS({"/remote.bin": remote_payload}, skip_instance_cache=True)

    victim = tmp_path / "victim.bin"
    victim.write_bytes(victim_payload)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    bad_fn = str(victim) if kind == "absolute" else os.path.relpath(victim, cache_dir)
    _write_index(
        cache_dir,
        {
            "/remote.bin": {
                "original": "/remote.bin",
                "fn": bad_fn,
                "blocks": [],
                "time": time.time(),
                "uid": mem.ukey("/remote.bin"),
                "size": len(remote_payload),
                "blocksize": 8,
            }
        },
    )

    fs = fsspec.filesystem(
        "blockcache",
        fs=mem,
        cache_storage=str(cache_dir),
        skip_instance_cache=True,
    )
    with fs.open("/remote.bin", "rb", block_size=8) as f:
        assert f.read() == remote_payload

    assert victim.read_bytes() == victim_payload
    assert fs._check_file("/remote.bin")
    with open(cache_dir / "cache") as f:
        saved = json.load(f)
    assert saved["/remote.bin"]["fn"] == fs._mapper("/remote.bin")


@pytest.mark.parametrize(
    "bad_fn",
    ["../victim.bin", "decoy_missing", "other_existing", "same_hash_stale_uid"],
)
def test_blockcache_poisoned_blocks_do_not_corrupt_fresh_read_or_saved_fn(
    tmp_path, bad_fn
):
    remote_payload = b"REMOTE_REAL_PAYLOAD_24B!"
    mem = _BufferedMemoryFS({"/remote.bin": remote_payload}, skip_instance_cache=True)

    victim = tmp_path / "victim.bin"
    victim.write_bytes(b"VICTIM_SECRET_BYTES_24B!")
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "other_existing").write_bytes(b"DECOY_EXISTING_FILE_24B!")

    probe = fsspec.filesystem(
        "blockcache", fs=mem, cache_storage=str(cache_dir), skip_instance_cache=True
    )
    expected_fn = probe._mapper("/remote.bin")
    if bad_fn == "same_hash_stale_uid":
        planted_fn = expected_fn
        (cache_dir / expected_fn).write_bytes(b"STALE_CACHED_CONTENT_24B")
        planted_uid = "stale-uid"
        planted_time = time.time()
        check_files = True
    else:
        planted_fn = bad_fn
        planted_uid = mem.ukey("/remote.bin")
        planted_time = time.time() - 3600 if bad_fn == "other_existing" else time.time()
        check_files = False

    _write_index(
        cache_dir,
        {
            "/remote.bin": {
                "original": "/remote.bin",
                "fn": planted_fn,
                "blocks": [0, 1, 2],
                "time": planted_time,
                "uid": planted_uid,
                "size": len(remote_payload),
                "blocksize": 8,
            }
        },
    )

    fs = fsspec.filesystem(
        "blockcache",
        fs=mem,
        cache_storage=str(cache_dir),
        expiry_time=60,
        check_files=check_files,
        skip_instance_cache=True,
    )
    with fs.open("/remote.bin", "rb", block_size=8) as f:
        assert f.read() == remote_payload

    assert victim.read_bytes() == b"VICTIM_SECRET_BYTES_24B!"
    hit = fs._check_file("/remote.bin")
    assert hit
    assert hit[1] == str(cache_dir / expected_fn)
    with open(cache_dir / "cache") as f:
        saved = json.load(f)
    assert saved["/remote.bin"]["fn"] == expected_fn


def test_save_writable_drops_unresolvable_sibling_and_merges_valid_blocks(tmp_path):
    remote_payload = b"0123456789abcdefABCDEFGH"
    mem = _BufferedMemoryFS({"/remote.bin": remote_payload}, skip_instance_cache=True)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    fs1 = fsspec.filesystem(
        "blockcache", fs=mem, cache_storage=str(cache_dir), skip_instance_cache=True
    )
    with fs1.open("/remote.bin", "rb", block_size=8) as f1:
        assert f1.read(4) == b"0123"

    # Inject a sibling poisoned entry into fs1's in-memory metadata while fs2 reads block 2
    fs1._metadata.cached_files[-1]["/poison_sibling"] = {
        "original": "/poison_sibling",
        "fn": "../outside",
        "blocks": True,
        "time": 1.0,
        "uid": "u-bad",
    }

    fs2 = fsspec.filesystem(
        "blockcache", fs=mem, cache_storage=str(cache_dir), skip_instance_cache=True
    )
    with fs2.open("/remote.bin", "rb", block_size=8) as f2:
        f2.seek(16)
        assert f2.read(4) == b"ABCD"

    fs1.save_cache()
    assert "/poison_sibling" not in fs1._metadata.cached_files[-1]
    assert 0 in fs1._metadata.cached_files[-1]["/remote.bin"]["blocks"]
    assert 2 in fs1._metadata.cached_files[-1]["/remote.bin"]["blocks"]
    with open(cache_dir / "cache") as f:
        saved = json.load(f)
    assert "/poison_sibling" not in saved


@pytest.mark.parametrize(
    "kind",
    [
        "absolute",
        "relative",
        "prefix_escape",
        "drive",
        "metadata_name",
        "symlink",
        "directory",
    ],
)
@pytest.mark.parametrize("keep_live_entry", [False, True])
def test_clear_expired_does_not_delete_outside_or_symlink_targets(
    tmp_path, monkeypatch, kind, keep_live_entry
):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe_file("/live.txt", b"live")
    mem.pipe_file("/poisoned.txt", b"poisoned")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    victim = tmp_path / "cache_victim"
    victim.write_bytes(b"do-not-delete")
    drive_victim = None

    if kind == "absolute":
        bad_fn = str(victim)
    elif kind == "relative":
        bad_fn = os.path.relpath(victim, cache_dir)
    elif kind == "prefix_escape":
        bad_fn = f"../{victim.name}"
    elif kind == "drive":
        if os.sep == "\\":
            monkeypatch.chdir(tmp_path)
            drive = os.path.splitdrive(str(tmp_path))[0] or "C:"
            bad_fn = f"{drive}evil"
            drive_victim = tmp_path / "evil"
        else:
            bad_fn = "C:evil"
            drive_victim = cache_dir / "C:evil"
        drive_victim.write_bytes(b"drive-relative")
    elif kind == "metadata_name":
        bad_fn = "cache"
    elif kind == "directory":
        bad_fn = "nested"
        (cache_dir / "nested").mkdir()
    else:
        bad_fn = "planted_link"
        _symlink_or_skip(victim, cache_dir / bad_fn)

    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(cache_dir),
        expiry_time=60,
        cache_check=0,
        skip_instance_cache=True,
    )
    if keep_live_entry:
        assert fs.cat("/live.txt") == b"live"

    now = time.time()
    entries = {}
    if keep_live_entry:
        with open(cache_dir / "cache") as f:
            entries = json.load(f)
    entries["/poisoned.txt"] = {
        "original": "/poisoned.txt",
        "fn": bad_fn,
        "blocks": True,
        "time": now - 3600,
        "uid": "u-poison",
    }
    _write_index(cache_dir, entries)
    fs.load_cache()

    fs.clear_expired_cache(expiry_time=60)

    assert victim.read_bytes() == b"do-not-delete"
    if drive_victim is not None and (os.sep == "\\" or keep_live_entry):
        assert drive_victim.read_bytes() == b"drive-relative"
    assert "/poisoned.txt" not in fs._metadata.cached_files[-1]
    if keep_live_entry:
        assert (cache_dir / "cache").is_file()
        assert fs._check_file("/live.txt")
        if kind == "symlink":
            assert os.path.islink(cache_dir / "planted_link")
        if kind == "directory":
            assert (cache_dir / "nested").is_dir()
        with open(cache_dir / "cache") as f:
            saved = json.load(f)
        assert "/poisoned.txt" not in saved
        assert "/live.txt" in saved


def test_clear_expired_missing_fn_still_raises_runtime_error(tmp_path):
    cache_dir = tmp_path / "cache"
    _write_index(
        cache_dir,
        {
            "/broken": {
                "original": "/broken",
                "blocks": True,
                "time": time.time() - 3600,
                "uid": "u1",
            }
        },
    )
    meta = CacheMetadata([str(cache_dir)])
    meta.load()
    with pytest.raises(RuntimeError, match="Cache metadata does not contain 'fn' for"):
        meta.clear_expired(60)


@pytest.mark.parametrize(
    "kind",
    [
        "absolute",
        "relative",
        "prefix_escape",
        "drive",
        "metadata_name",
        "symlink",
        "directory",
    ],
)
def test_clear_expired_direct_never_returns_invalid_or_symlink_paths(tmp_path, kind):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    victim = tmp_path / "cache_victim"
    victim.write_bytes(b"keep")

    if kind == "absolute":
        bad_fn = str(victim)
    elif kind == "relative":
        bad_fn = os.path.relpath(victim, cache_dir)
    elif kind == "prefix_escape":
        bad_fn = f"../{victim.name}"
    elif kind == "drive":
        bad_fn = "C:evil"
    elif kind == "metadata_name":
        bad_fn = "cache"
    elif kind == "directory":
        bad_fn = "nested"
        (cache_dir / "nested").mkdir()
    else:
        bad_fn = "planted_link"
        _symlink_or_skip(victim, cache_dir / bad_fn)

    _write_index(
        cache_dir,
        {
            "/poisoned": {
                "original": "/poisoned",
                "fn": bad_fn,
                "blocks": True,
                "time": time.time() - 3600,
                "uid": "u1",
            }
        },
    )
    meta = CacheMetadata([str(cache_dir)])
    meta.load()
    expired_files, empty = meta.clear_expired(60)
    assert expired_files == []
    assert empty is True
    assert meta.cached_files[-1] == {}


def test_pop_from_cache_rejects_traversal_matching_storage_prefix(tmp_path):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe_file("/remote.txt", b"remote")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    victim = tmp_path / "cache_victim"
    victim.write_bytes(b"precious")

    # Joined path ".../cache/../cache_victim" starts with str(cache_dir) (".../cache")!
    _write_index(
        cache_dir,
        {
            "/remote.txt": {
                "original": "/remote.txt",
                "fn": "../cache_victim",
                "blocks": True,
                "time": time.time(),
                "uid": "u1",
            }
        },
    )
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(cache_dir),
        skip_instance_cache=True,
    )

    fs.pop_from_cache("/remote.txt")

    assert victim.read_bytes() == b"precious"
    assert "/remote.txt" not in fs._metadata.cached_files[-1]


@pytest.mark.parametrize("also_in_writable", [False, True])
def test_pop_from_cache_sibling_prefix_readonly_layer_raises_permission_error(
    tmp_path, also_in_writable
):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe_file("/shared.txt", b"ro-layer-data")

    # Note: str(ro_dir) starts with str(rw_dir), which previously bypassed startswith!
    rw_dir = tmp_path / "cache"
    ro_dir = tmp_path / "cache_shared"

    ro_fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(ro_dir),
        skip_instance_cache=True,
    )
    assert ro_fs.cat("/shared.txt") == b"ro-layer-data"
    _, ro_cached_file = ro_fs._check_file("/shared.txt")

    if also_in_writable:
        mem.pipe_file("/shared.txt", b"rw-layer-data")
        rw_fs = fsspec.filesystem(
            "filecache",
            fs=mem,
            cache_storage=str(rw_dir),
            skip_instance_cache=True,
        )
        assert rw_fs.cat("/shared.txt") == b"rw-layer-data"

    layered_fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=[str(ro_dir), str(rw_dir)],
        skip_instance_cache=True,
    )

    with pytest.raises(
        PermissionError,
        match="Can only delete cached file in last, writable cache location",
    ):
        layered_fs.pop_from_cache("/shared.txt")

    assert os.path.isfile(ro_cached_file)
    with open(ro_cached_file, "rb") as f:
        assert f.read() == b"ro-layer-data"


def test_symlink_in_cache_storage_rejected_across_filecache_and_pop(tmp_path):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe_file("/remote.txt", b"remote-fresh")

    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"victim-secret")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    link = cache_dir / "planted"
    _symlink_or_skip(victim, link)

    _write_index(
        cache_dir,
        {
            "/remote.txt": {
                "original": "/remote.txt",
                "fn": "planted",
                "blocks": True,
                "time": time.time(),
                "uid": mem.ukey("/remote.txt"),
            }
        },
    )

    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(cache_dir),
        skip_instance_cache=True,
    )
    assert fs._check_file("/remote.txt") is False

    # pop_from_cache must neither follow nor delete the victim target
    fs.pop_from_cache("/remote.txt")
    assert victim.read_bytes() == b"victim-secret"

    # Re-plant index + symlink and verify cat/open replace locally without touching victim
    _write_index(
        cache_dir,
        {
            "/remote.txt": {
                "original": "/remote.txt",
                "fn": "planted",
                "blocks": True,
                "time": time.time(),
                "uid": mem.ukey("/remote.txt"),
            }
        },
    )
    fs.load_cache()
    assert fs.cat("/remote.txt") == b"remote-fresh"
    assert victim.read_bytes() == b"victim-secret"

    # Even a symlink pointing to another file inside cache_storage is rejected
    inside_other = cache_dir / "other_inside"
    inside_other.write_bytes(b"other-inside-bytes")
    _symlink_or_skip(inside_other, cache_dir / "inside_link")
    _write_index(
        cache_dir,
        {
            "/remote2.txt": {
                "original": "/remote2.txt",
                "fn": "inside_link",
                "blocks": True,
                "time": time.time(),
                "uid": "u2",
            }
        },
    )
    fs.load_cache()
    assert fs._check_file("/remote2.txt") is False


@pytest.mark.parametrize("dangling", [False, True])
def test_mmap_cache_rejects_symlink_location(tmp_path, dangling):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    victim = tmp_path / ("missing_victim.bin" if dangling else "existing_victim.bin")
    if not dangling:
        victim.write_bytes(b"VICTIM_SECRET_16")

    link = cache_dir / "mapped_entry"
    _symlink_or_skip(victim, link)

    with pytest.raises(OSError):
        MMapCache(4, lambda s, e: b"X" * (e - s), 16, location=str(link))

    if dangling:
        assert not victim.exists()
    else:
        assert victim.read_bytes() == b"VICTIM_SECRET_16"


@pytest.mark.parametrize("dangling", [False, True])
@pytest.mark.parametrize("preindexed", [False, True])
def test_blockcache_rejects_planted_symlink_at_mapped_path(
    tmp_path, dangling, preindexed
):
    mem = _BufferedMemoryFS(
        {"/remote.bin": b"REMOTE_BYTES_16!"}, skip_instance_cache=True
    )

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    victim = tmp_path / ("new_target.bin" if dangling else "existing_target.bin")
    if not dangling:
        victim.write_bytes(b"VICTIM_BYTES_16!")

    probe_fs = fsspec.filesystem(
        "blockcache",
        fs=mem,
        cache_storage=str(cache_dir),
        skip_instance_cache=True,
    )
    mapped_name = probe_fs._mapper("/remote.bin")
    _symlink_or_skip(victim, cache_dir / mapped_name)

    if preindexed:
        _write_index(
            cache_dir,
            {
                "/remote.bin": {
                    "original": "/remote.bin",
                    "fn": mapped_name,
                    "blocks": [],
                    "time": time.time(),
                    "uid": mem.ukey("/remote.bin"),
                    "size": 16,
                    "blocksize": 4,
                }
            },
        )

    fs = fsspec.filesystem(
        "blockcache",
        fs=mem,
        cache_storage=str(cache_dir),
        skip_instance_cache=True,
    )
    with pytest.raises(OSError):
        with fs.open("/remote.bin", "rb", block_size=4) as f:
            f.read()

    if dangling:
        assert not victim.exists()
    else:
        assert victim.read_bytes() == b"VICTIM_BYTES_16!"


def test_symlinked_cache_storage_directory_still_works(tmp_path):
    real_cache = tmp_path / "real_cache"
    real_cache.mkdir()
    link_cache = tmp_path / "link_cache"
    _symlink_or_skip(real_cache, link_cache)

    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe_file("/hello.txt", b"hello-world")

    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(link_cache),
        skip_instance_cache=True,
    )
    assert fs.cat("/hello.txt") == b"hello-world"
    assert fs.cat("/hello.txt") == b"hello-world"
    detail = fs._check_file("/hello.txt")
    assert detail
    assert str(link_cache) in detail[1]

    fs.pop_from_cache("/hello.txt")
    assert fs._check_file("/hello.txt") is False
    assert "/hello.txt" not in fs._metadata.cached_files[-1]
    assert "/hello.txt" not in json.loads((link_cache / "cache").read_text())


def test_mmap_cache_rejects_non_regular_file():
    if not os.path.exists(os.devnull):
        pytest.skip("os.devnull unavailable")

    with pytest.raises(OSError, match="Cache location must be a regular file"):
        MMapCache(4, lambda s, e: b"X" * (e - s), 16, location=os.devnull)


def test_metadata_hardlink_alias_rejected(tmp_path):
    storage = tmp_path / "cache"
    _write_index(
        storage,
        {
            "/poison": {
                "original": "/poison",
                "fn": "alias",
                "blocks": True,
                "time": time.time() - 3600,
                "uid": "u1",
            }
        },
    )
    try:
        os.link(storage / "cache", storage / "alias")
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"hardlinks unavailable: {exc}")

    meta = CacheMetadata([str(storage)])
    meta.load()
    assert meta._resolve_cached_file(str(storage), "alias") is None
    assert meta.check_file("/poison", None) is False
    expired_files, empty = meta.clear_expired(60)
    assert expired_files == []
    assert empty is True


@pytest.mark.parametrize(
    "win_bad_fn",
    ["file:stream", "trailing.", "trailing ", "CACHE", "CaChE.", "cache "],
)
def test_windows_fn_rules_reject_ads_trailing_and_casefolded_metadata(
    tmp_path, monkeypatch, win_bad_fn
):
    storage = tmp_path / "cache"
    storage.mkdir()
    meta = CacheMetadata([str(storage)])
    monkeypatch.setattr(os, "name", "nt")
    assert meta._resolve_cached_file(str(storage), win_bad_fn) is None


@pytest.mark.parametrize("dangling", [False, True])
def test_load_rejects_symlinked_metadata_file(tmp_path, dangling):
    storage = tmp_path / "cache"
    storage.mkdir()
    outside_index = tmp_path / "outside.json"
    if not dangling:
        outside_index.write_text("{}")
    _symlink_or_skip(outside_index, storage / "cache")

    meta = CacheMetadata([str(storage)])
    with pytest.raises(OSError):
        meta.load()


@pytest.mark.parametrize(
    "raw_json", ["[1, 2, 3]", '"not-a-dict"', "123", '{"/a": []}', '{"/a": "bad"}']
)
def test_load_rejects_non_dict_json_payloads(tmp_path, raw_json):
    storage = tmp_path / "cache"
    storage.mkdir()
    (storage / "cache").write_text(raw_json)

    meta = CacheMetadata([str(storage)])
    with pytest.raises(ValueError, match="Invalid cache metadata format"):
        meta.load()


def test_pop_file_unresolvable_in_memory_does_not_create_metadata_file(tmp_path):
    storage = tmp_path / "cache"
    storage.mkdir()
    meta = CacheMetadata([str(storage)])
    meta.update_file(
        "/poison",
        {
            "original": "/poison",
            "fn": "../outside",
            "blocks": True,
            "time": 1.0,
            "uid": "u",
        },
    )
    assert meta.pop_file("/poison") is None
    assert "/poison" not in meta.cached_files[-1]
    assert not (storage / "cache").exists()


def test_mmap_cache_create_uses_o_excl_and_handles_file_exists_race(
    tmp_path, monkeypatch
):
    location = tmp_path / "raced_cache"
    real_os_open = os.open
    seen_flags: list[int] = []
    created_in_race = False

    def racing_open(path, flags, mode=0o777):
        nonlocal created_in_race
        if os.fspath(path) == str(location):
            seen_flags.append(flags)
            if (flags & os.O_CREAT) and not created_in_race:
                created_in_race = True
                # Simulate another opener creating and sizing the file just before O_CREAT|O_EXCL
                location.write_bytes(b"\x00" * 15 + b"1")
        return real_os_open(path, flags, mode)

    monkeypatch.setattr(os, "open", racing_open)
    cache = MMapCache(
        4, lambda s, e: b"ABCDEFGHIJKLMNOP"[s:e], 16, location=str(location)
    )
    try:
        assert any((f & os.O_CREAT) and (f & os.O_EXCL) for f in seen_flags)
        assert cache._fetch(0, 4) == b"ABCD"
    finally:
        cache.close()


def test_resolve_cached_file_handles_realpath_escape_and_cross_drive(
    tmp_path, monkeypatch
):
    storage = tmp_path / "cache"
    storage.mkdir()
    entry = storage / "entry"
    entry.write_bytes(b"data")
    outside = tmp_path / "outside"
    outside.write_bytes(b"secret")

    meta = CacheMetadata([str(storage)])
    assert meta._resolve_cached_file(str(storage), "entry") == str(entry)

    # Simulate a non-islink reparse/mount escape resolved by realpath
    real_realpath = os.path.realpath
    monkeypatch.setattr(
        os.path,
        "realpath",
        lambda p: str(outside) if os.fspath(p) == str(entry) else real_realpath(p),
    )
    assert meta._resolve_cached_file(str(storage), "entry") is None

    # Simulate cross-drive ValueError from os.path.commonpath on Windows
    monkeypatch.setattr(
        os.path,
        "commonpath",
        lambda paths: (_ for _ in ()).throw(
            ValueError("Paths don't have the same drive")
        ),
    )
    assert meta._resolve_cached_file(str(storage), "entry") is None
