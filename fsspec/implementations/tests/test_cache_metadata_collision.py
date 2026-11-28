from __future__ import annotations

import asyncio
import gzip
import json
import os
import time
import unicodedata

import pytest

import fsspec
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.cache_mapper import AbstractCacheMapper, BasenameCacheMapper
from fsspec.implementations.local import make_path_posix
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.spec import AbstractBufferedFile


class _FixedCacheMapper(AbstractCacheMapper):
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping

    def __call__(self, path: str) -> str:
        return self.mapping.get(path, os.path.basename(path))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FixedCacheMapper) and self.mapping == other.mapping

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.mapping.items())))


class _BufferedMemoryFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        return self.fs.store[self.path].getvalue()[start:end]


class _BufferedMemoryFileSystem(MemoryFileSystem):
    protocol = "bufmem"

    def _open(self, path, mode="rb", block_size=None, autocommit=True, **kwargs):
        path = self._strip_protocol(path)
        if "r" not in mode:
            return super()._open(
                path, mode=mode, block_size=block_size, autocommit=autocommit, **kwargs
            )
        if path not in self.store:
            raise FileNotFoundError(path)
        kwargs.pop("size", None)
        size = len(self.store[path].getvalue())
        return _BufferedMemoryFile(
            self, path, mode=mode, block_size=block_size or 16, size=size, **kwargs
        )


class _AsyncMemoryFileSystem(AsyncFileSystemWrapper):
    protocol = "memory"
    _strip_protocol = MemoryFileSystem._strip_protocol


def _make_poisoned_source(tmp_path, poison_name="cache", compress=False):
    source = tmp_path / "source"
    source.mkdir()
    storage = tmp_path / "storage"
    outside = tmp_path / "outside"
    outside.write_bytes(b"sensitive outside content")

    victim = source / "victim"
    victim.write_bytes(b"legitimate victim content")

    victim_key = make_path_posix(str(victim))
    payload = json.dumps(
        {
            victim_key: {
                "original": victim_key,
                "fn": os.path.relpath(outside, storage),
                "blocks": True,
                "time": time.time(),
                "uid": "injected-uid",
            }
        }
    ).encode()
    if compress:
        payload = gzip.compress(payload)

    poison = source / poison_name
    poison.write_bytes(payload)
    return source, storage, outside, victim, poison


def test_filecache_recursive_cat_rejects_metadata_collision(tmp_path):
    source, storage, outside, victim, _ = _make_poisoned_source(tmp_path)
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat(f"{source}/*", recursive=True)

    assert not (storage / "cache").exists()
    assert fs.cat(str(victim)) == b"legitimate victim content"
    fs.clear_expired_cache(0)
    assert outside.read_bytes() == b"sensitive outside content"


@pytest.mark.parametrize(
    "mapper_kw", [{"same_names": True}, {"cache_mapper": BasenameCacheMapper()}]
)
def test_filecache_open_and_range_apis_reject_metadata_collision(tmp_path, mapper_kw):
    source, storage, outside, victim, poison = _make_poisoned_source(tmp_path)
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(storage),
        skip_instance_cache=True,
        **mapper_kw,
    )

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.open(str(poison), "rb")

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat(str(poison))

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat_file(str(poison))

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat_ranges([str(victim), str(poison)], [0, 0], [4, 2], on_error="raise")

    range_res = fs.cat_ranges(
        [str(victim), str(poison)], [0, 0], [10, 2], on_error="return"
    )
    assert range_res[0] == b"legitimate"
    assert isinstance(range_res[1], ValueError)
    assert "cache_metadata_name" in str(range_res[1])

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.hash_name(str(poison))

    # get() delegates get_file to the target FS and must not poison storage/cache.
    dst = tmp_path / "downloaded"
    fs.get(str(source), str(dst), recursive=True)
    assert fs.open(str(victim), "rb").read() == b"legitimate victim content"
    assert outside.read_bytes() == b"sensitive outside content"


def test_filecache_write_and_open_many_preserve_existing_metadata(tmp_path):
    source, storage, outside, victim, poison = _make_poisoned_source(tmp_path)
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )

    # Populate valid metadata first so <storage>/cache exists on disk.
    assert fs.cat(str(victim)) == b"legitimate victim content"
    meta_file = storage / "cache"
    before_meta = json.loads(meta_file.read_text())
    assert make_path_posix(str(victim)) in before_meta

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.open(str(poison), "wb")

    with pytest.raises(ValueError, match="cache_metadata_name"):
        with fsspec.open_files(
            [f"filecache::{victim}", f"filecache::{poison}"],
            mode="rb",
            filecache={
                "cache_storage": str(storage),
                "same_names": True,
                "skip_instance_cache": True,
            },
        ):
            pass

    with pytest.raises(ValueError, match="cache_metadata_name"):
        with fsspec.open_files(
            [f"filecache::{victim}", f"filecache::{poison}"],
            mode="wb",
            filecache={
                "cache_storage": str(storage),
                "same_names": True,
                "skip_instance_cache": True,
            },
        ):
            pass

    assert json.loads(meta_file.read_text()) == before_meta
    assert fs.cat(str(victim)) == b"legitimate victim content"
    assert outside.read_bytes() == b"sensitive outside content"


def test_filecache_compressed_rejects_metadata_collision(tmp_path):
    _, storage, outside, victim, poison = _make_poisoned_source(tmp_path, compress=True)
    victim.write_bytes(gzip.compress(b"legitimate victim content"))

    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(storage),
        same_names=True,
        compression="gzip",
        skip_instance_cache=True,
    )

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat(str(poison))

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.open(str(poison), "rb")

    assert not (storage / "cache").exists()
    assert fs.cat(str(victim)) == b"legitimate victim content"
    assert outside.read_bytes() == b"sensitive outside content"


def test_blockcache_rejects_metadata_collision(tmp_path):
    storage = tmp_path / "storage"
    outside = tmp_path / "outside"
    outside.write_bytes(b"sensitive outside content")

    backend = _BufferedMemoryFileSystem(skip_instance_cache=True)
    backend.pipe("/dir/victim", b"legitimate victim content")
    poison_payload = json.dumps(
        {
            "/dir/victim": {
                "original": "/dir/victim",
                "fn": os.path.relpath(outside, storage),
                "blocks": True,
                "time": time.time(),
                "uid": backend.ukey("/dir/victim"),
            }
        }
    ).encode()
    backend.pipe("/dir/cache", poison_payload)

    fs = fsspec.filesystem(
        "blockcache",
        fs=backend,
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )

    with pytest.raises(ValueError, match="cache_metadata_name"):
        with fs.open("/dir/cache", "rb") as f:
            f.read()

    assert not (storage / "cache").exists()
    with fs.open("/dir/victim", "rb") as f:
        assert f.read() == b"legitimate victim content"
    assert fs.cat(["/dir/victim", "/dir/cache"], on_error="omit") == {
        "/dir/victim": b"legitimate victim content"
    }
    returned = fs.cat(["/dir/victim", "/dir/cache"], on_error="return")
    assert returned["/dir/victim"] == b"legitimate victim content"
    assert isinstance(returned["/dir/cache"], ValueError)
    assert outside.read_bytes() == b"sensitive outside content"


def test_async_cat_file_and_cat_ranges_reject_metadata_collision(tmp_path):
    storage = tmp_path / "storage"
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe({"/dir/victim": b"0123456789", "/dir/cache": b"{}"})

    async def run():
        fs = fsspec.filesystem(
            "filecache",
            fs=_AsyncMemoryFileSystem(fs=mem, asynchronous=False),
            cache_storage=str(storage),
            same_names=True,
            skip_instance_cache=True,
        )

        with pytest.raises(ValueError, match="cache_metadata_name"):
            await fs._cat_file("memory:///dir/cache")

        with pytest.raises(ValueError, match="cache_metadata_name"):
            await fs._cat_ranges(
                ["memory:///dir/victim", "memory:///dir/cache"],
                [0, 0],
                [4, 2],
                on_error="raise",
            )

        assert not (storage / "cache").exists()

        # Default on_error="return" returns ValueError for colliding entry
        # while serving valid sibling ranges.
        res = await fs._cat_ranges(
            ["memory:///dir/victim", "memory:///dir/cache", "memory:///dir/victim"],
            [0, 0, 4],
            [4, 2, 8],
            on_error="return",
        )
        assert res[0] == b"0123"
        assert isinstance(res[1], ValueError)
        assert "cache_metadata_name" in str(res[1])
        assert res[2] == b"4567"

        # Single colliding entry with on_error="return" (empty download batch).
        single = await fs._cat_ranges(
            ["memory:///dir/cache"], [0], [2], on_error="return"
        )
        assert len(single) == 1
        assert isinstance(single[0], ValueError)
        assert "cache_metadata_name" in str(single[0])

    asyncio.run(run())


def test_cat_on_error_omit_and_return_with_collision(tmp_path):
    source, storage, outside, victim, poison = _make_poisoned_source(tmp_path)
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )

    omitted = fs.cat(f"{source}/*", recursive=True, on_error="omit")
    assert omitted == {make_path_posix(str(victim)): b"legitimate victim content"}

    # Reset cache to verify on_error="return" on cold cache as well.
    fs.clear_cache()
    returned = fs.cat(f"{source}/*", recursive=True, on_error="return")
    assert returned[make_path_posix(str(victim))] == b"legitimate victim content"
    err = returned[make_path_posix(str(poison))]
    assert isinstance(err, ValueError)
    assert "cache_metadata_name" in str(err)

    # Non-recursive multi-file glob must also return a dict and preserve errors.
    fs.clear_cache()
    nonrec_omitted = fs.cat(f"{source}/*", on_error="omit")
    assert nonrec_omitted == {
        make_path_posix(str(victim)): b"legitimate victim content"
    }

    fs.clear_cache()
    nonrec_returned = fs.cat(f"{source}/*", on_error="return")
    assert isinstance(nonrec_returned, dict)
    assert nonrec_returned[make_path_posix(str(victim))] == b"legitimate victim content"
    nonrec_err = nonrec_returned[make_path_posix(str(poison))]
    assert isinstance(nonrec_err, ValueError)
    assert "cache_metadata_name" in str(nonrec_err)
    assert outside.read_bytes() == b"sensitive outside content"


def test_custom_cache_metadata_name_allows_cache_and_rejects_configured_name(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    storage = tmp_path / "storage"
    cache_obj = source / "cache"
    cache_obj.write_bytes(b"remote file named cache")
    meta_obj = source / "metadata.json"
    meta_obj.write_bytes(b"{}")

    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(storage),
        same_names=True,
        cache_metadata_name="metadata.json",
        skip_instance_cache=True,
    )

    assert fs.cat(str(cache_obj)) == b"remote file named cache"
    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat(str(meta_obj))


@pytest.mark.parametrize(
    ("metadata_name", "mapped_name"),
    [
        ("cache", "cache"),
        ("cache", "./cache"),
        ("cache", "sub/../cache"),
        ("cache", "cache/nested"),
        ("cache", ""),
        ("cache", "."),
        ("cache", ".."),
        ("meta/cache", "meta"),
        ("meta/cache", "meta/cache"),
        ("meta/cache", "meta/cache/child"),
    ],
)
def test_collision_exact_and_subpath_variants_rejected(
    tmp_path, metadata_name, mapped_name
):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/item", b"payload")
    storage = tmp_path / "sub" / "storage"
    if "/" in metadata_name and not metadata_name.startswith(".."):
        (storage / os.path.dirname(metadata_name)).mkdir(parents=True, exist_ok=True)

    mapper = _FixedCacheMapper({"/remote/item": mapped_name})
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        cache_mapper=mapper,
        cache_metadata_name=metadata_name,
        skip_instance_cache=True,
    )

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat("/remote/item")


def test_anchored_escape_absolute_multilayer_and_symlink_collisions_rejected(tmp_path):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/item", b"payload")
    layer0 = tmp_path / "layer0"
    layer0.mkdir()
    storage = tmp_path / "storage"
    storage.mkdir()

    colliding_mapped_names = [
        f"../{storage.name}/cache",
        str(storage / "cache"),
        "../layer0/cache",
        "../layer0",
    ]
    if os.name != "nt":
        (storage / "self_link").symlink_to(storage, target_is_directory=True)
        colliding_mapped_names.append("self_link/cache")

    for mapped_name in colliding_mapped_names:
        fs = fsspec.filesystem(
            "filecache",
            fs=mem,
            cache_storage=[str(layer0), str(storage)],
            cache_mapper=_FixedCacheMapper({"/remote/item": mapped_name}),
            skip_instance_cache=True,
        )
        with pytest.raises(ValueError, match="cache_metadata_name"):
            fs.cat("/remote/item")
        assert not (storage / "cache").exists()
        assert not (layer0 / "cache").exists()


@pytest.mark.parametrize("remote_name", ["CACHE", "Cache", "cache.", "cache "])
def test_case_sensitive_posix_allows_non_colliding_case_and_dot_names(
    tmp_path, remote_name
):
    probe = tmp_path / ".case_probe_a"
    probe.write_bytes(b"x")
    is_case_insensitive = (tmp_path / ".CASE_PROBE_A").exists()
    probe.unlink()
    if os.name == "nt" or is_case_insensitive:
        pytest.skip("Requires case-sensitive POSIX filesystem")

    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe({f"/dir/{remote_name}": b"upper payload", "/dir/other": b"other payload"})
    storage = tmp_path / "storage"

    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )

    assert fs.cat(f"/dir/{remote_name}") == b"upper payload"
    assert fs.cat("/dir/other") == b"other payload"
    assert (storage / remote_name).read_bytes() == b"upper payload"
    assert not [p.name for p in storage.iterdir() if p.name.startswith(".fsspec_")]
    with open(storage / "cache") as f:
        meta = json.load(f)
    assert f"/dir/{remote_name}" in meta
    # Second lookup when <storage>/cache already exists on disk also succeeds cleanly.
    fs2 = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )
    assert fs2.hash_name(f"/dir/{remote_name}") == remote_name
    assert not [p.name for p in storage.iterdir() if p.name.startswith(".fsspec_")]


@pytest.mark.parametrize(
    ("metadata_name", "mapped_name"),
    [
        ("cache", "CACHE"),
        ("cache", "Cache"),
        ("meta\u00e9", "metae\u0301"),
        ("metadata.json", "METADATA.JSON"),
        ("meta/cache", "META"),
        ("meta/cache", "meta/CACHE"),
        ("../meta", "../META"),
    ],
)
def test_case_insensitive_storage_rejects_case_and_nfd_variants(
    tmp_path, monkeypatch, metadata_name, mapped_name
):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/item", b"payload")
    storage = tmp_path / "sub" / "storage"
    if "/" in metadata_name and not metadata_name.startswith(".."):
        (storage / os.path.dirname(metadata_name)).mkdir(parents=True, exist_ok=True)

    mapper = _FixedCacheMapper({"/remote/item": mapped_name})
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        cache_mapper=mapper,
        cache_metadata_name=metadata_name,
        skip_instance_cache=True,
    )
    orig_lexists = os.path.lexists

    def ci_lexists(p):
        parent, name = os.path.split(os.fspath(p))
        if not name or not os.path.isdir(parent):
            return orig_lexists(p)
        folded = unicodedata.normalize("NFD", name).casefold()
        return any(
            unicodedata.normalize("NFD", entry).casefold() == folded
            for entry in os.listdir(parent)
        )

    monkeypatch.setattr("fsspec.implementations.cached.os.path.lexists", ci_lexists)
    monkeypatch.setattr(
        "fsspec.implementations.cached.os.path.samefile", lambda _a, _b: True
    )

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat("/remote/item")
    assert not [p.name for p in storage.iterdir() if p.name.startswith(".fsspec_")]


def test_is_storage_case_insensitive_probe(tmp_path, monkeypatch):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/CACHE", b"payload")
    orig_lexists = os.path.lexists
    orig_open = open

    storage = tmp_path / "storage"
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )
    with monkeypatch.context() as m:
        m.setattr(
            "fsspec.implementations.cached.os.path.lexists",
            lambda p: True if ".fsspec_case_" in str(p) else orig_lexists(p),
        )
        m.setattr("fsspec.implementations.cached.os.path.samefile", lambda _a, _b: True)
        with pytest.raises(ValueError, match="cache_metadata_name"):
            fs.cat("/remote/CACHE")
    assert not [p.name for p in storage.iterdir() if p.name.startswith(".fsspec_")]

    storage_ro = tmp_path / "storage_ro"
    fs2 = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage_ro),
        same_names=True,
        skip_instance_cache=True,
    )

    def fail_probe(file, mode="r", *args, **kwargs):
        if ".fsspec_case_" in str(file):
            raise PermissionError("read-only")
        return orig_open(file, mode, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr("builtins.open", fail_probe)
        with pytest.raises(ValueError, match="cache_metadata_name"):
            fs2.cat("/remote/CACHE")
    assert not [p.name for p in storage_ro.iterdir() if p.name.startswith(".fsspec_")]

    # Non-existent parent subdir walks up to existing storage dir for probing.
    storage_sub = tmp_path / "storage_sub"
    fs3 = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage_sub),
        cache_mapper=_FixedCacheMapper({"/remote/CACHE": "missing_sub/CACHE"}),
        cache_metadata_name="missing_sub/cache",
        skip_instance_cache=True,
    )
    with monkeypatch.context() as m:
        m.setattr(
            "fsspec.implementations.cached.os.path.lexists",
            lambda p: True if ".fsspec_case_" in str(p) else orig_lexists(p),
        )
        m.setattr("fsspec.implementations.cached.os.path.samefile", lambda _a, _b: True)
        with pytest.raises(ValueError, match="cache_metadata_name"):
            fs3.cat("/remote/CACHE")
    assert not [p.name for p in storage_sub.iterdir() if p.name.startswith(".fsspec_")]

    # OSError in samefile on existing entries also fails closed.
    storage_err = tmp_path / "storage_err"
    fs4 = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage_err),
        same_names=True,
        skip_instance_cache=True,
    )
    (storage_err / "cache").write_text("{}")
    (storage_err / "CACHE").write_bytes(b"existing")

    def broken_samefile(_a, _b):
        raise OSError("dangling symlink")

    with monkeypatch.context() as m:
        m.setattr("fsspec.implementations.cached.os.path.samefile", broken_samefile)
        with pytest.raises(ValueError, match="cache_metadata_name"):
            fs4.cat("/remote/CACHE")


@pytest.mark.parametrize(
    ("metadata_name", "mapped_name"),
    [
        ("cache", "CACHE"),
        ("cache", "cache."),
        ("cache", "cache "),
        ("cache", "cache. "),
        ("cache", "cache:$DATA"),
        ("cache", "sub/../cache"),
        ("cache", "sub\\..\\cache"),
        ("metadata.json", "metadata.json."),
        ("meta/cache", "META "),
        ("meta/cache", "meta\\cache"),
    ],
)
def test_windows_semantics_rejects_case_trailing_and_backslash_variants(
    tmp_path, monkeypatch, metadata_name, mapped_name
):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/item", b"payload")
    storage = tmp_path / "sub" / "storage"
    if "/" in metadata_name and not metadata_name.startswith(".."):
        (storage / os.path.dirname(metadata_name)).mkdir(parents=True, exist_ok=True)

    mapper = _FixedCacheMapper({"/remote/item": mapped_name})
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        cache_mapper=mapper,
        cache_metadata_name=metadata_name,
        skip_instance_cache=True,
    )
    monkeypatch.setattr("fsspec.implementations.cached.os.name", "nt")

    with pytest.raises(ValueError, match="cache_metadata_name"):
        fs.cat("/remote/item")


def test_windows_nfd_variant_on_normalizing_storage_rejected(tmp_path, monkeypatch):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/item", b"payload")
    storage = tmp_path / "sub" / "storage"

    mapper = _FixedCacheMapper({"/remote/item": "metae\u0301"})
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        cache_mapper=mapper,
        cache_metadata_name="meta\u00e9",
        skip_instance_cache=True,
    )
    is_posix_host = os.name != "nt"
    orig_lexists = os.path.lexists
    orig_isdir = os.path.isdir
    orig_remove = os.remove
    orig_open = open

    def to_local(p):
        if isinstance(p, int):
            return p
        s = os.fspath(p)
        return s.replace("\\", "/") if is_posix_host else s

    def ci_lexists(p):
        lp = to_local(p)
        parent, name = os.path.split(lp)
        if not name or not orig_isdir(parent):
            return orig_lexists(lp)
        folded = unicodedata.normalize("NFD", name).casefold()
        return any(
            unicodedata.normalize("NFD", entry).casefold() == folded
            for entry in os.listdir(parent)
        )

    with monkeypatch.context() as m:
        m.setattr("fsspec.implementations.cached.os.name", "nt")
        m.setattr(
            "fsspec.implementations.cached.os.path.isdir",
            lambda p: orig_isdir(to_local(p)),
        )
        m.setattr("fsspec.implementations.cached.os.path.lexists", ci_lexists)
        m.setattr("fsspec.implementations.cached.os.path.samefile", lambda _a, _b: True)
        m.setattr(
            "fsspec.implementations.cached.os.remove",
            lambda p: orig_remove(to_local(p)),
        )
        m.setattr(
            "builtins.open",
            lambda file, *a, **kw: orig_open(to_local(file), *a, **kw),
        )

        with pytest.raises(ValueError, match="cache_metadata_name"):
            fs.cat("/remote/item")
    assert not [p.name for p in storage.iterdir() if p.name.startswith(".fsspec_")]

    # Non-normalizing Windows volume (standard NTFS) keeps distinct NFC/NFD names working.
    storage_ntfs = tmp_path / "sub_ntfs" / "storage"
    fs_ntfs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage_ntfs),
        cache_mapper=mapper,
        cache_metadata_name="meta\u00e9",
        skip_instance_cache=True,
    )
    with monkeypatch.context() as m:
        m.setattr("fsspec.implementations.cached.os.name", "nt")
        m.setattr(
            "fsspec.implementations.cached.os.path.isdir",
            lambda p: orig_isdir(to_local(p)),
        )
        m.setattr(
            "fsspec.implementations.cached.os.path.lexists",
            lambda p: orig_lexists(to_local(p)),
        )
        m.setattr(
            "fsspec.implementations.cached.os.remove",
            lambda p: orig_remove(to_local(p)),
        )
        m.setattr(
            "builtins.open",
            lambda file, *a, **kw: orig_open(to_local(file), *a, **kw),
        )
        assert fs_ntfs.hash_name("/remote/item") == "metae\u0301"
    assert not [p.name for p in storage_ntfs.iterdir() if p.name.startswith(".fsspec_")]


def test_subpath_metadata_allows_sibling_in_same_subdir(tmp_path):
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe("/remote/item", b"sibling payload")
    storage = tmp_path / "storage"
    (storage / "meta").mkdir(parents=True)

    mapper = _FixedCacheMapper({"/remote/item": "meta/other"})
    fs = fsspec.filesystem(
        "filecache",
        fs=mem,
        cache_storage=str(storage),
        cache_mapper=mapper,
        cache_metadata_name="meta/cache",
        skip_instance_cache=True,
    )

    assert fs.cat("/remote/item") == b"sibling payload"
    assert (storage / "meta" / "other").read_bytes() == b"sibling payload"
    assert (storage / "meta" / "cache").is_file()


def test_simplecache_same_names_allows_file_named_cache(tmp_path):
    storage = tmp_path / "storage"
    mem = MemoryFileSystem(skip_instance_cache=True)
    mem.pipe({"/dir/cache": b"cached payload", "/dir/other": b"other payload"})

    fs = fsspec.filesystem(
        "simplecache",
        fs=mem,
        cache_storage=str(storage),
        same_names=True,
        skip_instance_cache=True,
    )

    assert fs.hash_name("/dir/cache") == "cache"
    assert fs.cat("/dir/cache") == b"cached payload"
    with fs.open("/dir/cache", "rb") as f:
        assert f.read() == b"cached payload"
    assert fs.cat_ranges(["/dir/cache", "/dir/other"], [0, 0], [6, 5]) == [
        b"cached",
        b"other",
    ]

    with fs.open("/dir/written_cache", "wb") as f:
        f.write(b"new data")
    assert mem.cat("/dir/written_cache") == b"new data"

    async def run_async():
        afs = fsspec.filesystem(
            "simplecache",
            fs=_AsyncMemoryFileSystem(fs=mem, asynchronous=False),
            cache_storage=str(tmp_path / "async_storage"),
            same_names=True,
            skip_instance_cache=True,
        )
        assert await afs._cat_file("memory:///dir/cache") == b"cached payload"
        assert await afs._cat_ranges(["memory:///dir/cache"], [0], [6]) == [b"cached"]

    asyncio.run(run_async())
