import sys

import pytest

import fsspec
import fsspec.generic
from fsspec.generic import GenericFileSystem, rsync
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.registry import _registry, register_implementation
from fsspec.tests.conftest import data, server  # noqa: F401


def test_remote_async_ops(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    out = fs.info(server.realfile)
    assert out["size"] == len(data)
    assert out["type"] == "file"
    assert fs.isfile(server.realfile)  # this method from superclass


def test_touch_rm(m):
    m.touch("afile")
    m.touch("dir/afile")

    fs = fsspec.filesystem("generic", default_method="current")
    fs.rm("memory://afile")
    assert not m.exists("afile")

    fs.rm("memory://dir", recursive=True)
    assert not m.exists("dir/afile")
    assert not m.exists("dir")


def test_cp_async_to_sync(server, m):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    fs.cp([server.realfile], ["memory://realfile"])
    assert m.cat("realfile") == data

    fs.rm("memory://realfile")
    assert not m.exists("realfile")


def test_pipe_cat_sync(m):
    fs = fsspec.filesystem("generic", default_method="current")
    fs.pipe("memory://afile", b"data")
    assert fs.cat("memory://afile") == b"data"


def test_cat_async(server):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    fs = fsspec.filesystem("generic", default_method="current")
    assert fs.cat(server.realfile) == data


@pytest.mark.parametrize(
    "method,as_list",
    [("copy", True), ("copy", False), ("cp", True), ("cp_file", False)],
)
def test_cp_one(server, tmpdir, method, as_list):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    local = fsspec.filesystem("file")
    fn = f"file://{tmpdir}/afile"
    fs = fsspec.filesystem("generic", default_method="current")

    source, destination = (
        ([server.realfile], [fn]) if as_list else (server.realfile, fn)
    )
    getattr(fs, method)(source, destination)
    assert local.cat(fn) == data
    fs.rm(fn)
    assert not fs.exists(fn)


@pytest.mark.parametrize("mixed", ["source", "destination", "both"])
@pytest.mark.parametrize("reverse", [False, True])
def test_copy_mixed_protocols(m, tmp_path, mixed, reverse):
    m.pipe_file("source-a", b"memory-a")
    m.pipe_file("source-b", b"memory-b")
    local_source = tmp_path / "source-b"
    local_source.write_bytes(b"local-b")
    local_destination = tmp_path / "destination-b"
    fsspec.filesystem("file")
    fs = fsspec.filesystem("generic", default_method="current")
    sources = ["memory://source-a", "memory://source-b"]
    destinations = ["memory://destination-a", "memory://destination-b"]
    if mixed in ("source", "both"):
        sources[1] = local_source.as_uri()
    if mixed in ("destination", "both"):
        destinations[1] = local_destination.as_uri()
    if reverse:
        sources.reverse()
        destinations.reverse()

    side = "destination" if mixed == "destination" else "source"
    with pytest.raises(ValueError, match=f"Multiple {side} backends"):
        fs.copy(sources, destinations, on_error="raise")

    assert not m.exists("destination-a")
    assert not m.exists("destination-b")
    assert not m.exists(local_destination.as_uri())
    assert not local_destination.exists()


def test_copy_mixed_async_source(server, m):
    fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    m.pipe_file("source", b"memory-data")
    fs = fsspec.filesystem("generic", default_method="current")

    with pytest.raises(ValueError, match="Multiple source backends"):
        fs.copy(
            [server.realfile, "memory://source"],
            ["memory://from-http", "memory://from-memory"],
            batch_size=1,
            on_error="raise",
        )

    assert not m.exists("from-http")
    assert not m.exists("from-memory")


@pytest.mark.parametrize("local", [False, True])
def test_copy_homogeneous_batches(m, tmp_path, local):
    m.pipe({"source-a": b"a", "source-b": b"b"})
    if local:
        fsspec.filesystem("file")
        destinations = [(tmp_path / name).as_uri() for name in ("a", "b")]
        # Different protocol aliases still resolve to the same backend.
        destinations[1] = destinations[1].replace("file://", "local://", 1)
    else:
        destinations = ["memory://a", "memory://b"]
    fs = fsspec.filesystem("generic", default_method="current")

    fs.copy(["memory://source-a", "memory://source-b"], destinations, on_error="raise")

    if local:
        assert (tmp_path / "a").read_bytes() == b"a"
        assert (tmp_path / "b").read_bytes() == b"b"
    else:
        assert m.cat_file("a") == b"a"
        assert m.cat_file("b") == b"b"


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("recursive", [False, True])
def test_rm_mixed_protocols(m, tmp_path, reverse, recursive):
    m.pipe_file("target", b"memory target")
    local_target = tmp_path / "target"
    local_target.write_bytes(b"local target")
    # An unrelated memory object named after the local URL: without the
    # check below, the whole batch goes to the first backend and this
    # object is deleted instead of the local file.
    m.pipe_file(local_target.as_uri(), b"unrelated memory object")
    fsspec.filesystem("file")
    fs = fsspec.filesystem("generic", default_method="current")
    urls = ["memory://target", local_target.as_uri()]
    if reverse:
        urls.reverse()

    with pytest.raises(ValueError, match="Multiple backends"):
        fs.rm(urls, recursive=recursive)

    assert local_target.exists()
    assert m.exists("target")
    assert m.exists(local_target.as_uri())


@pytest.mark.parametrize("local", [False, True])
def test_rm_homogeneous_batches(m, tmp_path, local):
    if local:
        targets = [tmp_path / "a", tmp_path / "b"]
        for target in targets:
            target.write_bytes(b"x")
        urls = [target.as_uri() for target in targets]
        # Different protocol aliases still resolve to the same backend.
        urls[1] = urls[1].replace("file://", "local://", 1)
        fsspec.filesystem("file")
    else:
        m.pipe({"target-a": b"a", "target-b": b"b"})
        urls = ["memory://target-a", "memory://target-b"]
    fs = fsspec.filesystem("generic", default_method="current")

    fs.rm(urls)

    if local:
        assert not (tmp_path / "a").exists()
        assert not (tmp_path / "b").exists()
    else:
        assert not m.exists("target-a")
        assert not m.exists("target-b")


@pytest.mark.parametrize("as_list", [False, True])
def test_rm_uncached_single_target(as_list):
    backend = MemoryFileSystem()
    path = "/uncached-rm-single"
    backend.pipe_file(path, b"payload")
    fs = GenericFileSystem(
        default_method="options",
        storage_options={"memory": {"skip_instance_cache": True}},
    )
    url = "memory://" + path
    try:
        fs.rm([url] if as_list else url)
        assert not backend.exists(path)
    finally:
        if backend.exists(path):
            backend.rm(path)


@pytest.mark.parametrize("method", ["copy", "cp_file"])
def test_copy_preserves_tempdir(method, m, tmp_path):
    tempdir = tmp_path / "staging"
    tempdir.mkdir()
    sentinel = tempdir / "unrelated"
    sentinel.write_bytes(b"keep me")
    destination = tmp_path / "copied"
    m.pipe_file("source", b"data")
    fs = fsspec.filesystem("generic")

    getattr(fs, method)("memory://source", destination.as_uri(), tempdir=str(tempdir))

    assert destination.read_bytes() == b"data"
    assert tempdir.is_dir()
    assert sentinel.read_bytes() == b"keep me"
    assert list(tempdir.iterdir()) == [sentinel]


@pytest.mark.parametrize("method", ["copy", "cp_file"])
@pytest.mark.parametrize("missing_source", [True, False])
def test_copy_preserves_tempdir_on_error(method, missing_source, m, tmp_path):
    tempdir = tmp_path / "staging"
    tempdir.mkdir()
    sentinel = tempdir / "unrelated"
    sentinel.write_bytes(b"keep me")
    destination = tmp_path / "missing" / "copied"
    if not missing_source:
        m.pipe_file("source", b"data")
    fs = fsspec.filesystem("generic")

    with pytest.raises(FileNotFoundError):
        getattr(fs, method)(
            "memory://source",
            destination.as_uri(),
            tempdir=str(tempdir),
            on_error="raise",
        )

    assert not destination.exists()
    assert tempdir.is_dir()
    assert sentinel.read_bytes() == b"keep me"
    assert list(tempdir.iterdir()) == [sentinel]


def test_rsync_rejects_names_escaping_destination(tmp_path, m):
    m.pipe({"/src/ok": b"ok", "/src/../../escaped": b"data"})
    dest = tmp_path / "out" / "dest"
    dest.mkdir(parents=True)

    with pytest.raises(ValueError, match="outside the destination"):
        rsync("memory://src", f"file://{dest}")
    assert not (tmp_path / "escaped").exists()
    assert not (dest / "ok").exists()


def test_rsync_rejects_names_escaping_memory_destination(m):
    m.pipe({"/src/ok": b"ok", "/src/a/../../outside": b"data"})
    m.makedirs("/dst", exist_ok=True)

    with pytest.raises(ValueError, match="outside the destination"):
        rsync("memory:///src", "memory:///dst")
    assert not m.exists("/outside")
    assert not m.exists("/dst/ok")


def test_rsync_passes_local_paths_to_check_contained(tmp_path, m, monkeypatch):
    m.pipe({"/src/ok.txt": b"ok", "/src/sub/file.txt": b"data"})
    dest = tmp_path / "dest"
    dest.mkdir()

    seen = []
    orig_check_contained = fsspec.generic.check_contained

    def recording_check_contained(root, paths):
        seen.append((root, list(paths)))
        return orig_check_contained(root, paths)

    monkeypatch.setattr(fsspec.generic, "check_contained", recording_check_contained)
    rsync("memory:///src", f"file://{dest}")

    assert len(seen) == 1
    root_arg, path_args = seen[0]
    assert not root_arg.startswith("file://")
    assert all(not p.startswith("file://") for p in path_args)
    assert (dest / "ok.txt").read_bytes() == b"ok"
    assert (dest / "sub" / "file.txt").read_bytes() == b"data"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Win32 collapses '..' before resolving links",
)
def test_rsync_resolves_symlinks_in_containment(tmp_path, m):
    dest = tmp_path / "outer" / "dest"
    deep = dest / "a" / "b" / "c"
    deep.mkdir(parents=True)
    link = deep / "link"
    try:
        link.symlink_to(dest, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    m.pipe({"/src/ok": b"ok", "/src/a/b/c/link/../../outside.txt": b"data"})

    with pytest.raises(ValueError, match="outside the destination"):
        rsync("memory://src", f"file://{dest}")
    assert not (tmp_path / "outside.txt").exists()
    assert not (dest / "ok").exists()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Win32 collapses '..' before resolving links",
)
def test_rsync_wrapped_local_dirfs_containment(tmp_path, m):
    sandbox = tmp_path / "outer"
    dest = sandbox / "dest"
    deep = dest / "a" / "b" / "c"
    deep.mkdir(parents=True)
    link = deep / "link"
    try:
        link.symlink_to(dest, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    m.pipe({"/src/ok": b"ok", "/src/a/b/c/link/../../outside.txt": b"data"})
    dirfs = DirFileSystem(path=str(sandbox), fs=LocalFileSystem())
    prev_generic = fsspec.generic._generic_fs.copy()
    try:
        fsspec.generic._generic_fs["memory"] = m
        fsspec.generic._generic_fs["dir"] = dirfs
        gfs = GenericFileSystem(default_method="generic")
        with pytest.raises(ValueError, match="outside the destination"):
            rsync("memory:///src", "dir://dest", fs=gfs)
    finally:
        fsspec.generic._generic_fs.clear()
        fsspec.generic._generic_fs.update(prev_generic)
    assert not (tmp_path / "outside.txt").exists()
    assert not (dest / "ok").exists()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Win32 collapses '..' before resolving links",
)
def test_rsync_wrapped_local_async_wrapper_containment(tmp_path, m):
    dest = tmp_path / "outer" / "dest"
    deep = dest / "a" / "b" / "c"
    deep.mkdir(parents=True)
    link = deep / "link"
    try:
        link.symlink_to(dest, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    m.pipe({"/src/ok": b"ok", "/src/a/b/c/link/../../outside.txt": b"data"})
    prev_generic = fsspec.generic._generic_fs.copy()
    try:
        fsspec.generic._generic_fs["memory"] = m
        fsspec.generic._generic_fs["file"] = AsyncFileSystemWrapper(LocalFileSystem())
        gfs = GenericFileSystem(default_method="generic")
        with pytest.raises(ValueError, match="outside the destination"):
            rsync("memory:///src", f"file://{dest}", fs=gfs)
    finally:
        fsspec.generic._generic_fs.clear()
        fsspec.generic._generic_fs.update(prev_generic)
    assert not (tmp_path / "outside.txt").exists()
    assert not (dest / "ok").exists()


def test_rsync_bare_root_dirfs(tmp_path, m):
    root_dir = tmp_path / "root"
    root_dir.mkdir()
    (root_dir / "keep.txt").write_bytes(b"keep")
    (root_dir / "obsolete.txt").write_bytes(b"old")

    dirfs = DirFileSystem(path=str(root_dir), fs=LocalFileSystem())
    prev_generic = fsspec.generic._generic_fs.copy()
    try:
        fsspec.generic._generic_fs["memory"] = m
        fsspec.generic._generic_fs["dir"] = dirfs
        gfs = GenericFileSystem(default_method="generic")

        # Sync from bare dir:// root to memory:///dst
        rsync("dir://", "memory:///dst", fs=gfs)
        assert m.cat("memory:///dst/keep.txt") == b"keep"
        assert m.cat("memory:///dst/obsolete.txt") == b"old"

        # Sync back from memory:///dst into bare dir:// root with delete_missing
        m.rm("memory:///dst/obsolete.txt")
        m.pipe_file("memory:///dst/added.txt", b"new")
        rsync("memory:///dst", "dir://", delete_missing=True, fs=gfs)
        assert (root_dir / "keep.txt").read_bytes() == b"keep"
        assert (root_dir / "added.txt").read_bytes() == b"new"
        assert not (root_dir / "obsolete.txt").exists()
    finally:
        fsspec.generic._generic_fs.clear()
        fsspec.generic._generic_fs.update(prev_generic)


def test_rsync_trailing_slash_source_and_destination(tmp_path):
    class TrailingSlashMemoryFS(MemoryFileSystem):
        protocol = "tsmem"

        @classmethod
        def _strip_protocol(cls, path):
            if isinstance(path, list):
                return [cls._strip_protocol(p) for p in path]
            had_trailing = isinstance(path, str) and path.endswith("/")
            if isinstance(path, str):
                path = path.removeprefix("tsmem://")
            stripped = super()._strip_protocol(path)
            if had_trailing and stripped != "/" and not stripped.endswith("/"):
                return stripped + "/"
            return stripped

        def _norm(self, path):
            return self._strip_protocol(path).rstrip("/") or "/"

        def info(self, path, **kwargs):
            return super().info(self._norm(path), **kwargs)

        def ls(self, path, detail=True, **kwargs):
            return super().ls(self._norm(path), detail=detail, **kwargs)

        def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
            return super().find(
                self._norm(path),
                maxdepth=maxdepth,
                withdirs=withdirs,
                detail=detail,
                **kwargs,
            )

    tsfs = TrailingSlashMemoryFS(global_store=False, skip_instance_cache=True)
    tsfs.pipe({"/src/keep.txt": b"keep", "/src/sub/new.txt": b"new"})
    tsfs.pipe({"/dst/keep.txt": b"keep", "/dst/stale.txt": b"old"})
    tsfs.pipe({"/src_sibling/other.txt": b"other"})

    dest = tmp_path / "dest"
    dest.mkdir()

    prev_generic = fsspec.generic._generic_fs.copy()
    try:
        fsspec.generic._generic_fs["tsmem"] = tsfs
        fsspec.generic._generic_fs["file"] = LocalFileSystem()
        gfs = GenericFileSystem(default_method="generic")

        # Sync from a prefix whose _strip_protocol keeps a trailing slash to local.
        rsync("tsmem:///src/", f"file://{dest}/", fs=gfs)
        assert (dest / "keep.txt").read_bytes() == b"keep"
        assert (dest / "sub" / "new.txt").read_bytes() == b"new"
        assert not (dest / "other.txt").exists()

        # Sync between trailing-slash prefixes with delete_missing=True.
        rsync("tsmem:///src/", "tsmem:///dst/", delete_missing=True, fs=gfs)
        assert tsfs.cat("/dst/keep.txt") == b"keep"
        assert tsfs.cat("/dst/sub/new.txt") == b"new"
        assert not tsfs.exists("/dst/stale.txt")
        assert tsfs.exists("/src_sibling/other.txt")
    finally:
        fsspec.generic._generic_fs.clear()
        fsspec.generic._generic_fs.update(prev_generic)


def test_rsync_preserves_root_marker_object():
    class MarkerObjectFS(MemoryFileSystem):
        protocol = "markmem"

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.markers = set()

        @classmethod
        def _strip_protocol(cls, path):
            if isinstance(path, list):
                return [cls._strip_protocol(p) for p in path]
            if isinstance(path, str):
                path = path.removeprefix("markmem://")
            had_trailing = isinstance(path, str) and path.endswith("/")
            stripped = super()._strip_protocol(path)
            if had_trailing and stripped != "/" and not stripped.endswith("/"):
                return stripped + "/"
            return stripped

        def _norm(self, path):
            return self._strip_protocol(path).rstrip("/") or "/"

        def info(self, path, **kwargs):
            return super().info(self._norm(path), **kwargs)

        def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
            norm = self._norm(path)
            out = super().find(
                norm, maxdepth=maxdepth, withdirs=withdirs, detail=True, **kwargs
            )
            for mk in sorted(self.markers):
                if mk == norm + "/" or mk.startswith(norm + "/"):
                    out[mk] = {"name": mk, "size": 0, "type": "file"}
            return out if detail else list(out)

        def copy(self, path1, path2, **kwargs):
            p1s = [path1] if isinstance(path1, str) else list(path1)
            p2s = [path2] if isinstance(path2, str) else list(path2)
            for src, dst in zip(p1s, p2s):
                s = self._strip_protocol(src)
                d = self._strip_protocol(dst)
                if s.endswith("/"):
                    self.markers.add(d)
                else:
                    super().copy(s, d, **kwargs)

        def rm(self, path, recursive=False, maxdepth=None):
            paths = [path] if isinstance(path, str) else list(path)
            for p in paths:
                sp = self._strip_protocol(p)
                if sp in self.markers:
                    self.markers.remove(sp)
                else:
                    super().rm(sp, recursive=recursive, maxdepth=maxdepth)

    mfs = MarkerObjectFS(global_store=False, skip_instance_cache=True)
    mfs.pipe({"/src/keep.txt": b"keep", "/src/sub/new.txt": b"new"})
    mfs.markers.update({"/src/", "/src/sub/"})

    prev_generic = fsspec.generic._generic_fs.copy()
    try:
        fsspec.generic._generic_fs["markmem"] = mfs
        gfs = GenericFileSystem(default_method="generic")

        # Forward copy preserves the root marker with its trailing slash.
        rsync("markmem:///src", "markmem:///dst", fs=gfs)
        assert "/dst/" in mfs.markers
        assert "/dst/sub/" in mfs.markers
        assert "/dst" not in mfs.store

        # delete_missing=True keeps the destination root marker when present in source.
        mfs.pipe_file("/dst/stale.txt", b"old")
        rsync("markmem:///src", "markmem:///dst", delete_missing=True, fs=gfs)
        assert "/dst/" in mfs.markers
        assert not mfs.exists("/dst/stale.txt")
    finally:
        fsspec.generic._generic_fs.clear()
        fsspec.generic._generic_fs.update(prev_generic)


def test_rsync_backslash_in_source_name(tmp_path, m):
    key = "/src/a\\..\\..\\outside.txt"
    m.pipe({key: b"data", "/src/ok.txt": b"ok"})

    # Remote-to-remote sync treats backslashes as literal object-key characters.
    m.makedirs("/dst", exist_ok=True)
    rsync("memory:///src", "memory:///dst")
    assert m.cat("/dst/a\\..\\..\\outside.txt") == b"data"

    # Sync to local rejects backslashes escaping destination on Windows, and
    # preserves them as literal filenames on POSIX.
    dest = tmp_path / "dest"
    dest.mkdir()
    if sys.platform == "win32":
        with pytest.raises(ValueError, match="outside the destination"):
            rsync("memory:///src", f"file://{dest}")
        assert not (tmp_path / "outside.txt").exists()
    else:
        rsync("memory:///src", f"file://{dest}")
        assert (dest / "a\\..\\..\\outside.txt").read_bytes() == b"data"


@pytest.mark.parametrize("relative", ["ordinary.txt", "nested/memory:///src/data"])
def test_rsync_preserves_source_url_in_relative_name(m, relative):
    source = f"memory:///src/{relative}"
    destination = f"memory:///dst/{relative}"
    m.pipe_file(source, b"payload")

    copied = rsync("memory:///src", "memory:///dst")

    assert copied == {source: destination}
    assert m.cat(destination) == b"payload"
    assert m.find("/dst") == [destination.removeprefix("memory://")]


@pytest.mark.parametrize("relative", ["ordinary.txt", "nested/memory:///dst/data"])
def test_rsync_delete_missing_preserves_destination_url_in_relative_name(m, relative):
    source = f"memory:///src/{relative}"
    destination = f"memory:///dst/{relative}"
    m.pipe(
        {source: b"payload", destination: b"payload", "memory:///dst/obsolete": b"old"}
    )

    assert rsync("memory:///src", "memory:///dst", delete_missing=True) == {}

    assert m.cat(destination) == b"payload"
    assert not m.exists("/dst/obsolete")


def test_rsync(tmpdir, m):
    from fsspec.generic import GenericFileSystem, rsync

    fs = GenericFileSystem()
    fs.pipe("memory:///deep/path/afile", b"data1")
    fs.pipe("memory:///deep/afile", b"data2")

    with pytest.raises(ValueError):
        rsync("memory:///deep/afile", f"file://{tmpdir}")
    rsync("memory://", f"file://{tmpdir}")

    allfiles = fs.find(f"file://{tmpdir}", withdirs=True, detail=True)
    pos_tmpdir = fsspec.implementations.local.make_path_posix(str(tmpdir))  # for WIN
    assert set(allfiles) == {
        f"file://{pos_tmpdir}{_}"
        for _ in [
            "",
            "/deep",
            "/deep/path",
            "/deep/path/afile",
            "/deep/afile",
        ]
    }
    fs.rm("memory:///deep/afile")
    rsync("memory://", f"file://{tmpdir}", delete_missing=True)
    allfiles2 = fs.find(f"file://{tmpdir}", withdirs=True, detail=True)
    assert set(allfiles2) == {
        f"file://{pos_tmpdir}{_}"
        for _ in [
            "",
            "/deep",
            "/deep/path",
            "/deep/path/afile",
        ]
    }
    # the file was not updated, since size was correct
    assert (
        allfiles[f"file://{pos_tmpdir}/deep/path/afile"]
        == allfiles2[f"file://{pos_tmpdir}/deep/path/afile"]
    )


class _OptionsRequiredFS(MemoryFileSystem):
    """Backend that raises unless its storage_options reached the constructor."""

    protocol = "optsrequired"

    def __init__(self, *args, token=None, **kwargs):
        if token is None:
            raise ValueError("storage_options were not propagated")
        super().__init__(*args, **kwargs)

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, str):
            path = path.removeprefix(f"{cls.protocol}://")
        return MemoryFileSystem._strip_protocol(path)


@pytest.fixture()
def opts_fs(m):
    register_implementation(
        _OptionsRequiredFS.protocol, _OptionsRequiredFS, clobber=True
    )
    m.pipe_file("/afile", b"hello")
    m.makedirs("/adir", exist_ok=True)
    m.pipe_file("/adir/nested", b"world")
    yield fsspec.filesystem(
        "generic",
        default_method="options",
        storage_options={_OptionsRequiredFS.protocol: {"token": "set"}},
    )
    _registry.pop(_OptionsRequiredFS.protocol, None)
    _OptionsRequiredFS.clear_instance_cache()


def test_storage_options_propagated_to_backend(opts_fs):
    opts_fs.info("optsrequired:///afile")
    opts_fs.ls("optsrequired:///adir")
    opts_fs.cat_file("optsrequired:///afile")
    opts_fs.isdir("optsrequired:///adir")
    opts_fs.find("optsrequired:///adir")


def test_storage_options_propagated_cross_protocol(opts_fs, tmpdir):
    rsync(
        "optsrequired:///adir",
        f"file://{tmpdir}",
        inst_kwargs={
            "default_method": "options",
            "storage_options": {_OptionsRequiredFS.protocol: {"token": "set"}},
        },
    )
    assert (tmpdir / "nested").read_binary() == b"world"
