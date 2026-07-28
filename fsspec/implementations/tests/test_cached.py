import asyncio
import json
import os
import random
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import fsspec
from fsspec.compression import compr
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.cache_mapper import (
    BasenameCacheMapper,
    HashCacheMapper,
    create_cache_mapper,
)
from fsspec.implementations.cached import (
    CachingFileSystem,
    LocalTempFile,
    WholeFileCacheFileSystem,
    _replace_tempfile,
)
from fsspec.implementations.local import make_path_posix
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.implementations.zip import ZipFileSystem
from fsspec.tests.conftest import win

from .test_ftp import FTPFileSystem


@pytest.fixture
def local_filecache():
    import tempfile

    original_location = tempfile.mkdtemp()
    cache_location = tempfile.mkdtemp()
    original_file = os.path.join(original_location, "afile")
    data = b"test data"
    with open(original_file, "wb") as f:
        f.write(data)

    # we can access the file and read it
    fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=cache_location
    )

    return data, original_file, cache_location, fs


def test_mapper():
    mapper0 = create_cache_mapper(True)
    assert mapper0("somefile") == "somefile"
    assert mapper0("/somefile") == "somefile"
    assert mapper0("/somedir/somefile") == "somefile"
    assert mapper0("/otherdir/somefile") == "somefile"

    mapper1 = create_cache_mapper(False)
    assert (
        mapper1("somefile")
        == "dd00b9487898b02555b6a2d90a070586d63f93e80c70aaa60c992fa9e81a72fe"
    )
    assert (
        mapper1("/somefile")
        == "884c07bc2efe65c60fb9d280a620e7f180488718fb5d97736521b7f9cf5c8b37"
    )
    assert (
        mapper1("/somedir/somefile")
        == "67a6956e5a5f95231263f03758c1fd9254fdb1c564d311674cec56b0372d2056"
    )
    assert (
        mapper1("/otherdir/somefile")
        == "f043dee01ab9b752c7f2ecaeb1a5e1b2d872018e2d0a1a26c43835ebf34e7d3e"
    )

    assert mapper0 != mapper1
    assert create_cache_mapper(True) == mapper0
    assert create_cache_mapper(False) == mapper1

    assert hash(mapper0) != hash(mapper1)
    assert hash(create_cache_mapper(True)) == hash(mapper0)
    assert hash(create_cache_mapper(False)) == hash(mapper1)

    with pytest.raises(
        ValueError,
        match="BasenameCacheMapper requires zero or positive directory_levels",
    ):
        BasenameCacheMapper(-1)

    mapper2 = BasenameCacheMapper(1)
    assert mapper2("/somefile") == "somefile"
    assert mapper2("/somedir/somefile") == "somedir_@_somefile"
    assert mapper2("/otherdir/somefile") == "otherdir_@_somefile"
    assert mapper2("/dir1/dir2/dir3/somefile") == "dir3_@_somefile"

    assert mapper2 != mapper0
    assert mapper2 != mapper1
    assert BasenameCacheMapper(1) == mapper2

    assert hash(mapper2) != hash(mapper0)
    assert hash(mapper2) != hash(mapper1)
    assert hash(BasenameCacheMapper(1)) == hash(mapper2)

    mapper3 = BasenameCacheMapper(2)
    assert mapper3("/somefile") == "somefile"
    assert mapper3("/somedir/somefile") == "somedir_@_somefile"
    assert mapper3("/otherdir/somefile") == "otherdir_@_somefile"
    assert mapper3("/dir1/dir2/dir3/somefile") == "dir2_@_dir3_@_somefile"

    assert mapper3 != mapper0
    assert mapper3 != mapper1
    assert mapper3 != mapper2
    assert BasenameCacheMapper(2) == mapper3

    assert hash(mapper3) != hash(mapper0)
    assert hash(mapper3) != hash(mapper1)
    assert hash(mapper3) != hash(mapper2)
    assert hash(BasenameCacheMapper(2)) == hash(mapper3)


@pytest.mark.parametrize(
    "cache_mapper", [BasenameCacheMapper(), BasenameCacheMapper(1), HashCacheMapper()]
)
def test_metadata(tmpdir, cache_mapper):
    source = os.path.join(tmpdir, "source")
    afile = os.path.join(source, "afile")
    os.mkdir(source)
    open(afile, "w").write("test")

    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=os.path.join(tmpdir, "cache"),
        cache_mapper=cache_mapper,
    )

    with fs.open(afile, "rb") as f:
        assert f.read(5) == b"test"

    afile_posix = make_path_posix(afile)
    detail = fs._metadata.cached_files[0][afile_posix]
    assert sorted(detail.keys()) == ["blocks", "fn", "original", "time", "uid"]
    assert isinstance(detail["blocks"], bool)
    assert isinstance(detail["fn"], str)
    assert isinstance(detail["time"], float)
    assert isinstance(detail["uid"], str)

    assert detail["original"] == afile_posix
    assert detail["fn"] == fs._mapper(afile_posix)

    if isinstance(cache_mapper, BasenameCacheMapper):
        if cache_mapper.directory_levels == 0:
            assert detail["fn"] == "afile"
        else:
            assert detail["fn"] == "source_@_afile"


def test_constructor_kwargs(tmpdir):
    fs = fsspec.filesystem("filecache", target_protocol="file", same_names=True)
    assert isinstance(fs._mapper, BasenameCacheMapper)

    fs = fsspec.filesystem("filecache", target_protocol="file", same_names=False)
    assert isinstance(fs._mapper, HashCacheMapper)

    fs = fsspec.filesystem("filecache", target_protocol="file")
    assert isinstance(fs._mapper, HashCacheMapper)

    with pytest.raises(
        ValueError, match="Cannot specify both same_names and cache_mapper"
    ):
        fs = fsspec.filesystem(
            "filecache",
            target_protocol="file",
            cache_mapper=HashCacheMapper(),
            same_names=True,
        )


@pytest.mark.skipif(win, reason="POSIX file permissions")
@pytest.mark.parametrize("protocol", ["filecache", "simplecache", "blockcache"])
def test_cache_storage_mode(tmp_path, protocol):
    import stat

    cache = tmp_path / "cache"  # does not exist yet, fsspec must create it
    old = os.umask(0o022)
    try:
        fsspec.filesystem(
            protocol,
            target_protocol="file",
            cache_storage=str(cache),
            cache_storage_mode=0o700,
        )
    finally:
        os.umask(old)

    mode = stat.S_IMODE(os.stat(cache).st_mode)
    assert mode & (stat.S_IRWXG | stat.S_IRWXO) == 0, oct(mode)


def test_idempotent():
    import pickle

    fs = CachingFileSystem("file")
    fs2 = CachingFileSystem("file")
    assert fs2 is fs
    fs3 = pickle.loads(pickle.dumps(fs))
    assert fs3.storage == fs.storage


def test_blockcache_workflow(ftp_writable, tmp_path):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out", "wb") as f:
        f.write(b"test\n" * 4096)

    fs_kwargs = {
        "skip_instance_cache": True,
        "cache_storage": str(tmp_path),
        "target_protocol": "ftp",
        "target_options": {
            "host": host,
            "port": port,
            "username": user,
            "password": pw,
        },
    }

    # Open the blockcache and read a little bit of the data
    fs = fsspec.filesystem("blockcache", **fs_kwargs)
    with fs.open("/out", "rb", block_size=5) as f:
        assert f.read(5) == b"test\n"

    # Save the cache/close it
    fs.save_cache()
    del fs

    # Check that cache file only has the first two blocks
    with open(tmp_path / "cache", "r") as f:
        cache = json.load(f)
    assert "/out" in cache
    assert cache["/out"]["blocks"] == [0, 1]

    # Reopen the same cache and read some more...
    fs = fsspec.filesystem("blockcache", **fs_kwargs)
    with fs.open("/out", block_size=5) as f:
        assert f.read(5) == b"test\n"
        f.seek(30)
        assert f.read(5) == b"test\n"


@pytest.mark.parametrize("impl", ["filecache", "blockcache", "cached"])
def test_workflow(ftp_writable, impl):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out", "wb") as f:
        f.write(b"test")
    fs = fsspec.filesystem(
        impl,
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )
    assert os.listdir(fs.storage[-1]) == []
    with fs.open("/out") as f:
        assert os.listdir(fs.storage[-1])
        assert f.read() == b"test"
        assert fs._metadata.cached_files[-1]["/out"]["blocks"]
    assert fs.cat("/out") == b"test"
    assert fs._metadata.cached_files[-1]["/out"]["blocks"] is True

    with fs.open("/out", "wb") as f:
        f.write(b"changed")

    if impl == "filecache":
        assert (
            fs.cat("/out") == b"changed"
        )  # new value, because we overwrote the cached location


@pytest.mark.parametrize("impl", ["simplecache", "blockcache", "cached"])
def test_glob(ftp_writable, impl):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out", "wb") as f:
        f.write(b"test")
    with fs.open("/out2", "wb") as f:
        f.write(b"test2")
    fs = fsspec.filesystem(
        impl,
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )
    assert fs.glob("/wrong*") == []
    assert fs.glob("/ou*") == ["/out", "/out2"]


def test_write():
    tmp = str(tempfile.mkdtemp())
    fn = tmp + "afile"
    url = f"simplecache::file://{fn}"
    with fsspec.open(url, "wb") as f:
        f.write(b"hello")
        assert fn not in f.name
        assert not os.listdir(tmp)

    assert open(fn, "rb").read() == b"hello"


def test_clear():
    import tempfile

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    # populates first cache
    fs = fsspec.filesystem("filecache", target_protocol="file", cache_storage=cache1)
    assert fs.cat(f1) == data

    assert "cache" in os.listdir(cache1)
    assert len(os.listdir(cache1)) == 2
    assert fs._check_file(f1)

    fs.clear_cache()
    assert not fs._check_file(f1)
    assert len(os.listdir(cache1)) < 2


def test_clear_expired(tmp_path):
    def __ager(cache_fn, fn, del_fn=False):
        """
        Modify the cache file to virtually add time lag to selected files.

        Parameters
        ---------
        cache_fn: str
            cache path
        fn: str
            file name to be modified
        del_fn: bool
            whether or not to delete 'fn' from cache details
        """
        import pathlib
        import time

        if os.path.exists(cache_fn):
            with open(cache_fn, "r") as f:
                cached_files = json.load(f)
            fn_posix = pathlib.Path(fn).as_posix()
            cached_files[fn_posix]["time"] = cached_files[fn_posix]["time"] - 691200
            assert os.access(cache_fn, os.W_OK), "Cache is not writable"
            if del_fn:
                del cached_files[fn_posix]["fn"]
            with open(cache_fn, "w") as f:
                json.dump(cached_files, f)
            time.sleep(1)

    origin = tmp_path.joinpath("origin")
    cache1 = tmp_path.joinpath("cache1")
    cache2 = tmp_path.joinpath("cache2")
    cache3 = tmp_path.joinpath("cache3")

    origin.mkdir()
    cache1.mkdir()
    cache2.mkdir()
    cache3.mkdir()

    data = b"test data"
    f1 = origin.joinpath("afile")
    f2 = origin.joinpath("bfile")
    f3 = origin.joinpath("cfile")
    f4 = origin.joinpath("dfile")

    with open(f1, "wb") as f:
        f.write(data)
    with open(f2, "wb") as f:
        f.write(data)
    with open(f3, "wb") as f:
        f.write(data)
    with open(f4, "wb") as f:
        f.write(data)

    # populates first cache
    fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=str(cache1), cache_check=1
    )
    assert fs.cat(str(f1)) == data

    # populates "last" cache if file not found in first one
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=[str(cache1), str(cache2)],
        cache_check=1,
    )
    assert fs.cat(str(f2)) == data
    assert fs.cat(str(f3)) == data
    assert len(os.listdir(cache2)) == 3

    # force the expiration
    cache_fn = os.path.join(fs.storage[-1], "cache")
    __ager(cache_fn, f2)

    # remove from cache2 the expired files
    fs.clear_expired_cache()
    assert len(os.listdir(cache2)) == 2

    # check complete cleanup
    __ager(cache_fn, f3)

    fs.clear_expired_cache()
    assert not fs._check_file(f2)
    assert not fs._check_file(f3)
    assert len(os.listdir(cache2)) < 2

    # check cache1 to be untouched after cleaning
    assert len(os.listdir(cache1)) == 2

    # check cleaning with 'same_name' option enabled
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=[str(cache1), str(cache2), str(cache3)],
        same_names=True,
        cache_check=1,
    )
    assert fs.cat(str(f4)) == data

    cache_fn = os.path.join(fs.storage[-1], "cache")
    __ager(cache_fn, f4)

    fs.clear_expired_cache()
    assert not fs._check_file(str(f4))

    # check cache metadata lacking 'fn' raises RuntimeError.
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=str(cache1),
        same_names=True,
        cache_check=1,
    )
    assert fs.cat(str(f1)) == data

    cache_fn = os.path.join(fs.storage[-1], "cache")
    __ager(cache_fn, f1, del_fn=True)

    with pytest.raises(RuntimeError, match="Cache metadata does not contain 'fn' for"):
        fs.clear_expired_cache()


def test_pop():
    import tempfile

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    cache2 = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    f2 = os.path.join(origin, "bfile")
    with open(f1, "wb") as f:
        f.write(data)
    with open(f2, "wb") as f:
        f.write(data)

    # populates first cache
    fs = fsspec.filesystem("filecache", target_protocol="file", cache_storage=cache1)
    fs.cat(f1)

    # populates last cache if file not found in first cache
    fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=[cache1, cache2]
    )
    assert fs.cat(f2) == data
    assert len(os.listdir(cache2)) == 2
    assert fs._check_file(f1)
    with pytest.raises(PermissionError):
        fs.pop_from_cache(f1)
    fs.pop_from_cache(f2)
    fs.pop_from_cache(os.path.join(origin, "uncached-file"))
    assert len(os.listdir(cache2)) == 1
    assert not fs._check_file(f2)
    assert fs._check_file(f1)


def test_blocksize(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out_block", "wb") as f:
        f.write(b"test" * 4000)

    fs = fsspec.filesystem(
        "blockcache",
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )

    with fs.open("/out_block", block_size=20) as f:
        assert f.read(1) == b"t"
    with pytest.raises(BlocksizeMismatchError):
        fs.open("/out_block", block_size=30)


def test_blockcache_multiinstance(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/one", "wb") as f:
        f.write(b"test" * 40)
    with fs.open("/two", "wb") as f:
        f.write(b"test" * 40)
    fs = fsspec.filesystem(
        "blockcache",
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )

    with fs.open("/one", block_size=20) as f:
        assert f.read(1) == b"t"
    fs2 = fsspec.filesystem(
        "blockcache",
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
        skip_instance_cache=True,
        cache_storage=fs.storage,
    )
    assert fs2._metadata.cached_files  # loaded from metadata for "one"
    with fs2.open("/two", block_size=20) as f:
        assert f.read(1) == b"t"
    assert "/two" in fs2._metadata.cached_files[-1]
    fs.save_cache()
    assert list(fs._metadata.cached_files[-1]) == ["/one", "/two"]
    assert list(fs2._metadata.cached_files[-1]) == ["/one", "/two"]


def test_metadata_save_blocked(ftp_writable, caplog):
    import logging

    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/one", "wb") as f:
        f.write(b"test" * 40)
    fs = fsspec.filesystem(
        "blockcache",
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )

    with fs.open("/one", block_size=20) as f:
        assert f.read(1) == b"t"
    fn = os.path.join(fs.storage[-1], "cache")
    with caplog.at_level(logging.DEBUG):
        with fs.open("/one", block_size=20) as f:
            f.seek(21)
            assert f.read(1)
            os.remove(fn)
            os.mkdir(fn)
    assert "Cache saving failed while closing file" in caplog.text
    os.rmdir(fn)

    def open_raise(*_, **__):
        raise NameError

    try:
        # To simulate an interpreter shutdown we temporarily set an open function in the
        # cache_metadata module which is used on the next attempt to save metadata.
        with caplog.at_level(logging.DEBUG):
            with fs.open("/one", block_size=20) as f:
                fsspec.implementations.cache_metadata.open = open_raise
                f.seek(21)
                assert f.read(1)
    finally:
        fsspec.implementations.cache_metadata.__dict__.pop("open", None)
    assert "Cache save failed due to interpreter shutdown" in caplog.text


@pytest.mark.parametrize("impl", ["filecache", "simplecache", "blockcache", "cached"])
def test_local_filecache_creates_dir_if_needed(impl):
    import tempfile

    original_location = tempfile.mkdtemp()
    cache_location = tempfile.mkdtemp()
    os.rmdir(cache_location)
    assert not os.path.exists(cache_location)

    original_file = os.path.join(original_location, "afile")
    data = b"test data"
    with open(original_file, "wb") as f:
        f.write(data)

    # we can access the file and read it
    fs = fsspec.filesystem(impl, target_protocol="file", cache_storage=cache_location)

    with fs.open(original_file, "rb") as f:
        data_in_cache = f.read()

    assert os.path.exists(cache_location)

    assert data_in_cache == data


@pytest.mark.parametrize("toplevel", [True, False])
@pytest.mark.parametrize("impl", ["filecache", "simplecache", "blockcache"])
def test_get_mapper(impl, toplevel):
    import tempfile

    original_location = tempfile.mkdtemp()
    cache_location = tempfile.mkdtemp()
    os.rmdir(cache_location)
    original_file = os.path.join(original_location, "afile")
    data = b"test data"
    with open(original_file, "wb") as f:
        f.write(data)

    if toplevel:
        m = fsspec.get_mapper(
            f"{impl}::file://{original_location}",
            **{impl: {"cache_storage": cache_location}},
        )
    else:
        fs = fsspec.filesystem(
            impl, target_protocol="file", cache_storage=cache_location
        )
        m = fs.get_mapper(original_location)

    assert m["afile"] == data
    assert os.listdir(cache_location)
    assert m["afile"] == data


def test_local_filecache_basic(local_filecache):
    data, original_file, cache_location, fs = local_filecache

    # reading from the file contains the right data
    with fs.open(original_file, "rb") as f:
        assert f.read() == data
    assert "cache" in os.listdir(cache_location)

    # the file in the location contains the right data
    fn = next(iter(fs._metadata.cached_files[-1].values()))[
        "fn"
    ]  # this is a hash value
    assert fn in os.listdir(cache_location)
    with open(os.path.join(cache_location, fn), "rb") as f:
        assert f.read() == data

    # still there when original file is removed (check=False)
    os.remove(original_file)
    with fs.open(original_file, "rb") as f:
        assert f.read() == data


def test_local_filecache_does_not_change_when_original_data_changed(local_filecache):
    old_data, original_file, cache_location, fs = local_filecache
    new_data = b"abc"

    with fs.open(original_file, "rb") as f:
        assert f.read() == old_data

    with open(original_file, "wb") as f:
        f.write(new_data)

    with fs.open(original_file, "rb") as f:
        assert f.read() == old_data


def test_local_filecache_gets_from_original_if_cache_deleted(local_filecache):
    old_data, original_file, cache_location, fs = local_filecache
    new_data = b"abc"

    with fs.open(original_file, "rb") as f:
        assert f.read() == old_data

    with open(original_file, "wb") as f:
        f.write(new_data)

    shutil.rmtree(cache_location)
    assert os.path.exists(original_file)

    with open(original_file, "rb") as f:
        assert f.read() == new_data

    with fs.open(original_file, "rb") as f:
        assert f.read() == new_data

    # the file in the location contains the right data
    fn = next(iter(fs._metadata.cached_files[-1].values()))[
        "fn"
    ]  # this is a hash value
    assert fn in os.listdir(cache_location)
    with open(os.path.join(cache_location, fn), "rb") as f:
        assert f.read() == new_data


def test_local_filecache_with_new_cache_location_makes_a_new_copy(local_filecache):
    import tempfile

    data, original_file, old_cache_location, old_fs = local_filecache
    new_cache_location = tempfile.mkdtemp()

    with old_fs.open(original_file, "rb") as f:
        assert f.read() == data

    new_fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=new_cache_location
    )

    with new_fs.open(original_file, "rb") as f:
        assert f.read() == data

    # the file in the location contains the right data
    fn = next(iter(new_fs._metadata.cached_files[-1].values()))[
        "fn"
    ]  # this is a hash value
    assert fn in os.listdir(old_cache_location)
    assert fn in os.listdir(new_cache_location)

    with open(os.path.join(new_cache_location, fn), "rb") as f:
        assert f.read() == data


def test_filecache_multicache():
    import tempfile

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    cache2 = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    f2 = os.path.join(origin, "bfile")
    with open(f1, "wb") as f:
        f.write(data)
    with open(f2, "wb") as f:
        f.write(data * 2)

    # populates first cache
    fs = fsspec.filesystem("filecache", target_protocol="file", cache_storage=cache1)
    assert fs.cat(f1) == data

    assert len(os.listdir(cache1)) == 2  # cache and hashed afile
    assert len(os.listdir(cache2)) == 0  # hasn't been initialized yet

    # populates last cache if file not found in first cache
    fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=[cache1, cache2]
    )

    assert fs.cat(f1) == data
    assert fs.cat(f2) == data * 2

    assert "cache" in os.listdir(cache1)
    assert "cache" in os.listdir(cache2)

    cache1_contents = [f for f in os.listdir(cache1) if f != "cache"]
    assert len(cache1_contents) == 1

    with open(os.path.join(cache1, cache1_contents[0]), "rb") as f:
        assert f.read() == data

    cache2_contents = [f for f in os.listdir(cache2) if f != "cache"]
    assert len(cache2_contents) == 1

    with open(os.path.join(cache2, cache2_contents[0]), "rb") as f:
        assert f.read() == data * 2


@pytest.mark.parametrize("impl", ["filecache", "simplecache"])
def test_filecache_multicache_with_same_file_different_data_reads_from_first(impl):
    import tempfile

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    cache2 = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    # populate first cache
    fs1 = fsspec.filesystem(impl, target_protocol="file", cache_storage=cache1)
    assert fs1.cat(f1) == data

    with open(f1, "wb") as f:
        f.write(data * 2)

    # populate second cache
    fs2 = fsspec.filesystem(impl, target_protocol="file", cache_storage=cache2)

    assert fs2.cat(f1) == data * 2

    # the filenames in each cache are the same, but the data is different
    assert sorted(os.listdir(cache1)) == sorted(os.listdir(cache2))

    fs = fsspec.filesystem(impl, target_protocol="file", cache_storage=[cache1, cache2])

    assert fs.cat(f1) == data


def test_filecache_with_checks():
    import time

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    # populate first cache
    fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=cache1, expiry_time=0.1
    )
    fs2 = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=cache1, check_files=True
    )
    assert fs.cat(f1) == data
    assert fs2.cat(f1) == data

    with open(f1, "wb") as f:
        f.write(data * 2)

    assert fs.cat(f1) == data  # does not change
    assert fs2.cat(f1) == data * 2  # changed, since origin changed
    with fs2.open(f1) as f:
        assert f.read() == data * 2  # read also sees new data
    time.sleep(0.11)  # allow cache details to expire
    assert fs.cat(f1) == data * 2  # changed, since origin changed


@pytest.mark.parametrize("impl", ["filecache", "simplecache", "blockcache", "cached"])
@pytest.mark.parametrize("fs", ["local", "multi"], indirect=["fs"])
def test_filecache_takes_fs_instance(impl, fs):
    origin = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    fs2 = fsspec.filesystem(impl, fs=fs)

    assert fs2.cat(f1) == data


@pytest.mark.parametrize("impl", ["filecache", "simplecache", "blockcache", "cached"])
@pytest.mark.parametrize("fs", ["local", "multi"], indirect=["fs"])
def test_filecache_serialization(impl, fs):
    fs1 = fsspec.filesystem(impl, fs=fs)
    json1 = fs1.to_json()

    assert fs1 is fsspec.AbstractFileSystem.from_json(json1)


def test_add_file_to_cache_after_save(local_filecache):
    (data, original_file, cache_location, fs) = local_filecache

    fs.save_cache()

    fs.cat(original_file)
    assert len(fs._metadata.cached_files[-1]) == 1

    fs.save_cache()

    fs2 = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=cache_location,
        do_not_use_cache_for_this_instance=True,  # cache is masking the issue
    )
    assert len(fs2._metadata.cached_files[-1]) == 1


def test_cached_open_close_read(ftp_writable):
    # Regression test for <https://github.com/fsspec/filesystem_spec/issues/799>
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out_block", "wb") as f:
        f.write(b"test" * 4000)
    fs = fsspec.filesystem(
        "cached",
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )
    with fs.open("/out_block", block_size=1024) as f:
        pass
    with fs.open("/out_block", block_size=1024) as f:
        assert f.read(1) == b"t"
    # Regression test for <https://github.com/fsspec/filesystem_spec/issues/845>
    assert fs._metadata.cached_files[-1]["/out_block"]["blocks"] == {0}


@pytest.mark.parametrize("impl", ["filecache", "simplecache"])
@pytest.mark.parametrize("compression", ["gzip", "bz2"])
def test_with_compression(impl, compression):
    data = b"123456789"
    tempdir = tempfile.mkdtemp()
    cachedir = tempfile.mkdtemp()
    fn = os.path.join(tempdir, "data")
    f = compr[compression](open(fn, mode="wb"), mode="w")
    f.write(data)
    f.close()

    with fsspec.open(
        f"{impl}::{fn}",
        "rb",
        compression=compression,
        **{impl: {"same_names": True, "cache_storage": cachedir}},
    ) as f:
        # stores original compressed file, uncompress on read
        assert f.read() == data
        assert "data" in os.listdir(cachedir)
        assert open(os.path.join(cachedir, "data"), "rb").read() != data

    cachedir = tempfile.mkdtemp()

    with fsspec.open(
        f"{impl}::{fn}",
        "rb",
        **{
            impl: {
                "same_names": True,
                "compression": compression,
                "cache_storage": cachedir,
            }
        },
    ) as f:
        # stores uncompressed data
        assert f.read() == data
        assert "data" in os.listdir(cachedir)
        assert open(os.path.join(cachedir, "data"), "rb").read() == data


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_again(protocol):
    fn = "memory://afile"
    with fsspec.open(fn, "wb") as f:
        f.write(b"hello")
    d2 = tempfile.mkdtemp()
    lurl = fsspec.open_local(f"{protocol}::{fn}", **{protocol: {"cache_storage": d2}})
    assert os.path.exists(lurl)
    assert d2 in lurl
    assert open(lurl, "rb").read() == b"hello"

    # remove cache dir
    shutil.rmtree(d2)
    assert not os.path.exists(lurl)

    # gets recreated
    lurl = fsspec.open_local(f"{protocol}::{fn}", **{protocol: {"cache_storage": d2}})
    assert open(lurl, "rb").read() == b"hello"


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_multi_cache(protocol):
    with fsspec.open_files("memory://file*", "wb", num=2) as files:
        for f in files:
            f.write(b"hello")

    d2 = tempfile.mkdtemp()
    lurl = fsspec.open_local(
        f"{protocol}::memory://file*",
        mode="rb",
        **{protocol: {"cache_storage": d2, "same_names": True}},
    )
    assert all(d2 in u for u in lurl)
    assert all(os.path.basename(f) in ["file0", "file1"] for f in lurl)
    assert all(open(u, "rb").read() == b"hello" for u in lurl)

    d2 = tempfile.mkdtemp()
    lurl = fsspec.open_files(
        f"{protocol}::memory://file*",
        mode="rb",
        **{protocol: {"cache_storage": d2, "same_names": True}},
    )
    with lurl as files:
        for f in files:
            assert os.path.basename(f.name) in ["file0", "file1"]
            assert f.read() == b"hello"
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    with lurl as files:
        for f in files:
            assert os.path.basename(f.name) in ["file0", "file1"]
            assert f.read() == b"hello"


@pytest.mark.parametrize(
    "protocol", ["simplecache", "filecache", "blockcache", "cached"]
)
def test_multi_cat(protocol, ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    for fn in ("/file0", "/file1"):
        with fs.open(fn, "wb") as f:
            f.write(b"hello")

    d2 = tempfile.mkdtemp()
    fs = fsspec.filesystem(protocol, storage=d2, fs=fs)
    assert fs.cat("file*") == {"/file0": b"hello", "/file1": b"hello"}


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_multi_cache_chain(protocol):
    import zipfile

    d = tempfile.mkdtemp()
    fn = os.path.join(d, "test.zip")
    zipfile.ZipFile(fn, mode="w").open("test", "w").write(b"hello")

    with fsspec.open_files(f"zip://test::{protocol}::file://{fn}") as files:
        assert d not in files[0]._fileobj._file.name
        assert files[0].read() == b"hello"

    # special test contains "file:" string
    fn = os.path.join(d, "file.zip")
    zipfile.ZipFile(fn, mode="w").open("file", "w").write(b"hello")
    with fsspec.open_files(f"zip://file::{protocol}::file://{fn}") as files:
        assert d not in files[0]._fileobj._file.name
        assert files[0].read() == b"hello"


@pytest.mark.parametrize(
    "protocol", ["blockcache", "cached", "simplecache", "filecache"]
)
def test_strip(protocol):
    fs = fsspec.filesystem(protocol, target_protocol="memory")
    url1 = "memory://afile"
    assert fs._strip_protocol(url1) == "/afile"
    assert fs._strip_protocol(protocol + "://afile") == "/afile"
    assert fs._strip_protocol(protocol + "::memory://afile") == "/afile"


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_cached_write(protocol):
    d = tempfile.mkdtemp()
    ofs = fsspec.open_files(f"{protocol}::file://{d}/*.out", mode="wb", num=2)
    with ofs as files:
        for f in files:
            assert isinstance(f, LocalTempFile)
            f.write(b"data")
            fn = f.name

    assert sorted(os.listdir(d)) == ["0.out", "1.out"]
    assert not os.path.exists(fn)


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_cached_append_text(protocol):
    fn = "memory://afile"
    with fsspec.open(fn, "w") as f:
        f.write("hello")
    with fsspec.open(f"{protocol}::{fn}", mode="a") as f:
        assert isinstance(f.buffer, LocalTempFile)
        f.write("world")
    with fsspec.open(fn, "r") as f:
        assert f.read() == "helloworld"


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_cached_append_binary(protocol):
    fn = "memory://afile"
    with fsspec.open(fn, "wb") as f:
        f.write(b"hello")
    with fsspec.open(f"{protocol}::{fn}", mode="ab") as f:
        assert isinstance(f, LocalTempFile)
        f.write(b"world")
    with fsspec.open(fn, "rb") as f:
        assert f.read() == b"helloworld"


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_cached_update_text(protocol):
    fn = "memory://afile"
    with fsspec.open(fn, "w") as f:
        f.write("hello")
    with fsspec.open(f"{protocol}::{fn}", mode="r+") as f:
        assert isinstance(f.buffer, LocalTempFile)
        assert f.read() == "hello"
        f.seek(1)
        f.write("world")
    with fsspec.open(fn, "r") as f:
        assert f.read() == "hworld"


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_cached_update_binary(protocol):
    fn = "memory://afile"
    with fsspec.open(fn, "wb") as f:
        f.write(b"hello")
    with fsspec.open(f"{protocol}::{fn}", mode="r+b") as f:
        assert isinstance(f, LocalTempFile)
        assert f.read() == b"hello"
        f.seek(1)
        f.write(b"world")
    with fsspec.open(fn, "rb") as f:
        assert f.read() == b"hworld"


def test_expiry():
    import time

    d = tempfile.mkdtemp()
    fs = fsspec.filesystem("memory")
    fn = "/afile"
    fn0 = "memory://afile"
    data = b"hello"
    with fs.open(fn0, "wb") as f:
        f.write(data)

    fs = fsspec.filesystem(
        "filecache",
        fs=fs,
        cache_storage=d,
        check_files=False,
        expiry_time=0.1,
        same_names=True,
    )

    # get file
    assert fs._check_file(fn0) is False
    assert fs.open(fn0, mode="rb").read() == data
    start_time = fs._metadata.cached_files[-1][fn]["time"]

    # cache time..
    assert fs.last_cache - start_time < 0.19

    # cache should have refreshed
    time.sleep(0.01)

    # file should still be valid... re-read
    assert fs.open(fn0, mode="rb").read() == data
    detail, _ = fs._check_file(fn0)
    assert detail["time"] == start_time

    time.sleep(0.11)
    # file should still be invalid... re-read
    assert fs._check_file(fn0) is False
    assert fs.open(fn0, mode="rb").read() == data
    detail, _ = fs._check_file(fn0)
    assert detail["time"] - start_time > 0.09


def test_equality(tmpdir):
    """Test sane behaviour for equality and hashing.

    Make sure that different CachingFileSystem only test equal to each other
    when they should, and do not test equal to the filesystem they rely upon.
    Similarly, make sure their hashes differ when they should and are equal
    when they should not.

    Related: GitHub#577, GitHub#578
    """
    from fsspec.implementations.local import LocalFileSystem

    lfs = LocalFileSystem()
    dir1 = f"{tmpdir}/raspberry"
    dir2 = f"{tmpdir}/banana"
    cfs1 = CachingFileSystem(fs=lfs, cache_storage=dir1)
    cfs2 = CachingFileSystem(fs=lfs, cache_storage=dir2)
    cfs3 = CachingFileSystem(fs=lfs, cache_storage=dir2)
    assert cfs1 == cfs1
    assert cfs1 != cfs2
    assert cfs1 != cfs3
    assert cfs2 == cfs3
    assert cfs1 != lfs
    assert cfs2 != lfs
    assert cfs3 != lfs
    assert hash(lfs) != hash(cfs1)
    assert hash(lfs) != hash(cfs2)
    assert hash(lfs) != hash(cfs3)
    assert hash(cfs1) != hash(cfs2)
    assert hash(cfs1) != hash(cfs2)
    assert hash(cfs2) == hash(cfs3)


def test_str():
    """Test that the str representation refers to correct class."""
    from fsspec.implementations.local import LocalFileSystem

    lfs = LocalFileSystem()
    cfs = CachingFileSystem(fs=lfs)
    assert "CachingFileSystem" in str(cfs)


def test_getitems_errors(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, "afolder"))
    open(os.path.join(tmpdir, "afile"), "w").write("test")
    open(os.path.join(tmpdir, "afolder", "anotherfile"), "w").write("test2")
    m = fsspec.get_mapper(f"file://{tmpdir}")
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}

    # my code
    m2 = fsspec.get_mapper(f"simplecache::file://{tmpdir}")
    assert m2.getitems(["afile"], on_error="omit") == {"afile": b"test"}  # works
    assert m2.getitems(["afile", "bfile"], on_error="omit") == {
        "afile": b"test"
    }  # throws KeyError

    with pytest.raises(KeyError):
        m.getitems(["afile", "bfile"])
    out = m.getitems(["afile", "bfile"], on_error="return")
    assert isinstance(out["bfile"], KeyError)
    m = fsspec.get_mapper(f"file://{tmpdir}", missing_exceptions=())
    assert m.getitems(["afile", "bfile"], on_error="omit") == {"afile": b"test"}
    with pytest.raises(FileNotFoundError):
        m.getitems(["afile", "bfile"])


@pytest.mark.parametrize("temp_cache", [False, True])
def test_cache_dir_auto_deleted(temp_cache, tmpdir):
    import gc

    source = os.path.join(tmpdir, "source")
    afile = os.path.join(source, "afile")
    os.mkdir(source)
    open(afile, "w").write("test")

    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage="TMP" if temp_cache else os.path.join(tmpdir, "cache"),
        skip_instance_cache=True,  # Important to avoid fs itself being cached
    )

    cache_dir = fs.storage[-1]

    # Force cache to be created
    with fs.open(afile, "rb") as f:
        assert f.read(5) == b"test"

    # Confirm cache exists
    local = fsspec.filesystem("file")
    assert local.exists(cache_dir)

    # Delete file system
    del fs
    gc.collect()

    # Ensure cache has been deleted, if it is temporary
    if temp_cache:
        assert not local.exists(cache_dir)
    else:
        assert local.exists(cache_dir)


@pytest.mark.parametrize(
    "protocol", ["filecache", "blockcache", "cached", "simplecache"]
)
def test_cache_size(tmpdir, protocol):
    if win and protocol in {"blockcache", "cached"}:
        pytest.skip("Windows file locking affects blockcache size tests")

    source = os.path.join(tmpdir, "source")
    afile = os.path.join(source, "afile")
    os.mkdir(source)
    open(afile, "w").write("test")

    fs = fsspec.filesystem(protocol, target_protocol="file")
    empty_cache_size = fs.cache_size()

    # Create cache
    with fs.open(afile, "rb") as f:
        assert f.read(5) == b"test"
    single_file_cache_size = fs.cache_size()
    assert single_file_cache_size > empty_cache_size

    # Remove cached file but leave cache metadata file
    fs.pop_from_cache(afile)
    if win and protocol == "filecache":
        assert empty_cache_size < fs.cache_size()
    elif protocol != "simplecache":
        assert empty_cache_size < fs.cache_size() < single_file_cache_size
    else:
        # simplecache never stores metadata
        assert fs.cache_size() == single_file_cache_size

    # Completely remove cache
    fs.clear_cache()
    if protocol != "simplecache":
        assert fs.cache_size() == empty_cache_size
    else:
        # Whole cache directory has been deleted
        assert fs.cache_size() == 0


def test_spurious_directory_issue1410(tmpdir):
    import zipfile

    os.chdir(tmpdir)
    zipfile.ZipFile("dir.zip", mode="w").open("file.txt", "w").write(b"hello")
    fs = WholeFileCacheFileSystem(fs=ZipFileSystem("dir.zip"))

    assert len(os.listdir()) == 1
    with fs.open("/file.txt", "rb"):
        pass

    # There was a bug reported in issue #1410 in which a directory
    # would be created and the next assertion would fail.
    assert len(os.listdir()) == 1
    assert fs._parent("/any/path") == "any"  # correct for ZIP, which has no leading /


def test_write_transaction(tmpdir, m, monkeypatch):
    called = [0]
    orig = m.put

    def patched_put(*args, **kwargs):
        called[0] += 1
        orig(*args, **kwargs)

    monkeypatch.setattr(m, "put", patched_put)
    tmpdir = str(tmpdir)
    fs, _ = fsspec.core.url_to_fs("simplecache::memory://", cache_storage=tmpdir)
    with fs.transaction:
        fs.pipe("myfile", b"1")
        fs.pipe("otherfile", b"2")
        fs.pipe("deep/dir/otherfile", b"3")
        with fs.open("blarh", "wb") as f:
            f.write(b"ff")
        assert not m.find("")

        assert fs.info("otherfile")["size"] == 1
        assert fs.info("deep")["type"] == "directory"
        assert fs.isdir("deep")
        assert fs.ls("deep", detail=False) == ["/deep/dir"]

    assert m.cat("myfile") == b"1"
    assert m.cat("otherfile") == b"2"
    assert called[0] == 1  # copy was done in one go


def test_filecache_write(tmpdir, m):
    fs = fsspec.filesystem(
        "filecache", target_protocol="memory", cache_storage=str(tmpdir)
    )
    fn = "sample_file_in_mem.txt"
    data = "hello world from memory"
    with fs.open(fn, "w") as f:
        assert not m.exists(fn)
        f.write(data)

    assert m.cat(fn) == data.encode()
    assert fs.cat(fn) == data.encode()


def test_cache_protocol_is_preserved():
    fs = fsspec.filesystem("filecache", target_protocol="file")
    assert fs.protocol == "filecache"


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_local_temp_file_put_by_list2(protocol, mocker, tmp_path) -> None:
    fs = fsspec.filesystem(protocol, target_protocol="memory")

    spy_put = mocker.spy(fs.fs, "put")
    spy_isdir = mocker.spy(fs.fs, "isdir")

    with fs.open("memory://some/file.txt", mode="wb") as file:
        file.write(b"hello")

    # passed by list
    spy_put.assert_called_once_with([file.name], ["/some/file.txt"])
    # which avoids isdir() check
    spy_isdir.assert_not_called()


def test_simplecache_tokenization_independent_of_path():
    # check that the tokenization is independent of the path
    of0 = fsspec.open("simplecache::memory://foo/bar.txt")
    of1 = fsspec.open("simplecache::memory://baz/qux.txt")
    assert of0.path != of1.path
    assert of0.fs._fs_token_ == of1.fs._fs_token_
    assert of0.fs is of1.fs


def test_simplecache_instance_cache(instance_caches):
    # check that the simplecache instance cache does not grow with every unique path

    assert instance_caches.gather_counts() == {}

    # check that the cache does not grow with multiple paths
    fsspec.open("simplecache::memory://foo/bar.txt")
    fsspec.open("simplecache::memory://bar/baz.txt")
    fsspec.open("simplecache::memory://baz/qux.txt")
    fsspec.open("simplecache::file:///foo/bar.txt")
    fsspec.open("simplecache::memory://bar/baz.txt")
    fsspec.open("simplecache::https://example.com/")

    assert instance_caches.gather_counts() == {
        "simplecache": 3,
        "memory": 1,
        "file": 1,
        "http": 1,
    }


@pytest.mark.parametrize("protocol", ["filecache", "simplecache"])
def test_class_has_cat_file_and_cat_ranges(tmp_path, protocol):
    """Ensure _cat_file and _cat_ranges are available on the class, not just
    instances, so that external code inspecting ``type(fs)`` (e.g.
    universal_pathlib, zarr) can discover these capabilities.

    Regression test for https://github.com/fsspec/filesystem_spec/issues/2009
    """
    fs = fsspec.filesystem(
        protocol, target_protocol="memory", cache_storage=str(tmp_path)
    )
    for attr in ("_cat_file", "_cat_ranges"):
        assert hasattr(fs, attr), f"instance missing {attr}"
        assert hasattr(type(fs), attr), f"class missing {attr}"


@pytest.fixture(scope="module")
def slow_http_server():
    """A local HTTP server that streams a 1 MiB payload slowly.

    Downloads take long enough that staggered concurrent reads of the same
    URL overlap with an in-flight download, exercising the cache-write race
    of issue #639.
    """
    pytest.importorskip("aiohttp")

    payload = random.Random(42).randbytes(2**20)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            for i in range(0, len(payload), 2**16):
                try:
                    self.wfile.write(payload[i : i + 2**16])
                except (BrokenPipeError, ConnectionResetError):
                    return
                time.sleep(0.005)

        def log_message(self, format, *args):
            pass

    class QuietServer(ThreadingHTTPServer):
        def handle_error(self, request, client_address):
            pass

    server = QuietServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}/data", payload
    server.shutdown()


def _staggered_async_cat_file(protocol, url, cache_dir, n):
    # readers must start while a download is still in flight: simultaneous
    # starts all miss the cache and write identical bytes to identical
    # offsets, which would hide the race
    async def run():
        fs = fsspec.filesystem(
            protocol,
            target_protocol="http",
            cache_storage=cache_dir,
            asynchronous=True,
            target_options={"asynchronous": True, "skip_instance_cache": True},
            skip_instance_cache=True,
        )

        async def one(i):
            await asyncio.sleep(i * 0.03)
            return await fs._cat_file(url)

        return await asyncio.gather(*[one(i) for i in range(n)])

    return asyncio.run(run())


def _staggered_threaded_cat_file(protocol, url, cache_dir, n):
    fs = fsspec.filesystem(
        protocol,
        target_protocol="http",
        cache_storage=cache_dir,
        target_options={"skip_instance_cache": True},
        skip_instance_cache=True,
    )

    def one(i):
        time.sleep(i * 0.03)
        return fs.cat_file(url)

    with ThreadPoolExecutor(n) as pool:
        return list(pool.map(one, range(n)))


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_concurrent_cat_file_async(slow_http_server, tmp_path, protocol):
    """Concurrent reads of one uncached URL must all see complete bytes.

    Regression test for https://github.com/fsspec/filesystem_spec/issues/639:
    cache misses were downloaded directly to the final cache filename, so a
    concurrent reader of the same key could observe (and return) a
    partially-written file.
    """
    url, payload = slow_http_server
    for trial in range(3):
        data = _staggered_async_cat_file(
            protocol, url, str(tmp_path / f"async{trial}"), 8
        )
        assert [len(d) for d in data] == [len(payload)] * 8
        assert all(d == payload for d in data)


def test_concurrent_cat_file_threads(slow_http_server, tmp_path):
    """Same as test_concurrent_cat_file_async, for the sync open() path."""
    url, payload = slow_http_server
    for trial in range(3):
        data = _staggered_threaded_cat_file(
            "simplecache", url, str(tmp_path / f"thr{trial}"), 8
        )
        assert [len(d) for d in data] == [len(payload)] * 8
        assert all(d == payload for d in data)


def test_concurrent_downloads_leave_no_tempfiles(slow_http_server, tmp_path):
    url, payload = slow_http_server
    cache_dir = str(tmp_path / "clean")
    _staggered_async_cat_file("simplecache", url, cache_dir, 4)
    entries = os.listdir(cache_dir)
    assert [fn for fn in entries if fn.endswith(".part")] == []
    assert len(entries) == 1


@pytest.mark.parametrize("failure_mode", ["before_write", "mid_write"])
def test_failed_download_cleans_up_tempfiles(tmp_path, monkeypatch, failure_mode):
    # a failing download must leave neither a .part temp file nor the final
    # cache filename behind, and the next read must succeed; "before_write"
    # covers cleanup of a temp path that was never created
    mem = fsspec.filesystem("memory")
    mem.pipe("/raw/data", b"0123456789")
    cache_dir = str(tmp_path / "fail")
    fs = fsspec.filesystem(
        "simplecache", fs=mem, cache_storage=cache_dir, skip_instance_cache=True
    )

    def boom(rpath, lpath, **kwargs):
        if failure_mode == "mid_write":
            with open(lpath, "wb") as f:
                f.write(b"0123")
        raise OSError("simulated download failure")

    with monkeypatch.context() as m:
        m.setattr(mem, "get_file", boom)
        with pytest.raises(OSError, match="simulated download failure"):
            with fs.open("/raw/data", "rb") as f:
                f.read()
    assert os.listdir(cache_dir) == []

    with fs.open("/raw/data", "rb") as f:
        assert f.read() == b"0123456789"


def test_stale_part_file_is_ignored(tmp_path):
    # a *.part file left behind by a hard kill mid-download must not be
    # mistaken for a cache entry
    mem = fsspec.filesystem("memory")
    mem.pipe("/raw/stale", b"real content")
    cache_dir = tmp_path / "stale"
    fs = fsspec.filesystem(
        "simplecache", fs=mem, cache_storage=str(cache_dir), skip_instance_cache=True
    )
    sha = fs._mapper("/raw/stale")
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{sha}.0123456789abcdef.part").write_bytes(b"garbage")

    with fs.open("/raw/stale", "rb") as f:
        assert f.read() == b"real content"


def test_simplecache_cat_ranges_cold_cache(tmp_path):
    # SimpleCacheFileSystem.cat_ranges compared _check_file() results with
    # ``is False``, but _check_file returns None for missing entries, so
    # uncached files were never downloaded and cat_ranges failed on a cold
    # cache
    mem = fsspec.filesystem("memory")
    mem.pipe("/raw/one", b"0123456789")
    mem.pipe("/raw/two", b"abcdefghij")
    fs = fsspec.filesystem(
        "simplecache",
        fs=mem,
        cache_storage=str(tmp_path / "cr"),
        skip_instance_cache=True,
    )
    out = fs.cat_ranges(["/raw/one", "/raw/two", "/raw/one"], [0, 2, 4], [4, 6, 8])
    assert out == [b"0123", b"cdef", b"4567"]


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_async_cat_ranges_cold_cache(slow_http_server, tmp_path, protocol):
    # _cat_ranges compared _check_file() results with ``is None``, but
    # filecache's _check_file returns False for missing entries, so uncached
    # files were never downloaded; and repeats of one path within a single
    # call (e.g. several ranges of one file) got no local path at all
    url, payload = slow_http_server

    async def run():
        fs = fsspec.filesystem(
            protocol,
            target_protocol="http",
            cache_storage=str(tmp_path / "cr"),
            asynchronous=True,
            target_options={"asynchronous": True, "skip_instance_cache": True},
            skip_instance_cache=True,
        )
        return await fs._cat_ranges([url, url], [0, 10], [10, 20])

    assert asyncio.run(run()) == [payload[0:10], payload[10:20]]


def _async_caching_fs(protocol, cache_dir):
    return fsspec.filesystem(
        protocol,
        target_protocol="http",
        cache_storage=cache_dir,
        asynchronous=True,
        target_options={"asynchronous": True, "skip_instance_cache": True},
        skip_instance_cache=True,
    )


def _count_downloads(fs):
    downloads = []
    inner_get_file = fs.fs._get_file

    async def counting_get_file(rpath, lpath, **kwargs):
        downloads.append(rpath)
        return await inner_get_file(rpath, lpath, **kwargs)

    fs.fs._get_file = counting_get_file
    return downloads


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_async_cat_file_downloads_once(slow_http_server, tmp_path, protocol):
    # filecache's _cat_file did not record the download in its metadata, so
    # every subsequent _cat_file was a fresh cache miss and downloaded the
    # file again; and once metadata was recorded (e.g. by cat()), the
    # (detail, fn) tuple from _check_file was passed straight to open().
    # simplecache never had the bug (existence-based _check_file) and is
    # included as a regression guard.
    url, payload = slow_http_server

    async def run():
        fs = _async_caching_fs(protocol, str(tmp_path / "once"))
        downloads = _count_downloads(fs)
        out = [await fs._cat_file(url), await fs._cat_file(url)]
        return out, len(downloads)

    out, n_downloads = asyncio.run(run())
    assert out == [payload, payload]
    assert n_downloads == 1


@pytest.mark.parametrize("protocol", ["simplecache", "filecache"])
def test_async_cat_ranges_downloads_once(slow_http_server, tmp_path, protocol):
    # same as test_async_cat_file_downloads_once, for _cat_ranges
    url, payload = slow_http_server
    url2 = url + "2"

    async def run():
        fs = _async_caching_fs(protocol, str(tmp_path / "ranges_once"))
        downloads = _count_downloads(fs)
        out1 = await fs._cat_ranges([url, url2, url], [0, 5, 10], [10, 15, 20])
        out2 = await fs._cat_ranges([url, url2], [0, 5], [10, 15])
        return out1, out2, len(downloads)

    out1, out2, n_downloads = asyncio.run(run())
    assert out1 == [payload[0:10], payload[5:15], payload[10:20]]
    assert out2 == [payload[0:10], payload[5:15]]
    assert n_downloads == 2


def test_replace_tempfile_busy_destination(tmp_path, monkeypatch):
    # on Windows, os.replace onto a destination another process holds open
    # raises PermissionError; an identical redundant temp copy must be
    # discarded, while a differing destination (cache refresh racing a
    # reader of the stale copy) or an absent one must still propagate
    def busy_replace(src, dst):
        raise PermissionError("destination is open in another process")

    monkeypatch.setattr(os, "replace", busy_replace)

    tmp, dst = tmp_path / "x.abcd1234.part", tmp_path / "x"
    tmp.write_bytes(b"same bytes")
    dst.write_bytes(b"same bytes")
    _replace_tempfile(str(tmp), str(dst))
    assert not tmp.exists()
    assert dst.read_bytes() == b"same bytes"

    tmp.write_bytes(b"fresh bytes")
    dst.write_bytes(b"stale bytes")
    with pytest.raises(PermissionError):
        _replace_tempfile(str(tmp), str(dst))
    assert tmp.exists()
    assert dst.read_bytes() == b"stale bytes"

    with pytest.raises(PermissionError):
        _replace_tempfile(str(tmp), str(tmp_path / "missing"))
    assert tmp.exists()


class _AsyncMemoryFileSystem(AsyncFileSystemWrapper):
    # gives the wrapper memory's protocol handling, so _strip_protocol is
    # NOT the identity ("memory://afile" -> "/afile") — unlike http's —
    # and the default AbstractFileSystem.ukey (no override) is exercised
    protocol = "memory"
    _strip_protocol = MemoryFileSystem._strip_protocol


def _async_mem_caching_fs(cache_dir, **kwargs):
    return fsspec.filesystem(
        "filecache",
        fs=_AsyncMemoryFileSystem(fs=fsspec.filesystem("memory")),
        cache_storage=cache_dir,
        skip_instance_cache=True,
        **kwargs,
    )


def test_filecache_async_nonidentity_strip_and_persistence(tmp_path):
    # exercises what the http-based tests cannot: a target whose
    # _strip_protocol is not the identity (metadata must be recorded under
    # the stripped path or every read is a miss), the default-ukey branch
    # of _ukey_async, and persistence via save_cache (a fresh instance
    # must hit the cache without downloading)
    mem = fsspec.filesystem("memory")
    mem.pipe("/afile", b"0123456789")
    cache_dir = str(tmp_path / "strip")

    async def run():
        fs = _async_mem_caching_fs(cache_dir)
        downloads = _count_downloads(fs)
        out1 = await fs._cat_ranges(
            ["memory://afile", "memory://afile"], [0, 4], [4, 8]
        )
        out2 = await fs._cat_file("memory://afile")
        n_first = len(downloads)

        fs2 = _async_mem_caching_fs(cache_dir)
        downloads2 = _count_downloads(fs2)
        out3 = await fs2._cat_file("memory://afile")
        return out1, out2, n_first, out3, len(downloads2)

    out1, out2, n_first, out3, n_second = asyncio.run(run())
    assert out1 == [b"0123", b"4567"]
    assert out2 == b"0123456789" == out3
    assert n_first == 1
    assert n_second == 0


def test_filecache_async_check_files(tmp_path):
    # check_files=True must work on the async paths: the uid recorded by
    # _make_local_details_async has to round-trip through
    # _check_file_async's async-safe comparison (a sync ukey call here
    # would fail inside the running loop), and a remote change must
    # trigger a re-download
    mem = fsspec.filesystem("memory")
    mem.pipe("/cfile", b"version one")

    async def run():
        fs = _async_mem_caching_fs(str(tmp_path / "cf"), check_files=True)
        downloads = _count_downloads(fs)
        first = await fs._cat_file("memory://cfile")
        again = await fs._cat_file("memory://cfile")
        mem.pipe("/cfile", b"version two, longer")
        refreshed = await fs._cat_file("memory://cfile")
        return first, again, refreshed, len(downloads)

    first, again, refreshed, n_downloads = asyncio.run(run())
    assert (first, again) == (b"version one", b"version one")
    assert refreshed == b"version two, longer"
    assert n_downloads == 2


def test_filecache_async_cat_ranges_on_error(tmp_path):
    # a missing remote path must be reported per-range with
    # on_error="return" (the default), not raised out of the whole call:
    # the metadata/ukey lookup for a miss runs inside the gathered
    # download so its failure is captured like a download failure
    mem = fsspec.filesystem("memory")
    mem.pipe("/exists", b"0123456789")

    async def run():
        fs = _async_mem_caching_fs(str(tmp_path / "oe"))
        returned = await fs._cat_ranges(
            ["memory://exists", "memory://missing"], [0, 0], [4, 4], on_error="return"
        )
        with pytest.raises(FileNotFoundError):
            await fs._cat_ranges(["memory://missing"], [0], [4], on_error="raise")
        return returned

    returned = asyncio.run(run())
    assert returned[0] == b"0123"
    assert isinstance(returned[1], Exception)
