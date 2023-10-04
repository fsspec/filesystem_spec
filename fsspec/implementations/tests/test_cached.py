import json
import os
import pickle
import shutil
import tempfile

import pytest

import fsspec
from fsspec.compression import compr
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cache_mapper import (
    BasenameCacheMapper,
    HashCacheMapper,
    create_cache_mapper,
)
from fsspec.implementations.cached import CachingFileSystem, LocalTempFile
from fsspec.implementations.local import make_path_posix
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
@pytest.mark.parametrize("force_save_pickle", [True, False])
def test_metadata(tmpdir, cache_mapper, force_save_pickle):
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
    fs._metadata._force_save_pickle = force_save_pickle

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


def test_metadata_replace_pickle_with_json(tmpdir):
    # For backward compatibility will allow reading of old pickled metadata.
    # When the metadata is next saved, it is in json format.
    source = os.path.join(tmpdir, "source")
    afile = os.path.join(source, "afile")
    os.mkdir(source)
    open(afile, "w").write("test")

    # Save metadata in pickle format, to simulate old metadata
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=os.path.join(tmpdir, "cache"),
    )
    fs._metadata._force_save_pickle = True
    with fs.open(afile, "rb") as f:
        assert f.read(5) == b"test"

    # Confirm metadata is in pickle format
    cache_fn = os.path.join(fs.storage[-1], "cache")
    with open(cache_fn, "rb") as f:
        metadata = pickle.load(f)
    assert list(metadata.keys()) == [make_path_posix(afile)]

    # Force rewrite of metadata, now in json format
    fs._metadata._force_save_pickle = False
    fs.pop_from_cache(afile)
    with fs.open(afile, "rb") as f:
        assert f.read(5) == b"test"

    # Confirm metadata is in json format
    with open(cache_fn, "r") as f:
        metadata = json.load(f)
    assert list(metadata.keys()) == [make_path_posix(afile)]


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


def test_idempotent():
    fs = CachingFileSystem("file")
    fs2 = CachingFileSystem("file")
    assert fs2 is fs
    fs3 = pickle.loads(pickle.dumps(fs))
    assert fs3.storage == fs.storage


@pytest.mark.parametrize("force_save_pickle", [True, False])
def test_blockcache_workflow(ftp_writable, tmp_path, force_save_pickle):
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
    fs._metadata._force_save_pickle = force_save_pickle
    with fs.open("/out", "rb", block_size=5) as f:
        assert f.read(5) == b"test\n"

    # Save the cache/close it
    fs.save_cache()
    del fs

    # Check that cache file only has the first two blocks
    if force_save_pickle:
        with open(tmp_path / "cache", "rb") as f:
            cache = pickle.load(f)
    else:
        with open(tmp_path / "cache", "r") as f:
            cache = json.load(f)
    assert "/out" in cache
    assert cache["/out"]["blocks"] == [0, 1]

    # Reopen the same cache and read some more...
    fs = fsspec.filesystem("blockcache", **fs_kwargs)
    fs._metadata._force_save_pickle = force_save_pickle
    with fs.open("/out", block_size=5) as f:
        assert f.read(5) == b"test\n"
        f.seek(30)
        assert f.read(5) == b"test\n"


@pytest.mark.parametrize("impl", ["filecache", "blockcache"])
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

    assert fs.cat("/out") == b"test"  # old value


@pytest.mark.parametrize("impl", ["simplecache", "blockcache"])
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


@pytest.mark.parametrize("force_save_pickle", [True, False])
def test_clear_expired(tmp_path, force_save_pickle):
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
            if force_save_pickle:
                with open(cache_fn, "rb") as f:
                    cached_files = pickle.load(f)
            else:
                with open(cache_fn, "r") as f:
                    cached_files = json.load(f)
            fn_posix = pathlib.Path(fn).as_posix()
            cached_files[fn_posix]["time"] = cached_files[fn_posix]["time"] - 691200
            assert os.access(cache_fn, os.W_OK), "Cache is not writable"
            if del_fn:
                del cached_files[fn_posix]["fn"]
            if force_save_pickle:
                with open(cache_fn, "wb") as f:
                    pickle.dump(cached_files, f)
            else:
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
    fs._metadata._force_save_pickle = force_save_pickle
    assert fs.cat(str(f1)) == data

    # populates "last" cache if file not found in first one
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=[str(cache1), str(cache2)],
        cache_check=1,
    )
    fs._metadata._force_save_pickle = force_save_pickle
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
    fs._metadata._force_save_pickle = force_save_pickle
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
    fs._metadata._force_save_pickle = force_save_pickle
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


def test_write_pickle_context():
    tmp = str(tempfile.mkdtemp())
    fn = tmp + "afile"
    url = f"simplecache::file://{fn}"
    with fsspec.open(url, "wb") as f:
        pickle.loads(pickle.dumps(f))
        f.write(b"hello ")
        pickle.dumps(f)

    with pytest.raises(ValueError):
        pickle.dumps(f)

    assert open(fn, "rb").read() == b"hello "


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


@pytest.mark.parametrize("impl", ["filecache", "simplecache", "blockcache"])
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
    fn = list(fs._metadata.cached_files[-1].values())[0]["fn"]  # this is a hash value
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
    fn = list(fs._metadata.cached_files[-1].values())[0]["fn"]  # this is a hash value
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
    fn = list(new_fs._metadata.cached_files[-1].values())[0][
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


@pytest.mark.parametrize("impl", ["filecache", "simplecache", "blockcache"])
@pytest.mark.parametrize("fs", ["local", "multi"], indirect=["fs"])
def test_takes_fs_instance(impl, fs):
    origin = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    fs2 = fsspec.filesystem(impl, fs=fs)

    assert fs2.cat(f1) == data


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


@pytest.mark.parametrize("protocol", ["simplecache", "filecache", "blockcache"])
def test_multi_cat(protocol, ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    for fn in {"/file0", "/file1"}:
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


@pytest.mark.parametrize("protocol", ["blockcache", "simplecache", "filecache"])
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


@pytest.mark.parametrize("protocol", ["filecache", "blockcache", "simplecache"])
def test_cache_size(tmpdir, protocol):
    if win and protocol == "blockcache":
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
        empty_cache_size < fs.cache_size()
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
