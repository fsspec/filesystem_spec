import os
import pickle
import shutil
import tempfile

import pytest

import fsspec
from fsspec.compression import compr
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cached import CachingFileSystem, LocalTempFile

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


def test_idempotent():
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

    fs_kwargs = dict(
        skip_instance_cache=True,
        cache_storage=str(tmp_path),
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )

    # Open the blockcache and read a little bit of the data
    fs = fsspec.filesystem("blockcache", **fs_kwargs)
    with fs.open("/out", "rb", block_size=5) as f:
        assert f.read(5) == b"test\n"

    # Save the cache/close it
    fs.save_cache()
    del fs

    # Check that cache file only has the first two blocks
    with open(tmp_path / "cache", "rb") as f:
        cache = pickle.load(f)
        assert "/out" in cache
        assert cache["/out"]["blocks"] == [0, 1]

    # Reopen the same cache and read some more...
    fs = fsspec.filesystem("blockcache", **fs_kwargs)
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
        assert fs.cached_files[-1]["/out"]["blocks"]
    assert fs.cat("/out") == b"test"
    assert fs.cached_files[-1]["/out"]["blocks"] is True

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
    url = "simplecache::file://" + fn
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


def test_clear_expired():
    import tempfile

    def __ager(cache_fn, fn):
        """
        Modify the cache file to virtually add time lag to selected files.

        Parameters
        ---------
        cache_fn: str
            cache path
        fn: str
            file name to be modified
        """
        import pathlib
        import time

        if os.path.exists(cache_fn):
            with open(cache_fn, "rb") as f:
                cached_files = pickle.load(f)
                fn_posix = pathlib.Path(fn).as_posix()
                cached_files[fn_posix]["time"] = cached_files[fn_posix]["time"] - 691200
            assert os.access(cache_fn, os.W_OK), "Cache is not writable"
            with open(cache_fn, "wb") as f:
                pickle.dump(cached_files, f)
            time.sleep(1)

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    cache2 = tempfile.mkdtemp()
    cache3 = tempfile.mkdtemp()

    data = b"test data"
    f1 = os.path.join(origin, "afile")
    f2 = os.path.join(origin, "bfile")
    f3 = os.path.join(origin, "cfile")
    f4 = os.path.join(origin, "dfile")

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
        "filecache", target_protocol="file", cache_storage=cache1, cache_check=1
    )
    assert fs.cat(f1) == data

    # populates "last" cache if file not found in first one
    fs = fsspec.filesystem(
        "filecache",
        target_protocol="file",
        cache_storage=[cache1, cache2],
        cache_check=1,
    )
    assert fs.cat(f2) == data
    assert fs.cat(f3) == data
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
        cache_storage=[cache1, cache2, cache3],
        same_names=True,
        cache_check=1,
    )
    assert fs.cat(f4) == data

    cache_fn = os.path.join(fs.storage[-1], "cache")
    __ager(cache_fn, f4)

    fs.clear_expired_cache()
    assert not fs._check_file(f4)

    shutil.rmtree(origin)
    shutil.rmtree(cache1)
    shutil.rmtree(cache2)
    shutil.rmtree(cache3)
