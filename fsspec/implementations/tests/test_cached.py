import os
import shutil
import pickle
import pytest

import fsspec
from fsspec.implementations.cached import CachingFileSystem
from .test_ftp import ftp_writable, FTPFileSystem


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

    return (data, original_file, cache_location, fs)


def test_idempotent():
    fs = CachingFileSystem("file")
    fs2 = CachingFileSystem("file")
    assert fs2 is fs
    fs3 = pickle.loads(pickle.dumps(fs))
    assert fs3.storage == fs.storage


def test_worflow(ftp_writable):
    host, port, user, pw = ftp_writable
    fs = FTPFileSystem(host, port, user, pw)
    with fs.open("/out", "wb") as f:
        f.write(b"test")
    fs = fsspec.filesystem(
        "cached",
        target_protocol="ftp",
        target_options={"host": host, "port": port, "username": user, "password": pw},
    )
    assert os.listdir(fs.storage[-1]) == []
    with fs.open("/out") as f:
        assert os.listdir(fs.storage[-1])
        assert f.read() == b"test"
        assert fs.cached_files[-1]["ftp:///out"]["blocks"]
    assert fs.cat("/out") == b"test"
    assert fs.cached_files[-1]["ftp:///out"]["blocks"] is True

    with fs.open("/out", "wb") as f:
        f.write(b"changed")

    assert fs.cat("/out") == b"test"  # old value


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
    with pytest.raises(ValueError):
        fs.open("/out_block", block_size=30)


def test_local_filecache_creates_dir_if_needed():
    import tempfile

    original_location = tempfile.mkdtemp()
    cache_location = "foofoobarbar"
    assert not os.path.exists(cache_location)

    try:
        original_file = os.path.join(original_location, "afile")
        data = b"test data"
        with open(original_file, "wb") as f:
            f.write(data)

        # we can access the file and read it
        fs = fsspec.filesystem(
            "filecache", target_protocol="file", cache_storage=cache_location
        )

        with fs.open(original_file, "rb") as f:
            data_in_cache = f.read()

        assert os.path.exists(cache_location)

    finally:
        shutil.rmtree(cache_location)

    assert data_in_cache == data


def test_local_filecache_basic(local_filecache):
    data, original_file, cache_location, fs = local_filecache

    # reading from the file contains the right data
    with fs.open(original_file, "rb") as f:
        assert f.read() == data
    assert "cache" in os.listdir(cache_location)

    # the file in the location contains the right data
    fn = list(fs.cached_files[-1].values())[0]["fn"]  # this is a hash value
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
    fn = list(fs.cached_files[-1].values())[0]["fn"]  # this is a hash value
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
    fn = list(new_fs.cached_files[-1].values())[0]["fn"]  # this is a hash value
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
    assert len(os.listdir(cache2)) == 0  # hasn't been intialized yet

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


def test_filecache_multicache_with_same_file_different_data_reads_from_first():
    import tempfile

    origin = tempfile.mkdtemp()
    cache1 = tempfile.mkdtemp()
    cache2 = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    # populate first cache
    fs = fsspec.filesystem("filecache", target_protocol="file", cache_storage=cache1)
    assert fs.cat(f1) == data

    with open(f1, "wb") as f:
        f.write(data * 2)

    # populate second cache
    fs = fsspec.filesystem("filecache", target_protocol="file", cache_storage=cache2)

    assert fs.cat(f1) == data * 2

    # the filenames in each cache are the same, but the data is different
    assert os.listdir(cache1) == os.listdir(cache2)

    fs = fsspec.filesystem(
        "filecache", target_protocol="file", cache_storage=[cache1, cache2]
    )

    assert fs.cat(f1) == data


def test_filecache_with_checks():
    import tempfile
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
    time.sleep(0.11)  # allow cache details to expire
    assert fs.cat(f1) == data * 2  # changed, since origin changed


def test_takes_fs_instance():
    import tempfile

    origin = tempfile.mkdtemp()
    data = b"test data"
    f1 = os.path.join(origin, "afile")
    with open(f1, "wb") as f:
        f.write(data)

    fs = fsspec.filesystem("file")
    fs2 = fsspec.filesystem("filecache", target_protocol=fs)

    assert fs2.cat(f1) == data
