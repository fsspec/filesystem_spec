import json
import pickle

import numpy as np
import pytest

import fsspec
from fsspec.implementations.ftp import FTPFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile


class DummyTestFS(AbstractFileSystem):
    protocol = "mock"
    _fs_contents = (
        {"name": "top_level", "type": "directory"},
        {"name": "top_level/second_level", "type": "directory"},
        {"name": "top_level/second_level/date=2019-10-01", "type": "directory"},
        {
            "name": "top_level/second_level/date=2019-10-01/a.parquet",
            "type": "file",
            "size": 100,
        },
        {
            "name": "top_level/second_level/date=2019-10-01/b.parquet",
            "type": "file",
            "size": 100,
        },
        {"name": "top_level/second_level/date=2019-10-02", "type": "directory"},
        {
            "name": "top_level/second_level/date=2019-10-02/a.parquet",
            "type": "file",
            "size": 100,
        },
        {"name": "top_level/second_level/date=2019-10-04", "type": "directory"},
        {
            "name": "top_level/second_level/date=2019-10-04/a.parquet",
            "type": "file",
            "size": 100,
        },
        {"name": "misc", "type": "directory"},
        {"name": "misc/foo.txt", "type": "file", "size": 100},
        {"name": "glob_test", "type": "directory", "size": 0},
        {"name": "glob_test/hat", "type": "directory", "size": 0},
        {"name": "glob_test/hat/^foo.txt", "type": "file", "size": 100},
        {"name": "glob_test/dollar", "type": "directory", "size": 0},
        {"name": "glob_test/dollar/$foo.txt", "type": "file", "size": 100},
        {"name": "glob_test/lbrace", "type": "directory", "size": 0},
        {"name": "glob_test/lbrace/{foo.txt", "type": "file", "size": 100},
        {"name": "glob_test/rbrace", "type": "directory", "size": 0},
        {"name": "glob_test/rbrace/}foo.txt", "type": "file", "size": 100},
    )

    def __getitem__(self, name):
        for item in self._fs_contents:
            if item["name"] == name:
                return item
        raise IndexError("{name} not found!".format(name=name))

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)

        files = {
            file["name"]: file
            for file in self._fs_contents
            if path == self._parent(file["name"])
        }

        if detail:
            return [files[name] for name in sorted(files)]

        return list(sorted(files))

    @classmethod
    def get_test_paths(cls, start_with=""):
        """Helper to return directory and file paths with no details"""
        all = [
            file["name"]
            for file in cls._fs_contents
            if file["name"].startswith(start_with)
        ]
        return all


@pytest.mark.parametrize(
    "test_path, expected",
    [
        (
            "mock://top_level/second_level/date=2019-10-01/a.parquet",
            ["top_level/second_level/date=2019-10-01/a.parquet"],
        ),
        (
            "mock://top_level/second_level/date=2019-10-01/*",
            [
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
            ],
        ),
        ("mock://top_level/second_level/date=2019-10", []),
        (
            "mock://top_level/second_level/date=2019-10-0[1-4]",
            [
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-04",
            ],
        ),
        (
            "mock://top_level/second_level/date=2019-10-0[1-4]/*",
            [
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            "mock://top_level/second_level/date=2019-10-0[1-4]/[a].*",
            [
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        ("mock://glob_test/hat/^foo.*", ["glob_test/hat/^foo.txt"]),
        ("mock://glob_test/dollar/$foo.*", ["glob_test/dollar/$foo.txt"]),
        ("mock://glob_test/lbrace/{foo.*", ["glob_test/lbrace/{foo.txt"]),
        ("mock://glob_test/rbrace/}foo.*", ["glob_test/rbrace/}foo.txt"]),
    ],
)
def test_glob(test_path, expected):
    test_fs = DummyTestFS()
    res = test_fs.glob(test_path)
    res = sorted(res)  # FIXME: py35 back-compat
    assert res == expected
    res = test_fs.glob(test_path, detail=True)
    assert isinstance(res, dict)
    assert sorted(res) == expected  # FIXME: py35 back-compat
    for name, info in res.items():
        assert info == test_fs[name]


@pytest.mark.parametrize(
    ["test_paths", "expected"],
    [
        (
            ("top_level/second_level", "top_level/sec*", "top_level/*"),
            [
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (("misc/foo.txt", "misc/*.txt"), ["misc/foo.txt"]),
        (
            ("",),
            DummyTestFS.get_test_paths() + [DummyTestFS.root_marker],
        ),
    ],
    # ids=["all_second_level", "single_file"],
)
def test_expand_path_recursive(test_paths, expected):
    """Test a number of paths and then their combination which should all yield
    the same set of expanded paths"""
    test_fs = DummyTestFS()

    # test single query
    for test_path in test_paths:
        paths = test_fs.expand_path(test_path, recursive=True)
        assert sorted(paths) == sorted(expected)

    # test with all queries
    paths = test_fs.expand_path(list(test_paths), recursive=True)
    assert sorted(paths) == sorted(expected)


def test_find():
    """ Test .find() method on debian server (ftp, https) with constant folder """
    filesystem, host, test_path = (
        FTPFileSystem,
        "ftp.fau.de",
        "ftp://ftp.fau.de/debian-cd/current/amd64/log/success",
    )
    test_fs = filesystem(host)
    filenames_ftp = test_fs.find(test_path)

    filesystem, host, test_path = (
        HTTPFileSystem,
        "https://ftp.fau.de",
        "https://ftp.fau.de/debian-cd/current/amd64/log/success",
    )
    test_fs = filesystem()
    filenames_http = test_fs.find(test_path)
    roots = [f.rsplit("/", 1)[-1] for f in filenames_http]

    assert all(f.rsplit("/", 1)[-1] in roots for f in filenames_ftp)


def test_find_details():
    test_fs = DummyTestFS()
    filenames = test_fs.find("/")
    details = test_fs.find("/", detail=True)
    for filename in filenames:
        assert details[filename] == test_fs.info(filename)


def test_cache():
    fs = DummyTestFS()
    fs2 = DummyTestFS()
    assert fs is fs2

    assert len(fs._cache) == 1
    del fs2
    assert len(fs._cache) == 1
    del fs
    assert len(DummyTestFS._cache) == 1

    DummyTestFS.clear_instance_cache()
    assert len(DummyTestFS._cache) == 0


def test_alias():
    with pytest.warns(FutureWarning, match="add_aliases"):
        DummyTestFS(add_aliases=True)


def test_add_docs_warns():
    with pytest.warns(FutureWarning, match="add_docs"):
        AbstractFileSystem(add_docs=True)


def test_cache_options():
    fs = DummyTestFS()
    f = AbstractBufferedFile(fs, "misc/foo.txt", cache_type="bytes")
    assert f.cache.trim

    # TODO: dummy buffered file
    f = AbstractBufferedFile(
        fs, "misc/foo.txt", cache_type="bytes", cache_options=dict(trim=False)
    )
    assert f.cache.trim is False

    f = fs.open("misc/foo.txt", cache_type="bytes", cache_options=dict(trim=False))
    assert f.cache.trim is False


def test_trim_kwarg_warns():
    fs = DummyTestFS()
    with pytest.warns(FutureWarning, match="cache_options"):
        AbstractBufferedFile(fs, "misc/foo.txt", cache_type="bytes", trim=False)


def test_eq():
    fs = DummyTestFS()
    result = fs == 1
    assert result is False


def test_pickle_multiple():
    a = DummyTestFS(1)
    b = DummyTestFS(2, bar=1)

    x = pickle.dumps(a)
    y = pickle.dumps(b)

    del a, b
    DummyTestFS.clear_instance_cache()

    result = pickle.loads(x)
    assert result.storage_args == (1,)
    assert result.storage_options == {}

    result = pickle.loads(y)
    assert result.storage_args == (2,)
    assert result.storage_options == dict(bar=1)


def test_json():
    a = DummyTestFS(1)
    b = DummyTestFS(2, bar=1)

    outa = a.to_json()
    outb = b.to_json()

    assert json.loads(outb)  # is valid JSON
    assert a != b
    assert "bar" in outb

    assert DummyTestFS.from_json(outa) is a
    assert DummyTestFS.from_json(outb) is b


@pytest.mark.parametrize(
    "dt",
    [
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.float32,
        np.float64,
    ],
)
def test_readinto_with_numpy(tmpdir, dt):
    store_path = str(tmpdir / "test_arr.npy")
    arr = np.arange(10, dtype=dt)
    arr.tofile(store_path)

    arr2 = np.empty_like(arr)
    with fsspec.open(store_path, "rb") as f:
        f.readinto(arr2)

    assert np.array_equal(arr, arr2)


@pytest.mark.parametrize(
    "dt",
    [
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.float32,
        np.float64,
    ],
)
def test_readinto_with_multibyte(ftp_writable, tmpdir, dt):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    with ftp.open("/out", "wb") as fp:
        arr = np.arange(10, dtype=dt)
        fp.write(arr.tobytes())

    with ftp.open("/out", "rb") as fp:
        arr2 = np.empty_like(arr)
        fp.readinto(arr2)

    assert np.array_equal(arr, arr2)
