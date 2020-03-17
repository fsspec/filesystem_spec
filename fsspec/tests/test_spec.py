import pickle
import json

import pytest
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
