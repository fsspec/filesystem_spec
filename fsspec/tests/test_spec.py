import pytest
from fsspec.spec import AbstractFileSystem


class DummyTestFS(AbstractFileSystem):
    protocol = "mock"
    _fs_contents = (
        {"name": "top_level/second_level/date=2019-10-01/", "type": "directory"},
        {"name": "top_level/second_level/date=2019-10-01/a.parquet", "type": "file"},
        {"name": "top_level/second_level/date=2019-10-01/b.parquet", "type": "file"},
        {"name": "top_level/second_level/date=2019-10-02/", "type": "directory"},
        {"name": "top_level/second_level/date=2019-10-02/a.parquet", "type": "file"},
        {"name": "top_level/second_level/date=2019-10-04/", "type": "directory"},
        {"name": "top_level/second_level/date=2019-10-04/a.parquet", "type": "file"},
        {"name": "misc/", "type": "directory"},
        {"name": "misc/foo.txt", "type": "file"},
    )

    def ls(self, path, detail=True, **kwargs):
        files = (file for file in self._fs_contents if path in file["name"])

        if detail:
            return list(files)

        return list(sorted([file["name"] for file in files]))


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

    assert test_fs.glob(test_path) == expected


def test_add_docs_warns():
    with pytest.warns(FutureWarning, match="add_docs"):
        AbstractFileSystem(add_docs=True)


def test_exists():
    """ Test calling `exists` and `info` on partially completed file/directory names."""
    test_fs = DummyTestFS()

    assert test_fs.exists("top_level/second")
    assert test_fs.exists("top_level/second_level/date/")
    assert test_fs.exists("top_level/second_level/date=2019-10-01/a.parq")

    info = test_fs.info("top_level/second_level/date=2019-10-01/a.parq")
    assert info["type"] == "directory"
