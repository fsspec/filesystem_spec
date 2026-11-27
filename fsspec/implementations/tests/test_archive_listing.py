from io import BytesIO

import pytest

import fsspec
from fsspec.implementations.tests.test_archive import temptar, tempzip


@pytest.mark.parametrize("protocol", ["zip", "tar"])
def test_archive_parent_listing_index(protocol):
    data = {"root.txt": b"root", "a/one": b"1", "a/two": b"22", "b/deep/three": b"333"}
    context = tempzip(data) if protocol == "zip" else temptar(data)
    with context as filename:
        fs = fsspec.filesystem(protocol, fo=filename)
        try:
            assert fs.ls("", detail=False) == ["a", "b", "root.txt"]
            assert fs.ls("a", detail=False) == ["a/one", "a/two"]
            assert fs.ls("a/", detail=False) == ["a/one", "a/two"]
            assert fs.ls("b/deep", detail=False) == ["b/deep/three"]
            assert fs.ls("root.txt", detail=False) == ["root.txt"]
            assert [entry["size"] for entry in fs.ls("a")] == [1, 2]
            with pytest.raises(FileNotFoundError):
                fs.ls("missing")
            walked = list(fs.walk(""))
            assert sum(len(files) for _, _, files in walked) == len(data)
        finally:
            fs.close()


def test_zip_parent_listing_rebuilt_after_write():
    buffer = BytesIO()
    fs = fsspec.filesystem("zip", fo=buffer, mode="w")
    try:
        fs.pipe_file("a/first", b"one")
        assert fs.ls("a", detail=False) == ["a/first"]
        fs.pipe_file("a/second", b"two")
        fs.pipe_file("b/third", b"three")
        assert fs.ls("a", detail=False) == ["a/first", "a/second"]
        assert fs.ls("", detail=False) == ["a", "b"]
    finally:
        fs.close()


def test_archive_walk_does_not_rescan_members_for_each_directory():
    from fsspec.archive import AbstractArchiveFileSystem

    visited = 0

    class CountedMembers(dict):
        def items(self):
            nonlocal visited
            for item in super().items():
                visited += 1
                yield item

    class Archive(AbstractArchiveFileSystem):
        root_marker = ""

        def _get_dirs(self):
            pass

    fs = Archive(skip_instance_cache=True)
    fs.dir_cache = CountedMembers()
    count = 256
    for i in range(count):
        directory = f"dir-{i:03}"
        fs.dir_cache[directory] = {"name": directory, "type": "directory", "size": 0}
        name = directory + "/part"
        fs.dir_cache[name] = {"name": name, "type": "file", "size": 1}
    result = list(fs.walk(""))
    assert len(result) == count + 1
    assert sum(len(files) for _, _, files in result) == count
    assert visited <= 2 * len(fs.dir_cache)
