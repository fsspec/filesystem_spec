import collections.abc

import fsspec
from fsspec.implementations.tests.test_archive import archive_data, tempzip


def test_info():
    with tempzip(archive_data) as z:
        fs = fsspec.filesystem("zip", fo=z)

        # Iterate over all files.
        for f, v in archive_data.items():
            lhs = fs.info(f)

            # Probe some specific fields of Zip archives.
            assert "CRC" in lhs
            assert "compress_size" in lhs


def test_fsspec_get_mapper():
    """Added for #788"""

    with tempzip(archive_data) as z:
        mapping = fsspec.get_mapper(f"zip::{z}")

        assert isinstance(mapping, collections.abc.Mapping)
        keys = sorted(list(mapping.keys()))
        assert keys == ["a", "b", "deeply/nested/path"]

        # mapping.getitems() will call FSMap.fs.cat()
        # which was not accurately implemented for zip.
        assert isinstance(mapping, fsspec.mapping.FSMap)
        items = dict(mapping.getitems(keys))
        assert items == {"a": b"", "b": b"hello", "deeply/nested/path": b"stuff"}


def test_not_cached():
    with tempzip(archive_data) as z:
        fs = fsspec.filesystem("zip", fo=z)
        fs2 = fsspec.filesystem("zip", fo=z)
        assert fs is not fs2


def test_root_info():
    with tempzip(archive_data) as z:
        fs = fsspec.filesystem("zip", fo=z)
        assert fs.info("/") == {"name": "/", "type": "directory", "size": 0}
        assert fs.info("") == {"name": "/", "type": "directory", "size": 0}


def test_write_seek(m):
    with m.open("afile.zip", "wb") as f:
        fs = fsspec.filesystem("zip", fo=f, mode="w")
        fs.pipe("another", b"hi")
        fs.zip.close()

    with m.open("afile.zip", "rb") as f:
        fs = fsspec.filesystem("zip", fo=f)
        assert fs.cat("another") == b"hi"
