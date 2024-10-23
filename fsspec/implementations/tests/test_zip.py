import collections.abc
import os.path
from pathlib import Path
from shutil import make_archive

import pytest

import fsspec
from fsspec.implementations.tests.test_archive import archive_data, tempzip
from fsspec.implementations.zip import ZipFileSystem


def test_info():
    with tempzip(archive_data) as z:
        fs = fsspec.filesystem("zip", fo=z)

        # Iterate over all files.
        for f in archive_data:
            lhs = fs.info(f)

            # Probe some specific fields of Zip archives.
            assert "CRC" in lhs
            assert "compress_size" in lhs


def test_fsspec_get_mapper():
    """Added for #788"""

    with tempzip(archive_data) as z:
        mapping = fsspec.get_mapper(f"zip::{z}")

        assert isinstance(mapping, collections.abc.Mapping)
        keys = sorted(mapping.keys())
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
        assert fs.info("/") == {"name": "", "type": "directory", "size": 0}
        assert fs.info("") == {"name": "", "type": "directory", "size": 0}


def test_write_seek(m):
    with m.open("afile.zip", "wb") as f:
        fs = fsspec.filesystem("zip", fo=f, mode="w")
        fs.pipe("another", b"hi")
        fs.zip.close()

    with m.open("afile.zip", "rb") as f:
        fs = fsspec.filesystem("zip", fo=f)
        assert fs.cat("another") == b"hi"


def test_rw(m):
    # extra arg to zip means "create archive"
    with fsspec.open(
        "zip://afile::memory://out.zip", mode="wb", zip={"mode": "w"}
    ) as f:
        f.write(b"data")

    with fsspec.open("zip://afile::memory://out.zip", mode="rb") as f:
        assert f.read() == b"data"


def test_mapper(m):
    # extra arg to zip means "create archive"
    mapper = fsspec.get_mapper("zip::memory://out.zip", zip={"mode": "w"})
    with pytest.raises(KeyError):
        mapper["a"]

    mapper["a"] = b"data"
    with pytest.raises(OSError):
        # fails because this is write mode and we cannot also read
        mapper["a"]
    assert "a" in mapper  # but be can list


def test_zip_glob_star(m):
    with fsspec.open(
        "zip://adir/afile::memory://out.zip", mode="wb", zip={"mode": "w"}
    ) as f:
        f.write(b"data")

    fs, _ = fsspec.core.url_to_fs("zip::memory://out.zip")
    outfiles = fs.glob("*")
    assert len(outfiles) == 1

    fs = fsspec.filesystem("zip", fo="memory://out.zip", mode="w")
    fs.mkdir("adir")
    fs.pipe("adir/afile", b"data")
    outfiles = fs.glob("*")
    assert len(outfiles) == 1

    fn = f"{os.path.dirname(os.path.abspath((__file__)))}/out.zip"
    fs = fsspec.filesystem("zip", fo=fn, mode="r")
    outfiles = fs.glob("*")
    assert len(outfiles) == 1


def test_append(m, tmpdir):
    fs = fsspec.filesystem("zip", fo="memory://out.zip", mode="w")
    with fs.open("afile", "wb") as f:
        f.write(b"data")
    fs.close()

    fs = fsspec.filesystem("zip", fo="memory://out.zip", mode="a")
    with fs.open("bfile", "wb") as f:
        f.write(b"data")
    fs.close()

    assert len(fsspec.open_files("zip://*::memory://out.zip")) == 2

    fs = fsspec.filesystem("zip", fo=f"{tmpdir}/out.zip", mode="w")
    with fs.open("afile", "wb") as f:
        f.write(b"data")
    fs.close()

    fs = fsspec.filesystem("zip", fo=f"{tmpdir}/out.zip", mode="a")
    with fs.open("bfile", "wb") as f:
        f.write(b"data")
    fs.close()

    assert len(fsspec.open_files("zip://*::memory://out.zip")) == 2


@pytest.fixture(name="zip_file")
def zip_file_fixture(tmp_path):
    data_dir = tmp_path / "data/"
    data_dir.mkdir()
    file1 = data_dir / "file1.txt"
    file1.write_text("Hello, World!")
    file2 = data_dir / "file2.txt"
    file2.write_text("Lorem ipsum dolor sit amet")

    empty_dir = data_dir / "dir1"
    empty_dir.mkdir()

    dir_with_files = data_dir / "dir2"
    dir_with_files.mkdir()
    file3 = dir_with_files / "file3.txt"
    file3.write_text("Hello!")

    potential_mix_up_path = data_dir / "dir2startwithsamename.txt"
    potential_mix_up_path.write_text("Hello again!")

    zip_file = tmp_path / "test"
    return Path(make_archive(zip_file, "zip", data_dir))


def _assert_all_except_context_dependent_variables(result, expected_result):
    for path in expected_result.keys():
        assert result[path]
        fields = [
            "orig_filename",
            "filename",
            "compress_type",
            "comment",
            "extra",
            "CRC",
            "compress_size",
            "file_size",
            "name",
            "size",
            "type",
        ]

        result_without_date_time = {k: result[path][k] for k in fields}

        expected_result_without_date_time = {
            k: expected_result[path][k] for k in fields
        }
        assert result_without_date_time == expected_result_without_date_time


def test_find_returns_expected_result_detail_true(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find("/", detail=True)

    expected_result = {
        "dir2/file3.txt": {
            "orig_filename": "dir2/file3.txt",
            "filename": "dir2/file3.txt",
            "date_time": (2024, 8, 16, 10, 46, 18),
            "compress_type": 8,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 2175008768,
            "header_offset": 260,
            "CRC": 2636827734,
            "compress_size": 8,
            "file_size": 6,
            "_raw_time": 21961,
            "_end_offset": 312,
            "name": "dir2/file3.txt",
            "size": 6,
            "type": "file",
        },
        "file1.txt": {
            "orig_filename": "file1.txt",
            "filename": "file1.txt",
            "date_time": (2024, 8, 16, 10, 46, 18),
            "compress_type": 8,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 2175008768,
            "header_offset": 139,
            "CRC": 3964322768,
            "compress_size": 15,
            "file_size": 13,
            "_raw_time": 21961,
            "_end_offset": 193,
            "name": "file1.txt",
            "size": 13,
            "type": "file",
        },
        "file2.txt": {
            "orig_filename": "file2.txt",
            "filename": "file2.txt",
            "date_time": (2024, 8, 16, 10, 46, 18),
            "compress_type": 8,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 2175008768,
            "header_offset": 193,
            "CRC": 1596576865,
            "compress_size": 28,
            "file_size": 26,
            "_raw_time": 21961,
            "_end_offset": 260,
            "name": "file2.txt",
            "size": 26,
            "type": "file",
        },
    }

    _assert_all_except_context_dependent_variables(result, expected_result)


def test_find_returns_expected_result_detail_false(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find("/", detail=False)
    expected_result = [
        "dir2/file3.txt",
        "dir2startwithsamename.txt",
        "file1.txt",
        "file2.txt",
    ]

    assert result == expected_result


def test_find_returns_expected_result_detail_true_include_dirs(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find("/", detail=True, withdirs=True)
    expected_result = {
        "dir1": {
            "orig_filename": "dir1/",
            "filename": "dir1/",
            "date_time": (2024, 8, 16, 10, 54, 24),
            "compress_type": 0,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 1106051088,
            "header_offset": 0,
            "CRC": 0,
            "compress_size": 0,
            "file_size": 0,
            "_raw_time": 22220,
            "_end_offset": 35,
            "name": "dir1",
            "size": 0,
            "type": "directory",
        },
        "dir2": {
            "orig_filename": "dir2/",
            "filename": "dir2/",
            "date_time": (2024, 8, 16, 10, 54, 24),
            "compress_type": 0,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 1106051088,
            "header_offset": 35,
            "CRC": 0,
            "compress_size": 0,
            "file_size": 0,
            "_raw_time": 22220,
            "_end_offset": 70,
            "name": "dir2",
            "size": 0,
            "type": "directory",
        },
        "dir2/file3.txt": {
            "orig_filename": "dir2/file3.txt",
            "filename": "dir2/file3.txt",
            "date_time": (2024, 8, 16, 10, 54, 24),
            "compress_type": 8,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 2175008768,
            "header_offset": 260,
            "CRC": 2636827734,
            "compress_size": 8,
            "file_size": 6,
            "_raw_time": 22220,
            "_end_offset": 312,
            "name": "dir2/file3.txt",
            "size": 6,
            "type": "file",
        },
        "file1.txt": {
            "orig_filename": "file1.txt",
            "filename": "file1.txt",
            "date_time": (2024, 8, 16, 10, 54, 24),
            "compress_type": 8,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 2175008768,
            "header_offset": 139,
            "CRC": 3964322768,
            "compress_size": 15,
            "file_size": 13,
            "_raw_time": 22220,
            "_end_offset": 193,
            "name": "file1.txt",
            "size": 13,
            "type": "file",
        },
        "file2.txt": {
            "orig_filename": "file2.txt",
            "filename": "file2.txt",
            "date_time": (2024, 8, 16, 10, 54, 24),
            "compress_type": 8,
            "_compresslevel": None,
            "comment": b"",
            "extra": b"",
            "create_system": 3,
            "create_version": 20,
            "extract_version": 20,
            "reserved": 0,
            "flag_bits": 0,
            "volume": 0,
            "internal_attr": 0,
            "external_attr": 2175008768,
            "header_offset": 193,
            "CRC": 1596576865,
            "compress_size": 28,
            "file_size": 26,
            "_raw_time": 22220,
            "_end_offset": 260,
            "name": "file2.txt",
            "size": 26,
            "type": "file",
        },
    }

    _assert_all_except_context_dependent_variables(result, expected_result)


def test_find_returns_expected_result_detail_false_include_dirs(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find("/", detail=False, withdirs=True)
    expected_result = [
        "dir1",
        "dir2",
        "dir2/file3.txt",
        "dir2startwithsamename.txt",
        "file1.txt",
        "file2.txt",
    ]

    assert result == expected_result


def test_find_returns_expected_result_path_set(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find("/dir2")
    expected_result = ["dir2/file3.txt"]

    assert result == expected_result


def test_find_with_and_without_slash_should_return_same_result(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    assert zip_file_system.find("/dir2/") == zip_file_system.find("/dir2")


def test_find_should_return_file_if_exact_match(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find("/dir2startwithsamename.txt", detail=False)
    expected_result = ["dir2startwithsamename.txt"]

    assert result == expected_result


def test_find_returns_expected_result_recursion_depth_set(zip_file):
    zip_file_system = ZipFileSystem(zip_file)
    result = zip_file_system.find("/", maxdepth=1)

    expected_result = [
        "dir2startwithsamename.txt",
        "file1.txt",
        "file2.txt",
    ]

    assert result == expected_result
