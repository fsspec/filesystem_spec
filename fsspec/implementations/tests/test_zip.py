import collections.abc
import io
import os.path
import zipfile
from io import BytesIO
from pathlib import Path
from shutil import make_archive
from types import SimpleNamespace

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


@pytest.mark.parametrize("backend", ["memory", "file"])
def test_close_publishes_cached_archive(m, tmp_path, backend):
    path = "closed.zip" if backend == "memory" else str(tmp_path / "closed.zip")
    target = m if backend == "memory" else fsspec.filesystem("file")
    fs = ZipFileSystem(
        f"simplecache::{backend}://{path}",
        mode="w",
        target_options={"simplecache": {"cache_storage": str(tmp_path / "cache")}},
    )
    fs.pipe_file("data.jsonl", b'{"text":"example"}\n')
    fs.close()

    # Keep fs alive: publishing must not depend on its destructor.
    assert target.exists(path)
    with zipfile.ZipFile(io.BytesIO(target.cat_file(path))) as archive:
        assert archive.namelist() == ["data.jsonl"]
        assert archive.read("data.jsonl") == b'{"text":"example"}\n'
    assert fs.fo.closed
    fs.close()


def test_close_releases_open_file_context(m, tmp_path):
    of = fsspec.open(
        "simplecache::memory://context.zip",
        "wb",
        simplecache={"cache_storage": str(tmp_path)},
    )
    fs = ZipFileSystem(of, mode="w")
    fs.pipe_file("member", b"complete")
    fs.close()

    assert m.exists("context.zip")
    assert of.fobjects == []
    fs.close()


def test_close_releases_owned_read_handle(m):
    writer = ZipFileSystem("memory://read.zip", mode="w")
    writer.pipe_file("member", b"complete")
    writer.close()
    fs = ZipFileSystem("memory://read.zip")
    fs.close()

    assert fs.of.fobjects == []
    fs.close()


@pytest.mark.parametrize("backend", ["buffer", "file"])
def test_close_keeps_caller_file_open(tmp_path, backend):
    fo = io.BytesIO() if backend == "buffer" else open(tmp_path / "borrowed.zip", "w+b")
    with fo:
        fs = ZipFileSystem(fo, mode="w")
        fs.pipe_file("member", b"complete")
        fs.close()
        fs.close()

        assert not fo.closed
        fo.seek(0)
        with zipfile.ZipFile(fo) as archive:
            assert archive.read("member") == b"complete"


def test_close_with_open_member_can_be_retried(m, tmp_path):
    fs = ZipFileSystem(
        "simplecache::memory://retry.zip",
        mode="w",
        target_options={"simplecache": {"cache_storage": str(tmp_path)}},
    )
    with fs.open("member", "wb") as member:
        member.write(b"complete")
        with pytest.raises(ValueError, match="open writing handle"):
            fs.close()
        assert not fs.fo.closed
        assert not m.exists("retry.zip")

    fs.close()
    with zipfile.ZipFile(io.BytesIO(m.cat_file("retry.zip"))) as archive:
        assert archive.read("member") == b"complete"


def test_close_propagates_cached_upload_error(m, tmp_path, monkeypatch):
    fs = ZipFileSystem(
        "simplecache::memory://failed.zip",
        mode="w",
        target_options={"simplecache": {"cache_storage": str(tmp_path)}},
    )
    fs.pipe_file("member", b"complete")

    def fail_upload(*args, **kwargs):
        # The ZIP central directory must be finalized before upload is attempted.
        assert fs.zip.fp is None
        raise OSError("upload failed")

    monkeypatch.setattr(fs.fo, "commit", fail_upload)
    with pytest.raises(OSError, match="upload failed"):
        fs.close()
    assert not m.exists("failed.zip")


@pytest.mark.parametrize("prefix", ["", "/", "zip://", "zip:///"])
def test_pipe_file_normalizes_path(m, prefix):
    fs = fsspec.filesystem("zip", fo="memory://out.zip", mode="w")
    fs.pipe_file(f"{prefix}reports/result.csv", b"total\n12\n")
    fs.close()

    fs = fsspec.filesystem("zip", fo="memory://out.zip")
    assert fs.find("") == ["reports/result.csv"]
    assert fs.cat("reports/result.csv") == b"total\n12\n"
    fs.close()


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

    fn = f"{os.path.dirname(os.path.abspath(__file__))}/out.zip"
    fs = fsspec.filesystem("zip", fo=fn, mode="r")
    outfiles = fs.glob("*")
    assert len(outfiles) == 1


@pytest.mark.parametrize("backend", ["memory", "local"])
def test_append_creates_archive(m, tmp_path, backend):
    path = "memory://new.zip" if backend == "memory" else tmp_path / "new.zip"
    for name, content in [("first", b"original"), ("second", b"appended")]:
        fs = ZipFileSystem(fo=path, mode="a")
        try:
            fs.pipe_file(name, content)
        finally:
            fs.close()

    fs = ZipFileSystem(fo=path)
    try:
        assert fs.cat("first") == b"original"
        assert fs.cat("second") == b"appended"
        assert fs.find("") == ["first", "second"]
    finally:
        fs.close()


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


@pytest.fixture(name="zip_file2")
def zip_file_fixture2(tmp_path):
    file_path = tmp_path / "zip_file2.zip"

    with zipfile.ZipFile(file_path, "w") as z:
        z.writestr("a/b/c", "")
        z.writestr("a/b/d/e", "")

    return file_path


def _assert_all_except_context_dependent_variables(result, expected_result):
    for path in expected_result:
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


@pytest.mark.parametrize("prefix", ["/", "zip://", "zip:///"])
@pytest.mark.parametrize("detail", [False, True])
def test_find_returns_expected_result_path_set(zip_file, prefix, detail):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find(f"{prefix}dir2", detail=detail)
    expected_result = ["dir2/file3.txt"]

    assert list(result) == expected_result


def test_find_with_and_without_slash_should_return_same_result(zip_file):
    zip_file_system = ZipFileSystem(zip_file)

    assert zip_file_system.find("/dir2/") == zip_file_system.find("/dir2")


@pytest.mark.parametrize("prefix", ["/", "zip://", "zip:///"])
def test_find_should_return_file_if_exact_match(zip_file, prefix):
    zip_file_system = ZipFileSystem(zip_file)

    result = zip_file_system.find(f"{prefix}dir2startwithsamename.txt", detail=False)
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


@pytest.mark.parametrize(
    "args,expected_result",
    [
        pytest.param(
            ("a/b", 1),
            ["a/b/c"],
            id="find-maxdepth-correct-depth",
        ),
        pytest.param(
            ("a/b", None, True),
            ["a/b", "a/b/c", "a/b/d", "a/b/d/e"],
            id="find-withdirs-should-not-include-parents",
        ),
        pytest.param(
            ("a/b", 1, True),
            ["a/b", "a/b/c", "a/b/d"],
            id="find-withdirs-maxdepth",
        ),
        pytest.param(
            ("/a//b///", 1, True),
            ["a/b", "a/b/c", "a/b/d"],
            id="find-ill-formed-path",
        ),
        pytest.param(
            ("\\a\\\\b\\", 1, True),
            ["a/b", "a/b/c", "a/b/d"],
            id="find-ill-formed-path-windows",
        ),
        pytest.param(
            (Path("\\a\\\\b\\"), 1, True),
            ["a/b", "a/b/c", "a/b/d"],
            id="find-using-pathobj",
        ),
    ],
)
def test_find_generic(zip_file2, args, expected_result):
    zip_file_system = ZipFileSystem(zip_file2)
    assert zip_file_system.find(*args) == expected_result


@pytest.fixture
def zip_with_one_member(tmp_path):
    path = tmp_path / "archive.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("present.txt", "data")
    return path


@pytest.mark.parametrize(
    "read",
    [
        lambda fs: fs.open("missing.txt").read(),
        lambda fs: fs.cat("missing.txt"),
        lambda fs: fs.cat_file("missing.txt"),
        lambda fs: fs.head("missing.txt", 1),
    ],
    ids=["open", "cat", "cat_file", "head"],
)
def test_reading_missing_member_raises_file_not_found(zip_with_one_member, read):
    fs = ZipFileSystem(str(zip_with_one_member))
    with pytest.raises(FileNotFoundError):
        read(fs)


def test_mapper_default_for_missing_member(zip_with_one_member):
    # FSMap only substitutes the default for its missing_exceptions, which do
    # not include KeyError, so a leaked KeyError ignored the default.
    mapper = fsspec.get_mapper(f"zip://::file://{zip_with_one_member.as_posix()}")
    assert mapper.__getitem__("missing.txt", default=b"fallback") == b"fallback"
    with pytest.raises(KeyError):
        mapper["missing.txt"]


@pytest.mark.parametrize("method", ["pipe_file", "pipe"])
@pytest.mark.parametrize("mode", ["create", "overwrite"])
@pytest.mark.parametrize("existing", [False, True])
def test_pipe_write_modes(method, mode, existing):
    with BytesIO() as buffer:
        fs = ZipFileSystem(buffer, mode="w")
        try:
            if existing:
                fs.pipe_file("member", b"original")
            write = getattr(fs, method)
            if existing and mode == "create":
                with pytest.raises(FileExistsError):
                    write("member", b"replacement", mode=mode)
                expected = b"original"
            else:
                if existing:
                    with pytest.warns(UserWarning, match="Duplicate name"):
                        write("member", b"replacement", mode=mode)
                else:
                    write("member", b"replacement", mode=mode)
                expected = b"replacement"
        finally:
            fs.close()

        buffer.seek(0)
        with zipfile.ZipFile(buffer) as archive:
            assert archive.read("member") == expected
            assert archive.namelist() == (
                ["member", "member"] if existing and mode == "overwrite" else ["member"]
            )


def test_pipe_invalid_write_mode_preserves_contents():
    with BytesIO() as buffer:
        fs = ZipFileSystem(buffer, mode="w")
        try:
            fs.pipe_file("member", b"original")

            with pytest.raises(ValueError, match="Unsupported ZIP write mode"):
                fs.pipe_file("member", b"replacement", mode="append")
        finally:
            fs.close()

        buffer.seek(0)
        with zipfile.ZipFile(buffer) as archive:
            assert archive.read("member") == b"original"
            assert archive.namelist() == ["member"]


@pytest.mark.parametrize(
    "member,path,separator",
    [
        ("member", "member\x00ignored", "/"),
        ("dir/member", "dir\\member", "\\"),
    ],
)
def test_pipe_create_checks_stored_member_name(monkeypatch, member, path, separator):
    # Exercise ZipInfo name normalization without changing the global os module.
    zip_os = SimpleNamespace(**{**vars(os), "sep": separator})
    monkeypatch.setattr(zipfile, "os", zip_os)
    with BytesIO() as buffer:
        fs = ZipFileSystem(buffer, mode="w")
        try:
            fs.pipe_file(member, b"original")

            with pytest.raises(FileExistsError):
                fs.pipe_file(path, b"replacement", mode="create")
        finally:
            fs.close()

        buffer.seek(0)
        with zipfile.ZipFile(buffer) as archive:
            assert archive.read(member) == b"original"
            assert archive.namelist() == [member]


def test_pipe_create_preserves_existing_archive_member():
    with BytesIO() as buffer:
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("member", b"original")
        buffer.seek(0)
        fs = ZipFileSystem(buffer, mode="a")
        try:
            with pytest.raises(FileExistsError):
                fs.pipe_file("member", b"replacement", mode="create")
        finally:
            fs.close()

        buffer.seek(0)
        with zipfile.ZipFile(buffer) as archive:
            assert archive.read("member") == b"original"
            assert archive.namelist() == ["member"]
