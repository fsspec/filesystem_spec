# this test case checks that the libarchive can be used from a seekable source (any fs
# with a block cache active)
import stat
import tarfile
from io import BytesIO

import pytest

import fsspec
from fsspec.implementations.tests.test_archive import archive_data, temparchive


@pytest.mark.parametrize("prefix", ["./", "././", "//./"])
def test_dot_components(prefix):
    pytest.importorskip("libarchive")
    data = BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        root = tarfile.TarInfo(prefix)
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
        directory = tarfile.TarInfo(prefix + "directory/")
        directory.type = tarfile.DIRTYPE
        directory.mode = 0o711
        archive.addfile(directory)
        for name, payload in [
            (prefix + "file", b"prefixed"),
            (prefix + "directory/./file", b"nested"),
            (prefix + "empty", b""),
            ("plain", b"plain"),
            (".hidden", b"hidden"),
        ]:
            member = tarfile.TarInfo(name)
            member.size = len(payload)
            archive.addfile(member, BytesIO(payload))
    data.seek(0)
    fs = fsspec.filesystem("libarchive", fo=data)

    assert fs.ls("", detail=False) == [".hidden", "directory", "empty", "file", "plain"]
    assert fs.find("") == [".hidden", "directory/file", "empty", "file", "plain"]
    assert fs.ls("libarchive://./directory/", detail=False) == ["directory/file"]
    assert stat.S_IMODE(fs.info("./directory/")["mode"]) == 0o711
    assert fs.glob("directory/*") == ["directory/file"]
    assert fs.cat_file("file") == fs.cat_file(prefix + "file") == b"prefixed"
    assert fs.cat_file("libarchive:///./directory/file") == b"nested"
    assert fs.cat_file("empty") == b""
    assert fs.cat_file("plain") == b"plain"
    assert fs.cat_file(".hidden") == b"hidden"


@pytest.mark.parametrize("prefix", ["./", "././", "//./"])
def test_explicit_root_is_not_listed(prefix):
    pytest.importorskip("libarchive")
    data = BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        root = tarfile.TarInfo(prefix)
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
    data.seek(0)
    fs = fsspec.filesystem("libarchive", fo=data)
    assert fs.info("")["type"] == "directory"
    assert fs.ls("", detail=False) == []
    assert fs.ls("", detail=True) == []


@pytest.mark.parametrize("names", [("file", "./file"), ("./file", "file")])
@pytest.mark.parametrize("last_payload", [b"last", b""])
def test_normalized_member_collision(names, last_payload):
    pytest.importorskip("libarchive")
    data = BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name, payload in zip(names, [b"first", last_payload]):
            member = tarfile.TarInfo(name)
            member.size = len(payload)
            archive.addfile(member, BytesIO(payload))
    data.seek(0)
    fs = fsspec.filesystem("libarchive", fo=data)
    assert fs.ls("", detail=False) == ["file"]
    assert fs.info("file")["size"] == len(last_payload)
    assert fs.cat_file("file") == last_payload


@pytest.mark.parametrize(
    "name, path", [("./../file", "../file"), ("libarchive://file", "libarchive:/file")]
)
def test_literal_components(name, path):
    pytest.importorskip("libarchive")
    data = BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        member = tarfile.TarInfo(name)
        member.size = len(b"literal")
        archive.addfile(member, BytesIO(b"literal"))
    data.seek(0)
    fs = fsspec.filesystem("libarchive", fo=data)
    assert fs.info(path)["name"] == path
    assert fs.cat_file(path) == b"literal"
    with pytest.raises(FileNotFoundError):
        fs.cat_file("file")


@pytest.mark.parametrize(
    "payloads",
    [
        [b"", b"last"],
        [b"first", b""],
        [b"first", b"", b"last"],
        [b"first", b"last"],
        [b"", b""],
    ],
    ids=["empty_first", "empty_last", "empty_middle", "nonempty", "all_empty"],
)
def test_duplicate_members_with_empty_files(payloads):
    pytest.importorskip("libarchive")
    data = BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for payload in payloads:
            member = tarfile.TarInfo("file")
            member.size = len(payload)
            archive.addfile(member, BytesIO(payload))
    data.seek(0)
    fs = fsspec.filesystem("libarchive", fo=data)
    expected = payloads[-1]
    assert fs.info("file")["size"] == len(expected)
    assert fs.cat_file("file") == expected
    with fs.open("file", "rb") as stream:
        assert stream.read() == expected


@pytest.mark.parametrize("directory_first", [True, False])
def test_explicit_directory_metadata(directory_first):
    pytest.importorskip("libarchive")
    archive_data = BytesIO()
    directory = tarfile.TarInfo("directory")
    directory.type = tarfile.DIRTYPE
    directory.mode = 0o711
    directory.uid = 123
    directory.gid = 456
    directory.mtime = 123456789
    child = tarfile.TarInfo("directory/file")
    child.size = len(b"payload")
    with tarfile.open(fileobj=archive_data, mode="w") as archive:
        if directory_first:
            archive.addfile(directory)
        archive.addfile(child, BytesIO(b"payload"))
        if not directory_first:
            archive.addfile(directory)
    archive_data.seek(0)
    fs = fsspec.filesystem("libarchive", fo=archive_data)
    info = fs.info("directory")
    assert info["name"] == "directory"
    assert info["type"] == "directory"
    assert stat.S_IMODE(info["mode"]) == 0o711
    assert info["uid"] == 123
    assert info["gid"] == 456
    assert info["mtime"] == 123456789
    assert fs.info("directory/") == info
    assert fs.ls("") == [info]
    assert fs.cat_file("directory/file") == b"payload"


def test_cache(ftp_writable):
    host, port, username, password = "localhost", 2121, "user", "pass"

    with temparchive(archive_data) as archive_file:
        with fsspec.open(
            "ftp:///archive.7z",
            "wb",
            host=host,
            port=port,
            username=username,
            password=password,
        ) as f:
            f.write(open(archive_file, "rb").read())
        of = fsspec.open(
            "libarchive://deeply/nested/path::ftp:///archive.7z",
            ftp={
                "host": host,
                "port": port,
                "username": username,
                "password": password,
            },
        )

        with of as f:
            readdata = f.read()

        assert readdata == archive_data["deeply/nested/path"]
