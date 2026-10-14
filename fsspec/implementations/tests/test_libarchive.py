# this test case checks that the libarchive can be used from a seekable source (any fs
# with a block cache active)
import stat
import tarfile
from io import BytesIO

import pytest

import fsspec
from fsspec.implementations.tests.test_archive import archive_data, temparchive


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
