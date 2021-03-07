# this test case checks that the libarchive can be used from a seekable source (any fs
# with a block cache active)
import fsspec
from fsspec.implementations.tests.test_archive import temparchive, archive_data


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
