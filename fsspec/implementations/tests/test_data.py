from urllib.parse import quote_from_bytes

import pytest

import fsspec


def test_1():
    with fsspec.open("data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==") as f:
        assert f.read() == b"Hello, World!"

    with fsspec.open("data:,Hello%2C%20World%21") as f:
        assert f.read() == b"Hello, World!"

    # Trailing slashed should not be stripped
    with fsspec.open("data:text/plain;base64,YWI/") as f:
        assert f.read() == b"ab?"

    with fsspec.open("data:text/plain,/") as f:
        assert f.read() == b"/"


def test_info():
    fs = fsspec.filesystem("data")
    info = fs.info("data:text/html,%3Ch1%3EHello%2C%20World%21%3C%2Fh1%3E")
    assert info == {
        "name": "%3Ch1%3EHello%2C%20World%21%3C%2Fh1%3E",
        "size": 22,
        "type": "file",
        "mimetype": "text/html",
    }


def test_info_without_protocol():
    # url_to_fs and fsspec.open hand the filesystem paths with "data:" removed
    fs, path = fsspec.core.url_to_fs("data:text/plain;base64,SGk=")
    assert path == "text/plain;base64,SGk="
    assert fs.info(path)["mimetype"] == "text/plain"
    assert fs.size(path) == 2
    assert fs.exists(path)

    fs, path = fsspec.core.url_to_fs("data:,Hello")
    assert fs.info(path)["mimetype"] == ""
    assert fs.size(path) == 5


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("data:application/octet-stream,%FF%00%80", b"\xff\x00\x80"),
        ("data:text/plain;charset=iso-8859-1,%E9", b"\xe9"),
        ("data:text/plain;charset=utf-8,%C3%A9", b"\xc3\xa9"),
        ("data:,Hello%2C%20World%21", b"Hello, World!"),
        ("data:,%25+%2f", b"%+/"),
        ("data:,", b""),
    ],
)
def test_percent_encoded_bytes(uri, expected):
    with fsspec.open(uri, "rb") as f:
        assert f.read() == expected

    fs, path = fsspec.core.url_to_fs(uri)
    assert fs.info(path)["size"] == len(expected)
    assert fs.cat_file(path, start=1, end=3) == expected[1:3]
    assert fs.cat_file(path, start=-2) == expected[-2:]


def test_percent_encoded_all_byte_values():
    data = bytes(range(256))
    uri = "data:application/octet-stream," + quote_from_bytes(data)
    with fsspec.open(uri, "rb") as f:
        assert f.read() == data
