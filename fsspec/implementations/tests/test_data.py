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


@pytest.mark.parametrize(
    "metadata",
    [
        "text/plain;name=base64",
        "text/base64",
        "text/plain;base64=1",
        "text/plain;baſe64",
        "text/plain;\u00a0base64",
        "text/plain;base64\u00a0",
        "text/plain;\tbase64",
    ],
)
def test_base64_in_media_type_is_not_encoding(metadata):
    uri = f"data:{metadata},SGVsbG8="
    with fsspec.open(uri, "rb") as stream:
        assert stream.read() == b"SGVsbG8="
    fs, path = fsspec.core.url_to_fs(uri)
    assert fs.info(path)["size"] == 8


@pytest.mark.parametrize(
    "data, expected", [("SGVsbG8%3D", b"Hello"), ("%2Fw%3D%3D", b"\xff")]
)
def test_percent_encoded_base64(data, expected):
    uri = f"data:application/octet-stream;base64,{data}"
    with fsspec.open(uri, "rb") as stream:
        assert stream.read() == expected
    fs, path = fsspec.core.url_to_fs(uri)
    assert fs.info(path)["size"] == len(expected)
    assert fs.cat_file(path, start=-1) == expected[-1:]


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("data:text/plain,Hello", b"Hello"),
        ("data:text/plain;base64,SGVsbG8h", b"Hello!"),
        ("data:text/plain,Hello%23world", b"Hello#world"),
        ("data:text/plain,Hello?world", b"Hello?world"),
        ("data:text/plain;base64,SGVsbG8%3D", b"Hello"),
        ("data:,", b""),
    ],
)
@pytest.mark.parametrize("fragment", ["#", "#AAAA", "#part#two"])
def test_data_url_fragment(uri, expected, fragment):
    uri += fragment
    fs, path = fsspec.core.url_to_fs(uri)
    assert fs.cat_file(uri) == expected
    assert fs.cat_file(path, start=1, end=4) == expected[1:4]
    assert fs.cat_file(path, start=-2) == expected[-2:]
    assert fs.info(path)["size"] == len(expected)
    with fsspec.open(uri, "rb") as stream:
        assert stream.read() == expected


@pytest.mark.parametrize(
    "marker",
    [";base64", ";BASE64", ";bAsE64", "; base64", ";  BASE64", ";base64 \t\n\r\f"],
)
def test_base64_marker_case_and_spaces(marker):
    uri = f"data:text/plain{marker},SGVsbG8%3D"
    fs, path = fsspec.core.url_to_fs(uri)
    assert fs.cat_file(path) == b"Hello"
    assert fs.cat_file(path, start=1, end=4) == b"ell"
    assert fs.info(path)["size"] == 5
    with fsspec.open(uri, "rb") as stream:
        assert stream.read() == b"Hello"
