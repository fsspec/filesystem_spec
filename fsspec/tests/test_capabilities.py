import pytest

import fsspec


@pytest.mark.parametrize("protocol", ["file", "memory"])
def test_default_capabilities(protocol, tmp_path):
    fs = fsspec.filesystem(protocol, skip_instance_cache=True)
    assert fs.read_only is False
    assert fs.immutable is False

    path = str(tmp_path / "data")
    try:
        fs.pipe_file(path, b"first")
        assert fs.cat_file(path) == b"first"
        fs.pipe_file(path, b"second")
        assert fs.cat_file(path) == b"second"
    finally:
        fs.rm(path)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("data:,Hello%2C%20World%21", b"Hello, World!"),
        ("data:application/octet-stream;base64,AP+A", b"\x00\xff\x80"),
        ("data:,", b""),
        ("data:,Hello%23world#fragment", b"Hello#world"),
    ],
)
def test_data_url_capabilities(url, expected):
    fs, path = fsspec.core.url_to_fs(url)
    assert fs.read_only is True
    assert fs.immutable is True

    with fs.open(path) as stream:
        assert stream.read() == expected
    with pytest.raises(ValueError, match="Read only filesystem"):
        fs.pipe_file(path, b"changed")
    assert fs.cat_file(path) == expected
    assert fs.info(path)["size"] == len(expected)
