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
