import fsspec


def test_1():
    with fsspec.open("data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==") as f:
        assert f.read() == b"Hello, World!"

    with fsspec.open("data:,Hello%2C%20World%21") as f:
        assert f.read() == b"Hello, World!"


def test_info():
    fs = fsspec.filesystem("data")
    info = fs.info("data:text/html,%3Ch1%3EHello%2C%20World%21%3C%2Fh1%3E")
    assert info == {
        "name": "%3Ch1%3EHello%2C%20World%21%3C%2Fh1%3E",
        "size": 22,
        "type": "file",
        "mimetype": "text/html",
    }
