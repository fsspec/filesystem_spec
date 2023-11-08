import fsspec


def test_1():
    with fsspec.open("data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==") as f:
        assert f.read() == b"Hello, World!"

    with fsspec.open("data:,Hello%2C%20World%21") as f:
        assert f.read() == b"Hello, World!"
