import io
import pytest
import sys
from fsspec.utils import infer_storage_options, seek_delimiter, read_block


WIN = "win" in sys.platform


def test_read_block():
    delimiter = b"\n"
    data = delimiter.join([b"123", b"456", b"789"])
    f = io.BytesIO(data)

    assert read_block(f, 1, 2) == b"23"
    assert read_block(f, 0, 1, delimiter=b"\n") == b"123\n"
    assert read_block(f, 0, 2, delimiter=b"\n") == b"123\n"
    assert read_block(f, 0, 3, delimiter=b"\n") == b"123\n"
    assert read_block(f, 0, 5, delimiter=b"\n") == b"123\n456\n"
    assert read_block(f, 0, 8, delimiter=b"\n") == b"123\n456\n789"
    assert read_block(f, 0, 100, delimiter=b"\n") == b"123\n456\n789"
    assert read_block(f, 1, 1, delimiter=b"\n") == b""
    assert read_block(f, 1, 5, delimiter=b"\n") == b"456\n"
    assert read_block(f, 1, 8, delimiter=b"\n") == b"456\n789"

    for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)], [(0, 4), (4, 4), (8, 4)]]:
        out = [read_block(f, o, l, b"\n") for o, l in ols]
        assert b"".join(filter(None, out)) == data


def test_read_block_split_before():
    """Test start/middle/end cases of split_before."""  # noqa: I
    d = (
        "#header" + "".join(">foo{i}\nFOOBAR{i}\n".format(i=i) for i in range(100000))
    ).encode()

    # Read single record at beginning.
    # All reads include beginning of file and read through termination of
    # delimited record.
    assert read_block(io.BytesIO(d), 0, 10, delimiter=b"\n") == b"#header>foo0\n"
    assert (
        read_block(io.BytesIO(d), 0, 10, delimiter=b"\n", split_before=True)
        == b"#header>foo0"
    )
    assert (
        read_block(io.BytesIO(d), 0, 10, delimiter=b">") == b"#header>foo0\nFOOBAR0\n>"
    )
    assert (
        read_block(io.BytesIO(d), 0, 10, delimiter=b">", split_before=True)
        == b"#header>foo0\nFOOBAR0\n"
    )

    # Read multiple records at beginning.
    # All reads include beginning of file and read through termination of
    # delimited record.
    assert (
        read_block(io.BytesIO(d), 0, 27, delimiter=b"\n")
        == b"#header>foo0\nFOOBAR0\n>foo1\nFOOBAR1\n"
    )
    assert (
        read_block(io.BytesIO(d), 0, 27, delimiter=b"\n", split_before=True)
        == b"#header>foo0\nFOOBAR0\n>foo1\nFOOBAR1"
    )
    assert (
        read_block(io.BytesIO(d), 0, 27, delimiter=b">")
        == b"#header>foo0\nFOOBAR0\n>foo1\nFOOBAR1\n>"
    )
    assert (
        read_block(io.BytesIO(d), 0, 27, delimiter=b">", split_before=True)
        == b"#header>foo0\nFOOBAR0\n>foo1\nFOOBAR1\n"
    )

    # Read with offset spanning into next record, splits on either side of delimiter.
    # Read not spanning the full record returns nothing.
    assert read_block(io.BytesIO(d), 10, 3, delimiter=b"\n") == b"FOOBAR0\n"
    assert (
        read_block(io.BytesIO(d), 10, 3, delimiter=b"\n", split_before=True)
        == b"\nFOOBAR0"
    )
    assert read_block(io.BytesIO(d), 10, 3, delimiter=b">") == b""
    assert read_block(io.BytesIO(d), 10, 3, delimiter=b">", split_before=True) == b""

    # Read with offset spanning multiple records, splits on either side of delimiter
    assert (
        read_block(io.BytesIO(d), 10, 20, delimiter=b"\n")
        == b"FOOBAR0\n>foo1\nFOOBAR1\n"
    )
    assert (
        read_block(io.BytesIO(d), 10, 20, delimiter=b"\n", split_before=True)
        == b"\nFOOBAR0\n>foo1\nFOOBAR1"
    )
    assert read_block(io.BytesIO(d), 10, 20, delimiter=b">") == b"foo1\nFOOBAR1\n>"
    assert (
        read_block(io.BytesIO(d), 10, 20, delimiter=b">", split_before=True)
        == b">foo1\nFOOBAR1\n"
    )

    # Read record at end, all records read to end

    tlen = len(d)

    assert (
        read_block(io.BytesIO(d), tlen - 30, 35, delimiter=b"\n")
        == b">foo99999\nFOOBAR99999\n"
    )

    assert (
        read_block(io.BytesIO(d), tlen - 30, 35, delimiter=b"\n", split_before=True)
        == b"\n>foo99999\nFOOBAR99999\n"
    )

    assert (
        read_block(io.BytesIO(d), tlen - 30, 35, delimiter=b">")
        == b"foo99999\nFOOBAR99999\n"
    )

    assert (
        read_block(io.BytesIO(d), tlen - 30, 35, delimiter=b">", split_before=True)
        == b">foo99999\nFOOBAR99999\n"
    )


def test_seek_delimiter_endline():
    f = io.BytesIO(b"123\n456\n789")

    # if at zero, stay at zero
    seek_delimiter(f, b"\n", 5)
    assert f.tell() == 0

    # choose the first block
    for bs in [1, 5, 100]:
        f.seek(1)
        seek_delimiter(f, b"\n", blocksize=bs)
        assert f.tell() == 4

    # handle long delimiters well, even with short blocksizes
    f = io.BytesIO(b"123abc456abc789")
    for bs in [1, 2, 3, 4, 5, 6, 10]:
        f.seek(1)
        seek_delimiter(f, b"abc", blocksize=bs)
        assert f.tell() == 6

    # End at the end
    f = io.BytesIO(b"123\n456")
    f.seek(5)
    seek_delimiter(f, b"\n", 5)
    assert f.tell() == 7


def test_infer_options():
    so = infer_storage_options("/mnt/datasets/test.csv")
    assert so.pop("protocol") == "file"
    assert so.pop("path") == "/mnt/datasets/test.csv"
    assert not so

    assert infer_storage_options("./test.csv")["path"] == "./test.csv"
    assert infer_storage_options("../test.csv")["path"] == "../test.csv"

    so = infer_storage_options("C:\\test.csv")
    assert so.pop("protocol") == "file"
    assert so.pop("path") == "C:\\test.csv"
    assert not so

    assert infer_storage_options("d:\\test.csv")["path"] == "d:\\test.csv"
    assert infer_storage_options("\\test.csv")["path"] == "\\test.csv"
    assert infer_storage_options(".\\test.csv")["path"] == ".\\test.csv"
    assert infer_storage_options("test.csv")["path"] == "test.csv"

    so = infer_storage_options(
        "hdfs://username:pwd@Node:123/mnt/datasets/test.csv?q=1#fragm",
        inherit_storage_options={"extra": "value"},
    )
    assert so.pop("protocol") == "hdfs"
    assert so.pop("username") == "username"
    assert so.pop("password") == "pwd"
    assert so.pop("host") == "Node"
    assert so.pop("port") == 123
    assert so.pop("path") == "/mnt/datasets/test.csv#fragm"
    assert so.pop("url_query") == "q=1"
    assert so.pop("url_fragment") == "fragm"
    assert so.pop("extra") == "value"
    assert not so

    so = infer_storage_options("hdfs://User-name@Node-name.com/mnt/datasets/test.csv")
    assert so.pop("username") == "User-name"
    assert so.pop("host") == "Node-name.com"

    u = "http://127.0.0.1:8080/test.csv"
    assert infer_storage_options(u) == {"protocol": "http", "path": u}

    # For s3 and gcs the netloc is actually the bucket name, so we want to
    # include it in the path. Test that:
    # - Parsing doesn't lowercase the bucket
    # - The bucket is included in path
    for protocol in ["s3", "gcs", "gs"]:
        options = infer_storage_options("%s://Bucket-name.com/test.csv" % protocol)
        assert options["path"] == "Bucket-name.com/test.csv"

    with pytest.raises(KeyError):
        infer_storage_options("file:///bucket/file.csv", {"path": "collide"})
    with pytest.raises(KeyError):
        infer_storage_options("hdfs:///bucket/file.csv", {"protocol": "collide"})


def test_infer_simple():
    out = infer_storage_options("//mnt/datasets/test.csv")
    assert out["protocol"] == "file"
    assert out["path"] == "//mnt/datasets/test.csv"
    assert out.get("host", None) is None


@pytest.mark.parametrize(
    "urlpath, expected_path",
    (
        (r"c:\foo\bar", r"c:\foo\bar"),
        (r"C:\\foo\bar", r"C:\\foo\bar"),
        (r"c:/foo/bar", r"c:/foo/bar"),
        (r"file:///c|\foo\bar", r"c:\foo\bar"),
        (r"file:///C|/foo/bar", r"C:/foo/bar"),
        (r"file:///C:/foo/bar", r"C:/foo/bar"),
    ),
)
def test_infer_storage_options_c(urlpath, expected_path):
    so = infer_storage_options(urlpath)
    assert so["protocol"] == "file"
    assert so["path"] == expected_path
