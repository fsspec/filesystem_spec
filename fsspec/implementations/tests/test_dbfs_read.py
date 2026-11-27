import base64

import pytest

from fsspec.implementations.dbfs import DatabricksFile, DatabricksFileSystem


@pytest.fixture
def dbfs_reader(monkeypatch):
    block = DatabricksFile.DEFAULT_BLOCK_SIZE
    data = bytes(range(251)) * (3 * block // 251 + 1)
    calls = []
    fs = DatabricksFileSystem("example.invalid", "unused", skip_instance_cache=True)

    def api(method, endpoint, json):
        assert (method, endpoint) == ("get", "read")
        assert json["path"] == "/data"
        assert 0 < json["length"] <= block
        calls.append((json["offset"], json["length"]))
        start = json["offset"]
        return {"data": base64.b64encode(data[start : start + json["length"]])}

    monkeypatch.setattr(fs, "_send_to_api", api)
    with DatabricksFile(fs, "/data", size=len(data)) as reader:
        yield reader, data, calls
    fs.session.close()


@pytest.mark.parametrize(
    "start,length", [(0, 0), (17, 31), (0, 2**20), (123, 2**21 + 77)]
)
def test_dbfs_range_assembly(dbfs_reader, start, length):
    reader, data, calls = dbfs_reader
    assert reader._fetch_range(start, start + length) == data[start : start + length]
    block = reader.blocksize
    assert calls == [
        (s, min(block, start + length - s)) for s in range(start, start + length, block)
    ]


def test_dbfs_read_all_and_eof(dbfs_reader):
    reader, data, calls = dbfs_reader
    assert reader.read() == data
    assert reader.tell() == len(data)
    count = len(calls)
    assert reader.read() == b""
    assert len(calls) == count


def test_dbfs_short_last_response(dbfs_reader):
    reader, data, calls = dbfs_reader
    start = len(data) - reader.blocksize - 7
    assert reader._fetch_range(start, len(data) + 37) == data[start:]
    assert len(calls) == 2


def test_dbfs_mid_read_error_is_not_returned_as_partial_data(dbfs_reader, monkeypatch):
    reader, _, calls = dbfs_reader
    get_data = reader.fs._get_data

    def fail_second(path, start, end):
        if start >= reader.blocksize:
            raise OSError("download failed")
        return get_data(path, start, end)

    monkeypatch.setattr(reader.fs, "_get_data", fail_second)
    with pytest.raises(OSError, match="download failed"):
        reader.read()
    assert reader.tell() == 0
    assert len(calls) == 1
