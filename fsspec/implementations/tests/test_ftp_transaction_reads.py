from contextlib import nullcontext

import pytest

from fsspec.implementations.ftp import FTPFileSystem


@pytest.fixture
def transaction_fs(ftp_writable):
    host, port, user, password = ftp_writable
    fs = FTPFileSystem(host, port, user, password)
    fs.mkdir("/stage")
    fs.tempdir = "/stage"
    fs.pipe_file("/input", b"committed input\nsecond line\n")
    yield fs
    fs.ftp.close()


@pytest.mark.parametrize("mode", ["rb", "rt"])
@pytest.mark.parametrize("options", [{}, {"autocommit": False}, {"autocommit": True}])
@pytest.mark.parametrize("transaction", [False, True])
def test_read_committed_file(transaction_fs, mode, options, transaction):
    fs = transaction_fs
    before = fs.info("/input").copy()

    def read():
        with fs.open("/input", mode, **options) as source:
            expected = b"committed input\nsecond line\n"
            if mode == "rt":
                expected = expected.decode()
            assert source.read() == expected
            source.seek(0)
            assert source.read() == expected
        assert not fs.transaction.files

    if transaction:
        with fs.transaction:
            read()
    else:
        read()
    assert fs.info("/input") == before
    assert fs.ls("/stage") == []


@pytest.mark.parametrize("rollback", [False, True])
def test_read_then_write(transaction_fs, rollback):
    fs = transaction_fs
    before = fs.cat_file("/input")
    with pytest.raises(RuntimeError, match="^rollback$") if rollback else nullcontext():
        with fs.transaction:
            with fs.open("/input", "rb") as source:
                data = source.read()
            with fs.open("/output", "wb") as target:
                target.write(data.upper())
            assert len(fs.transaction.files) == 1
            assert not fs.exists("/output")
            assert len(fs.ls("/stage")) == 1
            if rollback:
                raise RuntimeError("rollback")
    assert fs.cat_file("/input") == before
    assert fs.exists("/output") is not rollback
    if not rollback:
        assert fs.cat_file("/output") == before.upper()
    assert fs.ls("/stage") == []


@pytest.mark.parametrize("rollback", [False, True])
def test_reader_outlives_transaction(transaction_fs, rollback):
    fs = transaction_fs
    source = None
    try:
        with (
            pytest.raises(RuntimeError, match="^rollback$")
            if rollback
            else nullcontext()
        ):
            with fs.transaction:
                source = fs.open("/input", "rb")
                assert not fs.transaction.files
                if rollback:
                    raise RuntimeError("rollback")
        assert source.read() == b"committed input\nsecond line\n"
    finally:
        if source is not None:
            source.close()
    assert fs.ls("/stage") == []
