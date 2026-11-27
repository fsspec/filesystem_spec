import pytest

import fsspec

pytest.importorskip("distributed")


@pytest.fixture()
def cli(tmpdir):
    import dask.distributed

    client = dask.distributed.Client(n_workers=1)

    def setup():
        m = fsspec.filesystem("memory")
        with m.open("afile", "wb") as f:
            f.write(b"data")

    client.run(setup)
    try:
        yield client
    finally:
        client.shutdown()


def test_basic(cli):
    fs = fsspec.filesystem("dask", target_protocol="memory")
    assert fs.ls("", detail=False) == ["/afile"]
    assert fs.cat("/afile") == b"data"


def test_transaction_context_keeps_worker_actor(cli, tmp_path):
    from fsspec.transaction import DaskTransaction

    class Marker:
        def commit(self):
            (tmp_path / "commit").touch()

        def discard(self):
            (tmp_path / "discard").touch()

    fs = fsspec.filesystem("file", skip_instance_cache=True)
    fs.transaction_type = DaskTransaction
    with fs.transaction as transaction:
        transaction.files.append(Marker()).result()
    assert (tmp_path / "commit").exists()

    with pytest.raises(RuntimeError, match="abort"):
        with fs.transaction as transaction:
            transaction.files.append(Marker()).result()
            raise RuntimeError("abort")
    assert (tmp_path / "discard").exists()
    assert not fs._intrans


def test_failed_transaction_discards_remaining_worker_files(cli, tmp_path):
    from fsspec.transaction import DaskTransaction

    class Marker:
        def __init__(self, name):
            self.temporary = tmp_path / f"{name}.pending"
            self.temporary.touch()

        def commit(self):
            if self.temporary.stem == "failure":
                raise OSError("worker commit failed")
            self.temporary.rename(self.temporary.with_suffix(".committed"))

        def discard(self):
            self.temporary.unlink()

    fs = fsspec.filesystem("file", skip_instance_cache=True)
    fs.transaction_type = DaskTransaction
    with pytest.raises(OSError, match="worker commit failed"):
        with fs.transaction as transaction:
            for name in ("first", "failure", "pending"):
                transaction.files.append(Marker(name)).result()

    assert sorted(path.name for path in tmp_path.iterdir()) == ["first.committed"]
    assert not fs._intrans
    assert fs._transaction is None
