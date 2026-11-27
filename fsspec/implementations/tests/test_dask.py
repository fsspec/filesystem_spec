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
