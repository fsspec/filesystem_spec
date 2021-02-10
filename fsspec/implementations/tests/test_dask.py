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
        client.close()


def test_basic(cli):

    fs = fsspec.filesystem("dask", target_protocol="memory")
    assert fs.ls("") == ["afile"]
    assert fs.cat("afile") == b"data"
