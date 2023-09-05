import pytest

pytest.importorskip("s3fs")
pytest.importorskip("moto")

from s3fs.tests.test_s3fs import (  # noqa: E402,F401
    endpoint_uri,
    s3,
    s3_base,
    test_bucket_name,
)

so = {"anon": False, "client_kwargs": {"endpoint_url": endpoint_uri}}


def test_pandas(s3):
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df.to_csv(f"s3://{test_bucket_name}/a.csv", storage_options=so)
    df2 = pd.read_csv(f"s3://{test_bucket_name}/a.csv", storage_options=so)

    assert df.a.equals(df2.a)


def test_xarray_zarr(s3):
    xr = pytest.importorskip("xarray")
    pytest.importorskip("zarr")
    import numpy as np

    x = np.arange(5)
    xarr = xr.DataArray(x)
    ds = xr.Dataset({"x": xarr})
    ds.to_zarr(f"s3://{test_bucket_name}/a.zarr", storage_options=so)

    ds2 = xr.open_zarr(f"s3://{test_bucket_name}/a.zarr", storage_options=so)

    assert (ds.x == ds2.x).all()
