"""
Test-Cases for the DataBricks Filesystem.
This test case is somewhat special, as there is no "mock" databricks
API available. We use the "vcr" package to record the requests and
responses to the real databricks API and replay them on tests.

This however means, that when you change the tests (or when the API
itself changes, which is very unlikely to occur as it is versioned),
you need to re-record the answers. This can be done as follows:

1. Delete all casettes files in the "./cassettes" folder
2. Spin up a databricks cluster. For example,
   you can use an Azure Databricks instance for this.
3. Take note of the instance details (the instance URL. For example for an Azure
   databricks cluster, this has the form
   adb-<some-number>.<two digits>.azuredatabricks.net)
   and your personal token (Find out more here:
   https://docs.databricks.com/dev-tools/api/latest/authentication.html)
4. Set the two environment variables `DBFS_INSTANCE` and `DBFS_TOKEN`
5. Now execute the tests as normal. The results of the API calls will be recorded.
6. Unset the environment variables and replay the tests.
"""
import os
from urllib.parse import urlparse

import pytest

import fsspec

DUMMY_INSTANCE = "my_instance.com"
INSTANCE = os.getenv("DBFS_INSTANCE", DUMMY_INSTANCE)
TOKEN = os.getenv("DBFS_TOKEN", "")


@pytest.fixture(scope="module")
def vcr_config():
    """
    To not record information in the instance and token details
    (which are sensitive), we delete them from both the
    request and the response before storing it.
    We also delete the date as it is likely to change
    (and will make git diffs harder).
    If the DBFS_TOKEN env variable is set, we record with VCR.
    If not, we only replay (to not accidentally record with a wrong URL).
    """

    def before_record_response(response):
        try:
            del response["headers"]["x-databricks-org-id"]
            del response["headers"]["date"]
        except KeyError:
            pass
        return response

    def before_record_request(request):
        # Replace the instance URL
        uri = urlparse(request.uri)
        uri = uri._replace(netloc=DUMMY_INSTANCE)
        request.uri = uri.geturl()

        return request

    if TOKEN:
        return {
            "record_mode": "once",
            "filter_headers": [("authorization", "DUMMY")],
            "before_record_response": before_record_response,
            "before_record_request": before_record_request,
        }
    else:
        return {
            "record_mode": "none",
        }


@pytest.fixture
def dbfsFS():
    fs = fsspec.filesystem(
        "dbfs",
        instance=INSTANCE,
        token=TOKEN,
    )

    return fs


@pytest.mark.vcr()
def test_dbfs_file_listing(dbfsFS):
    assert "/FileStore" in dbfsFS.ls("/", detail=False)
    assert {"name": "/FileStore", "size": 0, "type": "directory"} in dbfsFS.ls(
        "/", detail=True
    )


@pytest.mark.vcr()
def test_dbfs_mkdir(dbfsFS):
    dbfsFS.rm("/FileStore/my", recursive=True)
    assert "/FileStore/my" not in dbfsFS.ls("/FileStore/", detail=False)

    dbfsFS.mkdir("/FileStore/my/dir", create_parents=True)

    assert "/FileStore/my" in dbfsFS.ls("/FileStore/", detail=False)
    assert "/FileStore/my/dir" in dbfsFS.ls("/FileStore/my/", detail=False)

    with pytest.raises(FileExistsError):
        dbfsFS.mkdir("/FileStore/my/dir", create_parents=True, exist_ok=False)

    with pytest.raises(OSError):
        dbfsFS.rm("/FileStore/my", recursive=False)

    assert "/FileStore/my" in dbfsFS.ls("/FileStore/", detail=False)

    dbfsFS.rm("/FileStore/my", recursive=True)
    assert "/FileStore/my" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_write_and_read(dbfsFS):
    dbfsFS.rm("/FileStore/file.csv")
    assert "/FileStore/file.csv" not in dbfsFS.ls("/FileStore/", detail=False)

    content = b"This is a test\n" * 100000 + b"For this is the end\n"

    with dbfsFS.open("/FileStore/file.csv", "wb") as f:
        f.write(content)

    assert "/FileStore/file.csv" in dbfsFS.ls("/FileStore", detail=False)

    with dbfsFS.open("/FileStore/file.csv", "rb") as f:
        data = f.read()
        assert data == content
    dbfsFS.rm("/FileStore/file.csv")
    assert "/FileStore/file.csv" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_read_range(dbfsFS):
    dbfsFS.rm("/FileStore/file.txt")
    assert "/FileStore/file.txt" not in dbfsFS.ls("/FileStore/", detail=False)
    content = b"This is a test\n"
    with dbfsFS.open("/FileStore/file.txt", "wb") as f:
        f.write(content)
    assert "/FileStore/file.txt" in dbfsFS.ls("/FileStore", detail=False)
    assert dbfsFS.cat_file("/FileStore/file.txt", start=8, end=14) == content[8:14]
    dbfsFS.rm("/FileStore/file.txt")
    assert "/FileStore/file.txt" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_read_range_chunked(dbfsFS):
    dbfsFS.rm("/FileStore/large_file.txt")
    assert "/FileStore/large_file.txt" not in dbfsFS.ls("/FileStore/", detail=False)
    content = b"This is a test\n" * (1 * 2 ** 18) + b"For this is the end\n"
    with dbfsFS.open("/FileStore/large_file.txt", "wb") as f:
        f.write(content)
    assert "/FileStore/large_file.txt" in dbfsFS.ls("/FileStore", detail=False)
    assert dbfsFS.cat_file("/FileStore/large_file.txt", start=8) == content[8:]
    dbfsFS.rm("/FileStore/large_file.txt")
    assert "/FileStore/large_file.txt" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_write_pyarrow_non_partitioned(dbfsFS):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)

    test_df = pd.read_csv("data/diabetes.csv")
    arr_table = pa.Table.from_pandas(df=test_df)

    pq.write_to_dataset(arr_table, filesystem=dbfsFS, compression='none',
                        existing_data_behavior='error',
                        root_path="/FileStore/pyarrow/diabetes",
                        use_threads=False)

    assert len(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)) is 1
    assert ("/FileStore/pyarrow/diabetes" in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0] and
            ".parquet" in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0])

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)


# @pytest.mark.vcr()
# def test_dbfs_write_pyarrow_partitioned(dbfsFS):
#     import pandas as pd
#     import pyarrow as pa
#     import pyarrow.parquet as pq
#
#     dbfsFS.rm("/FileStore/pyarrow", recursive=True)
#     assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)
#
#     test_df = pd.read_csv("data/diabetes.csv")
#     arr_table = pa.Table.from_pandas(df=test_df)
#
#     pq.write_to_dataset(arr_table, filesystem=dbfsFS, compression='none',
#                         existing_data_behavior='error', partition_cols=["Pregnancies"],
#                         root_path="/FileStore/pyarrow/diabetes",
#                         use_threads=False)
#
#     assert len(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)) is 17
#     assert set(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)).difference(
#         set(["/FileStore/pyarrow/diabetes/Pregnancies=0",
#             "/FileStore/pyarrow/diabetes/Pregnancies=1",
#             "/FileStore/pyarrow/diabetes/Pregnancies=2",
#             "/FileStore/pyarrow/diabetes/Pregnancies=3",
#             "/FileStore/pyarrow/diabetes/Pregnancies=4",
#             "/FileStore/pyarrow/diabetes/Pregnancies=5",
#             "/FileStore/pyarrow/diabetes/Pregnancies=6",
#             "/FileStore/pyarrow/diabetes/Pregnancies=7",
#             "/FileStore/pyarrow/diabetes/Pregnancies=8",
#             "/FileStore/pyarrow/diabetes/Pregnancies=9",
#             "/FileStore/pyarrow/diabetes/Pregnancies=10",
#             "/FileStore/pyarrow/diabetes/Pregnancies=11",
#             "/FileStore/pyarrow/diabetes/Pregnancies=12",
#             "/FileStore/pyarrow/diabetes/Pregnancies=13",
#             "/FileStore/pyarrow/diabetes/Pregnancies=14",
#             "/FileStore/pyarrow/diabetes/Pregnancies=15",
#             "/FileStore/pyarrow/diabetes/Pregnancies=17"
#         ])
#     ) == set()
#
#     dbfsFS.rm("/FileStore/pyarrow", recursive=True)
#     assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_read_pyarrow_non_partitioned(dbfsFS):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)

    test_df = pd.read_csv("data/diabetes.csv")
    arr_table = pa.Table.from_pandas(df=test_df)

    pq.write_to_dataset(arr_table, filesystem=dbfsFS, compression='none',
                        existing_data_behavior='error',
                        root_path="/FileStore/pyarrow/diabetes",
                        use_threads=False)

    assert len(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)) is 1
    assert ("/FileStore/pyarrow/diabetes" in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0] and
            ".parquet" in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0])

    arr_res = ds.dataset(
                source="/FileStore/pyarrow/diabetes",
                filesystem=dbfsFS,
            ).to_table()

    assert arr_res.num_rows == arr_table.num_rows
    assert arr_res.num_columns == arr_table.num_columns
    assert set(arr_res.schema).difference(set(arr_table.schema)) == set()

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)


# def test_dbfs_read_pyarrow_partitioned(dbfsFS):
#     import pandas as pd
#     import pyarrow as pa
#     import pyarrow.parquet as pq
#     import pyarrow.dataset as ds
#
#     dbfsFS.rm("/FileStore/pyarrow", recursive=True)
#     assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)
#
#     test_df = pd.read_csv("data/diabetes.csv")
#     arr_table = pa.Table.from_pandas(df=test_df)
#
#     pq.write_to_dataset(arr_table, filesystem=dbfsFS, compression='none',
#                         existing_data_behavior='error', partition_cols=["Pregnancies"],
#                         root_path="/FileStore/pyarrow/diabetes",
#                         use_threads=False)
#
#     assert len(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)) is 17
#
#     arr_res = ds.dataset(
#                 source="/FileStore/pyarrow/diabetes",
#                 filesystem=dbfsFS,
#                 partitioning='hive'
#             ).to_table()
#
#     assert arr_res.num_rows == arr_table.num_rows
#     assert arr_res.num_columns == arr_table.num_columns
#     assert set(arr_res.schema).difference(set(arr_table.schema)) == set()
#
#     dbfsFS.rm("/FileStore/pyarrow", recursive=True)
#     assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)