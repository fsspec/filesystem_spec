"""
Test-Cases for the DataBricks Filesystem.
This test case is somewhat special, as there is no "mock" databricks
API available. We use the [vcr(https://github.com/kevin1024/vcrpy)
package to record the requests and responses to the real databricks API and
replay them on tests.

This however means, that when you change the tests (or when the API
itself changes, which is very unlikely to occur as it is versioned),
you need to re-record the answers. This can be done as follows:

1. Delete all casettes files in the "./cassettes/test_dbfs" folder
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
import sys
from urllib.parse import urlparse

import numpy
import pytest

import fsspec

if sys.version_info >= (3, 10):
    pytest.skip("These tests need to be re-recorded.", allow_module_level=True)

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
    fs = fsspec.filesystem("dbfs", instance=INSTANCE, token=TOKEN)

    return fs


@pytest.fixture
def make_mock_diabetes_ds():
    pa = pytest.importorskip("pyarrow")

    names = [
        "Pregnancies",
        "Glucose",
        "BloodPressure",
        "SkinThickness",
        "Insulin",
        "BMI",
        "DiabetesPedigreeFunction",
        "Age",
        "Outcome",
    ]
    pregnancies = pa.array(numpy.random.randint(low=0, high=17, size=25))
    glucose = pa.array(numpy.random.randint(low=0, high=199, size=25))
    blood_pressure = pa.array(numpy.random.randint(low=0, high=122, size=25))
    skin_thickness = pa.array(numpy.random.randint(low=0, high=99, size=25))
    insulin = pa.array(numpy.random.randint(low=0, high=846, size=25))
    bmi = pa.array(numpy.random.uniform(0.0, 67.1, size=25))
    diabetes_pedigree_function = pa.array(numpy.random.uniform(0.08, 2.42, size=25))
    age = pa.array(numpy.random.randint(low=21, high=81, size=25))
    outcome = pa.array(numpy.random.randint(low=0, high=1, size=25))

    return pa.Table.from_arrays(
        arrays=[
            pregnancies,
            glucose,
            blood_pressure,
            skin_thickness,
            insulin,
            bmi,
            diabetes_pedigree_function,
            age,
            outcome,
        ],
        names=names,
    )


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
    content = b"This is a test\n" * (1 * 2**18) + b"For this is the end\n"
    with dbfsFS.open("/FileStore/large_file.txt", "wb") as f:
        f.write(content)
    assert "/FileStore/large_file.txt" in dbfsFS.ls("/FileStore", detail=False)
    assert dbfsFS.cat_file("/FileStore/large_file.txt", start=8) == content[8:]
    dbfsFS.rm("/FileStore/large_file.txt")
    assert "/FileStore/large_file.txt" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_write_pyarrow_non_partitioned(dbfsFS, make_mock_diabetes_ds):
    pytest.importorskip("pyarrow.dataset")
    pq = pytest.importorskip("pyarrow.parquet")

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)

    pq.write_to_dataset(
        make_mock_diabetes_ds,
        filesystem=dbfsFS,
        compression="none",
        existing_data_behavior="error",
        root_path="/FileStore/pyarrow/diabetes",
        use_threads=False,
    )

    assert len(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)) == 1
    assert (
        "/FileStore/pyarrow/diabetes"
        in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0]
        and ".parquet" in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0]
    )

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)


@pytest.mark.vcr()
def test_dbfs_read_pyarrow_non_partitioned(dbfsFS, make_mock_diabetes_ds):
    ds = pytest.importorskip("pyarrow.dataset")
    pq = pytest.importorskip("pyarrow.parquet")

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)

    pq.write_to_dataset(
        make_mock_diabetes_ds,
        filesystem=dbfsFS,
        compression="none",
        existing_data_behavior="error",
        root_path="/FileStore/pyarrow/diabetes",
        use_threads=False,
    )

    assert len(dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)) == 1
    assert (
        "/FileStore/pyarrow/diabetes"
        in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0]
        and ".parquet" in dbfsFS.ls("/FileStore/pyarrow/diabetes", detail=False)[0]
    )

    arr_res = ds.dataset(
        source="/FileStore/pyarrow/diabetes",
        filesystem=dbfsFS,
    ).to_table()

    assert arr_res.num_rows == make_mock_diabetes_ds.num_rows
    assert arr_res.num_columns == make_mock_diabetes_ds.num_columns
    assert set(arr_res.schema).difference(set(make_mock_diabetes_ds.schema)) == set()

    dbfsFS.rm("/FileStore/pyarrow", recursive=True)
    assert "/FileStore/pyarrow" not in dbfsFS.ls("/FileStore/", detail=False)
