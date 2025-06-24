import sys

import pytest

import fsspec

if (3, 11) < sys.version_info < (3, 13):
    pytest.skip("Too many tests bust rate limit", allow_module_level=True)


def test_github_open_small_file():
    # test opening a small file <1 MB
    with fsspec.open("github://mwaskom:seaborn-data@4e06bf0/penguins.csv") as f:
        assert f.readline().startswith(b"species,island")


def test_github_open_large_file():
    # test opening a large file >1 MB
    # use block_size=0 to get a streaming interface to the file, ensuring that
    # we fetch only the parts we need instead of downloading the full file all
    # at once
    with fsspec.open(
        "github://mwaskom:seaborn-data@83bfba7/brain_networks.csv", block_size=0
    ) as f:
        # read only the first 20 bytes of the file
        assert f.read(20) == b"network,1,1,2,2,3,3,"


def test_github_open_lfs_file():
    # test opening a git-lfs tracked file
    with fsspec.open(
        "github://cBioPortal:datahub@55cd360"
        "/public/acc_2019/data_gene_panel_matrix.txt",
        block_size=0,
    ) as f:
        assert f.read(19) == b"SAMPLE_ID\tmutations"


def test_github_cat():
    # test using cat to fetch the content of multiple files
    fs = fsspec.filesystem("github", org="mwaskom", repo="seaborn-data")
    paths = ["penguins.csv", "mpg.csv"]
    cat_result = fs.cat(paths)
    assert set(cat_result.keys()) == {"penguins.csv", "mpg.csv"}
    assert cat_result["penguins.csv"].startswith(b"species,island")
    assert cat_result["mpg.csv"].startswith(b"mpg,cylinders")


def test_github_ls():
    # test using ls to list the files in a resository
    fs = fsspec.filesystem("github", org="mwaskom", repo="seaborn-data")
    ls_result = set(fs.ls(""))
    expected = {"brain_networks.csv", "mpg.csv", "penguins.csv", "README.md", "raw"}
    # check if the result is a subset of the expected files
    assert expected.issubset(ls_result)


def test_github_rm():
    # trying to remove a file without passing authentication should raise ValueError
    fs = fsspec.filesystem("github", org="mwaskom", repo="seaborn-data")
    with pytest.raises(ValueError):
        fs.rm("mpg.csv")

    # trying to remove a file which doesn't exist should raise FineNotFoundError
    fs = fsspec.filesystem(
        "github", org="mwaskom", repo="seaborn-data", username="user", token="token"
    )
    with pytest.raises(FileNotFoundError):
        fs.rm("/this-file-doesnt-exist")
