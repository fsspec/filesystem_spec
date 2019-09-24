import pytest
import fsspec
import sys


@pytest.fixture()
def m():
    m = fsspec.filesystem("memory")
    m.store.clear()
    try:
        yield m
    finally:
        m.store.clear()


def test_1(m):
    m.touch("/somefile")  # NB: is found with or without initial /
    m.touch("afiles/and/anothers")
    assert m.find("") == ["afiles/and/anothers", "somefile"]
    assert list(m.get_mapper("")) == ["afiles/and/anothers", "somefile"]


@pytest.mark.xfail(
    sys.version_info < (3, 6),
    reason="py35 error, see https://github.com/intake/filesystem_spec/issues/148",
)
def test_ls(m):
    m.touch("/dir/afile")
    m.touch("/dir/dir1/bfile")
    m.touch("/dir/dir1/cfile")

    assert m.ls("/", False) == ["/dir/"]
    assert m.ls("/dir", False) == ["/dir/afile", "/dir/dir1/"]
    assert m.ls("/dir", True)[0]["type"] == "file"
    assert m.ls("/dir", True)[1]["type"] == "directory"

    assert len(m.ls("/dir/dir1")) == 2
