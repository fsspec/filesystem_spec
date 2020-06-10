import pytest
import sys


def test_1(m):
    m.touch("/somefile")  # NB: is found with or without initial /
    m.touch("afiles/and/anothers")
    files = m.find("")
    if "somefile" in files:
        assert files == ["afiles/and/anothers", "somefile"]
    else:
        assert files == ["/somefile", "afiles/and/anothers"]

    files = sorted(m.get_mapper(""))
    if "somefile" in files:
        assert files == ["afiles/and/anothers", "somefile"]
    else:
        assert files == ["/somefile", "afiles/and/anothers"]


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


def test_directories(m):
    with pytest.raises(NotADirectoryError):
        m.mkdir("outer/inner", create_parents=False)
    m.mkdir("outer/inner")

    assert m.ls("outer")
    assert m.ls("outer/inner") == []

    with pytest.raises(OSError):
        m.rmdir("outer")

    m.rmdir("outer/inner")
    m.rmdir("outer")

    assert not m.store
