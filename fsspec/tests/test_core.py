import os
import pickle
import tempfile
import zipfile
from contextlib import contextmanager

import pytest

import fsspec
from fsspec.core import (
    OpenFile,
    OpenFiles,
    _expand_paths,
    get_compression,
    open_files,
    open_local,
)


@pytest.mark.parametrize(
    "path, name_function, num, out",
    [
        [["apath"], None, 1, ["apath"]],
        ["apath.*.csv", None, 1, ["apath.0.csv"]],
        ["apath.*.csv", None, 2, ["apath.0.csv", "apath.1.csv"]],
        ["a*", lambda x: "abc"[x], 2, ["aa", "ab"]],
    ],
)
def test_expand_paths(path, name_function, num, out):
    assert _expand_paths(path, name_function, num) == out


def test_expand_error():
    with pytest.raises(ValueError):
        _expand_paths("*.*", None, 1)


def test_openfile_api(m):
    m.open("somepath", "wb").write(b"data")
    of = OpenFile(m, "somepath")
    assert str(of) == "<OpenFile 'somepath'>"
    f = of.open()
    assert f.read() == b"data"
    f.close()
    with OpenFile(m, "somepath", mode="rt") as f:
        f.read() == "data"


def test_openfile_open(m):
    of = OpenFile(m, "somepath", mode="wt")
    f = of.open()
    f.write("hello")
    assert m.size("somepath") == 0  # no flush yet
    del of
    assert m.size("somepath") == 0  # still no flush
    f.close()
    assert m.size("somepath") == 5


def test_open_local():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    d2 = str(tempfile.mkdtemp())
    fn = open_local("simplecache://" + f1, cache_storage=d2, target_protocol="file")
    assert isinstance(fn, str)
    assert open(fn).read() == "test1"
    assert d2 in fn


def test_xz_lzma_compressions():
    pytest.importorskip("lzma")
    # Ensure that both 'xz' and 'lzma' compression names can be parsed
    assert get_compression("some_file.xz", "infer") == "xz"
    assert get_compression("some_file.xz", "xz") == "xz"
    assert get_compression("some_file.xz", "lzma") == "lzma"


def test_list():
    here = os.path.abspath(os.path.dirname(__file__))
    flist = os.listdir(here)
    plist = [os.path.join(here, p).replace("\\", "/") for p in flist]
    of = open_files(plist)
    assert len(of) == len(flist)
    assert [f.path for f in of] == plist


def test_pathobject(tmpdir):
    import pathlib

    tmpdir = str(tmpdir)
    plist_str = [os.path.join(str(tmpdir), f).replace("\\", "/") for f in ["a", "b"]]
    open(plist_str[0], "w").write("first file")
    open(plist_str[1], "w").write("second file")
    plist = [pathlib.Path(p) for p in plist_str]
    of = open_files(plist)
    assert len(of) == 2
    assert [f.path for f in of] == plist_str

    of = open_files(plist[0])
    assert len(of) == 1
    assert of[0].path == plist_str[0]
    with of[0] as f:
        assert f.read() == open(plist_str[0], "rb").read()


def test_automkdir(tmpdir):
    dir = os.path.join(str(tmpdir), "a")
    of = fsspec.open(os.path.join(dir, "afile"), "w")
    with of:
        pass
    assert "afile" in os.listdir(dir)

    dir = os.path.join(str(tmpdir), "b")
    of = fsspec.open(os.path.join(dir, "bfile"), "w", auto_mkdir=True)
    with of:
        pass

    assert "bfile" in os.listdir(dir)

    dir = os.path.join(str(tmpdir), "c")
    with pytest.raises(FileNotFoundError):
        of = fsspec.open(os.path.join(dir, "bfile"), "w", auto_mkdir=False)
        with of:
            pass


def test_automkdir_readonly(tmpdir):
    dir = os.path.join(str(tmpdir), "d")
    with pytest.raises(FileNotFoundError):
        of = fsspec.open(os.path.join(dir, "dfile"), "r")
        with of:
            pass


def test_openfile_pickle_newline():
    # GH#318
    test = fsspec.open(__file__, newline=b"")

    pickled = pickle.dumps(test)
    restored = pickle.loads(pickled)

    assert test.newline == restored.newline


def test_mismatch():
    with pytest.raises(ValueError, match="protocol"):
        open_files(["s3://test/path.csv", "/other/path.csv"])


def test_url_kwargs_chain(ftp_writable):
    host, port, username, password = ftp_writable
    data = b"hello"
    with fsspec.open(
        "ftp:///afile", "wb", host=host, port=port, username=username, password=password
    ) as f:
        f.write(data)

    with fsspec.open(
        f"simplecache::ftp://{username}:{password}@{host}:{port}//afile",
        "rb",
    ) as f:
        assert f.read() == data


def test_multi_context(tmpdir):
    fns = [os.path.join(tmpdir, fn) for fn in ["a", "b"]]
    files = open_files(fns, "wb")
    assert isinstance(files, OpenFiles)
    assert isinstance(files[0], OpenFile)
    assert len(files) == 2
    with files as of:
        assert len(of) == 2
        assert not of[0].closed
        assert of[0].name.endswith("a")
    assert of[0].closed
    assert repr(files) == "<List of 2 OpenFile instances>"


def test_not_local():
    with pytest.raises(ValueError, match="attribute local_file=True"):
        open_local("memory://afile")


def test_url_to_fs(ftp_writable):
    host, port, username, password = ftp_writable
    data = b"hello"
    with fsspec.open(f"ftp://{username}:{password}@{host}:{port}/afile", "wb") as f:
        f.write(data)
    fs, url = fsspec.core.url_to_fs(
        f"simplecache::ftp://{username}:{password}@{host}:{port}/afile"
    )
    fs, url = fsspec.core.url_to_fs(f"ftp://{username}:{password}@{host}:{port}/afile")
    assert url == "/afile"


def test_target_protocol_options(ftp_writable):
    host, port, username, password = ftp_writable
    data = b"hello_protocol"
    options = {"host": host, "port": port, "username": username, "password": password}
    with fsspec.open(f"ftp://{username}:{password}@{host}:{port}/afile", "wb") as f:
        f.write(data)
    with fsspec.open(f"ftp://{username}:{password}@{host}:{port}/afile", "rb") as f:
        assert f.read() == data
    with fsspec.open("ftp:///afile", "rb", **options) as f:
        assert f.read() == data    
    with fsspec.open(
        "simplecache://afile",
        "rb",
        target_protocol="ftp",
        target_options=options,
    ) as f:
        assert f.read() == data


def test_chained_url(ftp_writable):
    host, port, username, password = ftp_writable

    @contextmanager
    def tempzip(data={}):
        f = tempfile.mkstemp(suffix="zip")[1]
        with zipfile.ZipFile(f, mode="w") as z:
            for k, v in data.items():
                z.writestr(k, v)
        try:
            yield f
        finally:
            try:
                os.remove(f)
            except (IOError, OSError):
                pass

    data = {"afile": b"hello"}

    cls = fsspec.get_filesystem_class("ftp")
    fs = cls(host=host, port=port, username=username, password=password)
    with tempzip(data) as lfile:
        fs.put_file(lfile, "archive.zip")

    urls = [
        "zip://afile",
        "zip://afile::simplecache",
        "simplecache::zip://afile",
        "simplecache::zip://afile::simplecache",
    ]
    for url in urls:
        url += f"::ftp://{username}:{password}@{host}:{port}/archive.zip"
        with fsspec.open(url, "rb") as f:
            assert f.read() == data["afile"]
