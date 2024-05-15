import os
import pickle
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path

import pytest

import fsspec
from fsspec.core import (
    OpenFile,
    OpenFiles,
    _expand_paths,
    expand_paths_if_needed,
    get_compression,
    get_fs_token_paths,
    open_files,
    open_local,
)


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
        except OSError:
            pass


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


@pytest.mark.parametrize(
    "create_files, path, out",
    [
        [["apath"], "apath", ["apath"]],
        [["apath1"], "apath*", ["apath1"]],
        [["apath1", "apath2"], "apath*", ["apath1", "apath2"]],
        [["apath1", "apath2"], "apath[1]", ["apath1"]],
        [["apath1", "apath11"], "apath?", ["apath1"]],
    ],
)
def test_expand_paths_if_needed_in_read_mode(create_files, path, out):
    d = str(tempfile.mkdtemp())
    for f in create_files:
        f = os.path.join(d, f)
        open(f, "w").write("test")

    path = os.path.join(d, path)

    fs = fsspec.filesystem("file")
    res = expand_paths_if_needed([path], "r", 0, fs, None)
    assert [os.path.basename(p) for p in res] == out


def test_expand_error():
    with pytest.raises(ValueError):
        _expand_paths("*.*", None, 1)


@pytest.mark.parametrize("mode", ["w", "w+", "x", "x+"])
def test_expand_fs_token_paths(mode):
    assert len(get_fs_token_paths("path", mode, num=2, expand=True)[-1]) == 2


def test_openfile_api(m):
    m.open("somepath", "wb").write(b"data")
    of = OpenFile(m, "somepath")
    assert str(of) == "<OpenFile 'somepath'>"
    f = of.open()
    assert f.read() == b"data"
    f.close()
    with OpenFile(m, "somepath", mode="rt") as f:
        assert f.read() == "data"


def test_openfile_open(m):
    of = OpenFile(m, "somepath", mode="wt")
    f = of.open()
    f.write("hello")
    assert m.size("somepath") == 0  # no flush yet
    of.close()
    assert m.size("somepath") == 5


def test_open_local_w_cache():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    d2 = str(tempfile.mkdtemp())
    fn = open_local(f"simplecache://{f1}", cache_storage=d2, target_protocol="file")
    assert isinstance(fn, str)
    assert open(fn).read() == "test1"
    assert d2 in fn


def test_open_local_w_magic():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    fn = open_local(os.path.join(d1, "f*"))
    assert len(fn) == 1
    assert isinstance(fn, list)


def test_open_local_w_list_of_str():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    fn = open_local([f1, f1])
    assert len(fn) == 2
    assert isinstance(fn, list)
    assert all(isinstance(elem, str) for elem in fn)


def test_open_local_w_path():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    p = Path(f1)
    fn = open_local(p)
    assert isinstance(fn, str)


def test_open_local_w_list_of_path():
    d1 = str(tempfile.mkdtemp())
    f1 = os.path.join(d1, "f1")
    open(f1, "w").write("test1")
    p = Path(f1)
    fn = open_local([p, p])
    assert len(fn) == 2
    assert isinstance(fn, list)
    assert all(isinstance(elem, str) for elem in fn)


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


def test_open_expand(m, monkeypatch):
    m.pipe("/myfile", b"hello")
    with pytest.raises(FileNotFoundError, match="expand=True"):
        with fsspec.open("memory://my*", expand=False):
            pass
    with fsspec.open("memory://my*", expand=True) as f:
        assert f.path == "/myfile"
    monkeypatch.setattr(fsspec.core, "DEFAULT_EXPAND", True)
    with fsspec.open("memory://my*") as f:
        assert f.path == "/myfile"


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
    of = fsspec.open(os.path.join(dir, "afile"), "w", auto_mkdir=False)
    with pytest.raises(IOError):
        with of:
            pass

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


def test_pickle_after_open_open():
    of = fsspec.open(__file__, mode="rt")
    test = of.open()
    of2 = pickle.loads(pickle.dumps(of))
    test2 = of2.open()
    test.close()

    assert not test2.closed
    of.close()
    of2.close()


# Define a list of special glob characters.
# Note that we need to escape some characters and also consider file system limitations.
# '*' and '?' are excluded because they are not valid for many file systems.
# Similarly, we're careful with '{', '}', and '@' as their special meaning is
# context-specific and might not be considered special for filenames.
# Add tests for more file systems and for more glob magic later
glob_magic_characters = ["[", "]", "!"]
if os.name != "nt":
    glob_magic_characters.extend(("*", "?"))  # not valid on Windows


@pytest.mark.parametrize("char", glob_magic_characters)
def test_open_file_read_with_special_characters(tmp_path, char):
    # Create a filename incorporating the special character
    file_name = f"test{char}.txt"
    file_path = tmp_path / file_name
    expected_content = "Hello, world!"

    with open(file_path, "w") as f:
        f.write(expected_content)

    with fsspec.open(file_path, "r") as f:
        actual_content = f.read()

    assert actual_content == expected_content


@pytest.mark.parametrize("char", glob_magic_characters)
def test_open_files_read_with_special_characters(tmp_path, char):
    # Create a filename incorporating the special character
    file_name = f"test{char}.txt"
    file_path = tmp_path / file_name
    expected_content = "Hello, world!"

    with open(file_path, "w") as f:
        f.write(expected_content)

    with fsspec.open_files(file_path, "r")[0] as f:
        actual_content = f.read()

    assert actual_content == expected_content


@pytest.mark.parametrize("char", glob_magic_characters)
def test_open_file_write_with_special_characters(tmp_path, char, monkeypatch):
    # Create a filename incorporating the special character
    file_name = f"test{char}.txt"
    file_path = tmp_path / file_name
    expected_content = "Hello, world!"

    with fsspec.open(file_path, "w", expand=False) as f:
        f.write(expected_content)

    with open(file_path, "r") as f:
        actual_content = f.read()

    monkeypatch.setattr(fsspec.core, "DEFAULT_EXPAND", False)
    with fsspec.open(file_path, "w") as f:
        f.write(expected_content * 2)

    with open(file_path, "r") as f:
        assert f.read() == actual_content * 2

    assert actual_content == expected_content


@pytest.mark.parametrize("char", glob_magic_characters)
def test_open_files_read_with_special_characters(tmp_path, char):
    # Create a filename incorporating the special character
    file_name = f"test{char}.txt"
    file_path = tmp_path / file_name
    expected_content = "Hello, world!"

    with open(file_path, "w") as f:
        f.write(expected_content)

    with fsspec.open_files(
        urlpath=[os.fspath(file_path)], mode="r", auto_mkdir=False, expand=False
    )[0] as f:
        actual_content = f.read()

    assert actual_content == expected_content


@pytest.mark.parametrize("char", glob_magic_characters)
def test_open_files_write_with_special_characters(tmp_path, char):
    # Create a filename incorporating the special character
    file_name = f"test{char}.txt"
    file_path = tmp_path / file_name
    expected_content = "Hello, world!"

    with fsspec.open_files(
        urlpath=[os.fspath(file_path)], mode="w", auto_mkdir=False, expand=False
    )[0] as f:
        f.write(expected_content)

    with open(file_path, "r") as f:
        actual_content = f.read()

    assert actual_content == expected_content


def test_mismatch():
    pytest.importorskip("s3fs")
    with pytest.raises(ValueError):
        open_files(["s3://test/path.csv", "/other/path.csv"])


def test_url_kwargs_chain(ftp_writable):
    host, port, username, password = ftp_writable
    data = b"hello"
    with fsspec.open(
        "ftp:///afile", "wb", host=host, port=port, username=username, password=password
    ) as f:
        f.write(data)

    with fsspec.open(
        f"simplecache::ftp://{username}:{password}@{host}:{port}//afile", "rb"
    ) as f:
        assert f.read() == data


def test_multi_context(tmpdir):
    fns = [os.path.join(tmpdir, fn) for fn in ["a", "b"]]
    files = open_files(fns, "wb")
    assert isinstance(files, OpenFiles)
    assert isinstance(files[0], OpenFile)
    assert len(files) == 2
    assert isinstance(files[:1], OpenFiles)
    assert len(files[:1]) == 1
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
    assert url == "/afile"
    fs, url = fsspec.core.url_to_fs(f"ftp://{username}:{password}@{host}:{port}/afile")
    assert url == "/afile"

    with fsspec.open(f"ftp://{username}:{password}@{host}:{port}/afile.zip", "wb") as f:
        import zipfile

        with zipfile.ZipFile(f, "w") as z:
            with z.open("inner", "w") as f2:
                f2.write(b"hello")
        f.write(data)

    fs, url = fsspec.core.url_to_fs(
        f"zip://inner::ftp://{username}:{password}@{host}:{port}/afile.zip"
    )
    assert url == "inner"
    fs, url = fsspec.core.url_to_fs(
        f"simplecache::zip::ftp://{username}:{password}@{host}:{port}/afile.zip"
    )
    assert url == ""


def test_target_protocol_options(ftp_writable):
    host, port, username, password = ftp_writable
    data = {"afile": b"hello"}
    options = {"host": host, "port": port, "username": username, "password": password}
    with tempzip(data) as lfile, fsspec.open(
        "ftp:///archive.zip", "wb", **options
    ) as f:
        f.write(open(lfile, "rb").read())
    with fsspec.open(
        "zip://afile",
        "rb",
        target_protocol="ftp",
        target_options=options,
        fo="archive.zip",
    ) as f:
        assert f.read() == data["afile"]


def test_chained_url(ftp_writable):
    host, port, username, password = ftp_writable
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


def test_automkdir_local():
    fs, _ = fsspec.core.url_to_fs("file://", auto_mkdir=True)
    assert fs.auto_mkdir is True
