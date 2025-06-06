import bz2
import errno
import gzip
import os
import os.path
import pickle
import posixpath
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

import fsspec
from fsspec import compression
from fsspec.core import OpenFile, get_fs_token_paths, open_files
from fsspec.implementations.local import LocalFileSystem, get_umask, make_path_posix
from fsspec.tests.test_utils import WIN

files = {
    ".test.accounts.1.json": (
        b'{"amount": 100, "name": "Alice"}\n'
        b'{"amount": 200, "name": "Bob"}\n'
        b'{"amount": 300, "name": "Charlie"}\n'
        b'{"amount": 400, "name": "Dennis"}\n'
    ),
    ".test.accounts.2.json": (
        b'{"amount": 500, "name": "Alice"}\n'
        b'{"amount": 600, "name": "Bob"}\n'
        b'{"amount": 700, "name": "Charlie"}\n'
        b'{"amount": 800, "name": "Dennis"}\n'
    ),
}

csv_files = {
    ".test.fakedata.1.csv": (b"a,b\n1,2\n"),
    ".test.fakedata.2.csv": (b"a,b\n3,4\n"),
}
odir = os.getcwd()


@pytest.fixture()
def cwd():
    pth = os.getcwd().replace("\\", "/")
    assert not pth.endswith("/")
    yield pth


@pytest.fixture()
def current_drive(cwd):
    drive = os.path.splitdrive(cwd)[0]
    assert not drive or (len(drive) == 2 and drive.endswith(":"))
    yield drive


@pytest.fixture()
def user_home():
    pth = os.path.expanduser("~").replace("\\", "/")
    assert not pth.endswith("/")
    yield pth


def winonly(*args):
    return pytest.param(*args, marks=pytest.mark.skipif(not WIN, reason="Windows only"))


def posixonly(*args):
    return pytest.param(*args, marks=pytest.mark.skipif(WIN, reason="Posix only"))


@contextmanager
def filetexts(d, open=open, mode="t"):
    """Dumps a number of textfiles to disk

    d - dict
        a mapping from filename to text like {'a.csv': '1,1\n2,2'}

    Since this is meant for use in tests, this context manager will
    automatically switch to a temporary current directory, to avoid
    race conditions when running tests in parallel.
    """
    dirname = tempfile.mkdtemp()
    try:
        os.chdir(dirname)
        for filename, text in d.items():
            if dirname := os.path.dirname(filename):
                os.makedirs(dirname, exist_ok=True)
            f = open(filename, f"w{mode}")
            try:
                f.write(text)
            finally:
                try:
                    f.close()
                except AttributeError:
                    pass

        yield list(d)

        for filename in d:
            if os.path.exists(filename):
                try:
                    os.remove(filename)
                except OSError:
                    pass
    finally:
        os.chdir(odir)


def test_urlpath_inference_strips_protocol(tmpdir):
    tmpdir = make_path_posix(str(tmpdir))
    paths = ["/".join([tmpdir, f"test.{i:02d}.csv"]) for i in range(20)]

    for path in paths:
        with open(path, "wb") as f:
            f.write(b"1,2,3\n" * 10)

    # globstring
    protocol = "file:///" if sys.platform == "win32" else "file://"
    urlpath = protocol + os.path.join(tmpdir, "test.*.csv")
    _, _, paths2 = get_fs_token_paths(urlpath)
    assert paths2 == paths

    # list of paths
    _, _, paths2 = get_fs_token_paths([protocol + p for p in paths])
    assert paths2 == paths


def test_urlpath_inference_errors():
    # Empty list
    with pytest.raises(ValueError) as err:
        get_fs_token_paths([])
    assert "empty" in str(err.value)

    pytest.importorskip("s3fs")
    # Protocols differ
    with pytest.raises(ValueError) as err:
        get_fs_token_paths(["s3://test/path.csv", "/other/path.csv"])
    assert "Protocol" in str(err.value)


def test_urlpath_expand_read():
    """Make sure * is expanded in file paths when reading."""
    # when reading, globs should be expanded to read files by mask
    with filetexts(csv_files, mode="b"):
        _, _, paths = get_fs_token_paths("./.*.csv")
        assert len(paths) == 2
        _, _, paths = get_fs_token_paths(["./.*.csv"])
        assert len(paths) == 2


def test_cats():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        assert fs.cat(".test.fakedata.1.csv") == b"a,b\n1,2\n"
        out = set(fs.cat([".test.fakedata.1.csv", ".test.fakedata.2.csv"]).values())
        assert out == {b"a,b\n1,2\n", b"a,b\n3,4\n"}
        assert fs.cat(".test.fakedata.1.csv", None, None) == b"a,b\n1,2\n"
        assert fs.cat(".test.fakedata.1.csv", start=1, end=6) == b"a,b\n1,2\n"[1:6]
        assert fs.cat(".test.fakedata.1.csv", start=-1) == b"a,b\n1,2\n"[-1:]
        assert fs.cat(".test.fakedata.1.csv", start=1, end=-2) == b"a,b\n1,2\n"[1:-2]
        out = set(
            fs.cat(
                [".test.fakedata.1.csv", ".test.fakedata.2.csv"], start=1, end=-1
            ).values()
        )
        assert out == {b"a,b\n1,2\n"[1:-1], b"a,b\n3,4\n"[1:-1]}


def test_urlpath_expand_write():
    """Make sure * is expanded in file paths when writing."""
    _, _, paths = get_fs_token_paths("prefix-*.csv", mode="wb", num=2)
    assert all(
        p.endswith(pa) for p, pa in zip(paths, ["/prefix-0.csv", "/prefix-1.csv"])
    )
    _, _, paths = get_fs_token_paths(["prefix-*.csv"], mode="wb", num=2)
    assert all(
        p.endswith(pa) for p, pa in zip(paths, ["/prefix-0.csv", "/prefix-1.csv"])
    )
    # we can read with multiple masks, but not write
    with pytest.raises(ValueError):
        _, _, paths = get_fs_token_paths(
            ["prefix1-*.csv", "prefix2-*.csv"], mode="wb", num=2
        )


def test_open_files():
    with filetexts(files, mode="b"):
        myfiles = open_files("./.test.accounts.*")
        assert len(myfiles) == len(files)
        for lazy_file, data_file in zip(myfiles, sorted(files)):
            with lazy_file as f:
                x = f.read()
                assert x == files[data_file]


@pytest.mark.parametrize("encoding", ["utf-8", "ascii"])
def test_open_files_text_mode(encoding):
    with filetexts(files, mode="b"):
        myfiles = open_files("./.test.accounts.*", mode="rt", encoding=encoding)
        assert len(myfiles) == len(files)
        data = []
        for file in myfiles:
            with file as f:
                data.append(f.read())
        assert list(data) == [files[k].decode(encoding) for k in sorted(files)]


@pytest.mark.parametrize("mode", ["rt", "rb"])
@pytest.mark.parametrize("fmt", list(compression.compr))
def test_compressions(fmt, mode, tmpdir):
    tmpdir = str(tmpdir)
    fn = os.path.join(tmpdir, ".tmp.getsize")
    fs = LocalFileSystem()
    f = OpenFile(fs, fn, compression=fmt, mode="wb")
    data = b"Long line of readily compressible text"
    with f as fo:
        fo.write(data)
    if fmt is None:
        assert fs.size(fn) == len(data)
    else:
        assert fs.size(fn) != len(data)

    f = OpenFile(fs, fn, compression=fmt, mode=mode)
    with f as fo:
        if mode == "rb":
            assert fo.read() == data
        else:
            assert fo.read() == data.decode()


def test_bad_compression():
    with filetexts(files, mode="b"):
        for func in [open_files]:
            with pytest.raises(ValueError):
                func("./.test.accounts.*", compression="not-found")


def test_not_found():
    fn = "not-a-file"
    fs = LocalFileSystem()
    with pytest.raises((FileNotFoundError, OSError)):
        with OpenFile(fs, fn, mode="rb"):
            pass


def test_isfile():
    fs = LocalFileSystem()
    with filetexts(files, mode="b"):
        for f in files:
            assert fs.isfile(f)
            assert fs.isfile(f"file://{f}")
        assert not fs.isfile("not-a-file")
        assert not fs.isfile("file://not-a-file")


def test_isdir():
    fs = LocalFileSystem()
    with filetexts(files, mode="b"):
        for f in files:
            assert fs.isdir(os.path.dirname(os.path.abspath(f)))
            assert not fs.isdir(f)
        assert not fs.isdir("not-a-dir")


@pytest.mark.parametrize("compression_opener", [(None, open), ("gzip", gzip.open)])
def test_open_files_write(tmpdir, compression_opener):
    tmpdir = str(tmpdir)
    compression, opener = compression_opener
    fn = str(tmpdir) + "/*.part"
    files = open_files(fn, num=2, mode="wb", compression=compression)
    assert len(files) == 2
    assert {f.mode for f in files} == {"wb"}
    for fil in files:
        with fil as f:
            f.write(b"000")
    files = sorted(os.listdir(tmpdir))
    assert files == ["0.part", "1.part"]

    with opener(os.path.join(tmpdir, files[0]), "rb") as f:
        d = f.read()
    assert d == b"000"


def test_pickability_of_lazy_files(tmpdir):
    tmpdir = str(tmpdir)
    cloudpickle = pytest.importorskip("cloudpickle")

    with filetexts(files, mode="b"):
        myfiles = open_files("./.test.accounts.*")
        myfiles2 = cloudpickle.loads(cloudpickle.dumps(myfiles))

        for f, f2 in zip(myfiles, myfiles2):
            assert f.path == f2.path
            assert isinstance(f.fs, type(f2.fs))
            with f as f_open, f2 as f2_open:
                assert f_open.read() == f2_open.read()


def test_abs_paths(tmpdir):
    tmpdir = str(tmpdir)
    here = os.getcwd()
    os.chdir(tmpdir)
    with open("tmp", "w") as f:
        f.write("hi")
    out = LocalFileSystem().glob("./*")
    assert len(out) == 1
    assert "/" in out[0]
    assert "tmp" in out[0]

    # I don't know what this was testing - but should avoid local paths anyway
    # fs = LocalFileSystem()
    os.chdir(here)
    # with fs.open('tmp', 'r') as f:
    #     res = f.read()
    # assert res == 'hi'


@pytest.mark.parametrize("sep", ["/", "\\"])
@pytest.mark.parametrize("chars", ["+", "++", "(", ")", "|", "\\"])
def test_glob_weird_characters(tmpdir, sep, chars):
    tmpdir = make_path_posix(str(tmpdir))

    subdir = f"{tmpdir}{sep}test{chars}x"
    try:
        os.makedirs(subdir, exist_ok=True)
    except OSError as e:
        if WIN and "label syntax" in str(e):
            pytest.xfail("Illegal windows directory name")
        else:
            raise
    with open(subdir + sep + "tmp", "w") as f:
        f.write("hi")

    out = LocalFileSystem().glob(subdir + sep + "*")
    assert len(out) == 1
    assert "/" in out[0]
    assert "tmp" in out[0]


def test_globfind_dirs(tmpdir):
    tmpdir = make_path_posix(str(tmpdir))
    fs = fsspec.filesystem("file")
    fs.mkdir(tmpdir + "/dir")
    fs.touch(tmpdir + "/dir/afile")
    assert [tmpdir + "/dir"] == fs.glob(tmpdir + "/*")
    assert fs.glob(tmpdir + "/*", detail=True)[tmpdir + "/dir"]["type"] == "directory"
    assert (
        fs.glob(tmpdir + "/dir/*", detail=True)[tmpdir + "/dir/afile"]["type"] == "file"
    )
    assert [tmpdir + "/dir/afile"] == fs.find(tmpdir)
    assert [tmpdir, tmpdir + "/dir", tmpdir + "/dir/afile"] == fs.find(
        tmpdir, withdirs=True
    )


def test_touch(tmpdir):
    import time

    fn = str(tmpdir + "/in/file")
    fs = fsspec.filesystem("file", auto_mkdir=False)
    with pytest.raises(OSError):
        fs.touch(fn)
    fs = fsspec.filesystem("file", auto_mkdir=True)
    fs.touch(fn)
    info = fs.info(fn)
    time.sleep(0.2)
    fs.touch(fn)
    info2 = fs.info(fn)
    if not WIN:
        assert info2["mtime"] > info["mtime"]


def test_touch_truncate(tmpdir):
    fn = str(tmpdir + "/tfile")
    fs = fsspec.filesystem("file")
    fs.touch(fn, truncate=True)
    fs.pipe(fn, b"a")
    fs.touch(fn, truncate=True)
    assert fs.cat(fn) == b""
    fs.pipe(fn, b"a")
    fs.touch(fn, truncate=False)
    assert fs.cat(fn) == b"a"


def test_directories(tmpdir):
    tmpdir = make_path_posix(str(tmpdir))
    fs = LocalFileSystem()
    fs.mkdir(tmpdir + "/dir")
    assert tmpdir + "/dir" in fs.ls(tmpdir)
    assert fs.ls(tmpdir, True)[0]["type"] == "directory"
    fs.rmdir(tmpdir + "/dir")
    assert not fs.ls(tmpdir)
    assert fs.ls(fs.root_marker)


def test_ls_on_file(tmpdir):
    tmpdir = make_path_posix(str(tmpdir))
    fs = LocalFileSystem()
    resource = tmpdir + "/a.json"
    fs.touch(resource)
    assert fs.exists(resource)
    assert fs.ls(tmpdir) == fs.ls(resource)
    assert fs.ls(resource, detail=True)[0] == fs.info(resource)
    assert fs.ls(resource, detail=False)[0] == resource


def test_ls_on_files(tmpdir):
    tmpdir = make_path_posix(str(tmpdir))
    fs = LocalFileSystem()
    resources = [f"{tmpdir}/{file}" for file in ["file_1.json", "file_2.json"]]
    for r in resources:
        fs.touch(r)

    actual_files = fs.ls(tmpdir, detail=True)
    assert {f["name"] for f in actual_files} == set(resources)

    actual_files = fs.ls(tmpdir, detail=False)
    assert set(actual_files) == set(resources)


@pytest.mark.parametrize("file_protocol", ["", "file://"])
def test_file_ops(tmpdir, file_protocol):
    tmpdir = make_path_posix(str(tmpdir))
    tmpdir_with_protocol = file_protocol + tmpdir
    fs = LocalFileSystem(auto_mkdir=True)
    with pytest.raises(FileNotFoundError):
        fs.info(tmpdir_with_protocol + "/nofile")
    fs.touch(tmpdir_with_protocol + "/afile")
    i1 = fs.ukey(tmpdir_with_protocol + "/afile")

    assert tmpdir + "/afile" in fs.ls(tmpdir_with_protocol)

    with fs.open(tmpdir_with_protocol + "/afile", "wb") as f:
        f.write(b"data")
    i2 = fs.ukey(tmpdir_with_protocol + "/afile")
    assert i1 != i2  # because file changed

    fs.copy(tmpdir_with_protocol + "/afile", tmpdir_with_protocol + "/afile2")
    assert tmpdir + "/afile2" in fs.ls(tmpdir_with_protocol)

    fs.move(tmpdir_with_protocol + "/afile", tmpdir_with_protocol + "/afile3")
    assert not fs.exists(tmpdir_with_protocol + "/afile")

    fs.cp(
        tmpdir_with_protocol + "/afile3", tmpdir_with_protocol + "/deeply/nested/file"
    )
    assert fs.exists(tmpdir_with_protocol + "/deeply/nested/file")

    fs.rm(tmpdir_with_protocol + "/afile3", recursive=True)
    assert not fs.exists(tmpdir_with_protocol + "/afile3")

    files = [tmpdir_with_protocol + "/afile4", tmpdir_with_protocol + "/afile5"]
    [fs.touch(f) for f in files]

    with pytest.raises(AttributeError):
        fs.rm_file(files)
    fs.rm(files)
    assert all(not fs.exists(f) for f in files)

    fs.touch(tmpdir_with_protocol + "/afile6")
    fs.rm_file(tmpdir_with_protocol + "/afile6")
    assert not fs.exists(tmpdir_with_protocol + "/afile6")

    # IsADirectoryError raised on Linux, PermissionError on Windows
    with pytest.raises((IsADirectoryError, PermissionError)):
        fs.rm_file(tmpdir_with_protocol)

    fs.rm(tmpdir_with_protocol, recursive=True)
    assert not fs.exists(tmpdir_with_protocol)


def test_recursive_get_put(tmpdir):
    tmpdir = make_path_posix(str(tmpdir))
    fs = LocalFileSystem(auto_mkdir=True)

    fs.mkdir(tmpdir + "/a1/a2/a3")
    fs.touch(tmpdir + "/a1/a2/a3/afile")
    fs.touch(tmpdir + "/a1/afile")

    fs.get(f"file://{tmpdir}/a1", tmpdir + "/b1", recursive=True)
    assert fs.isfile(tmpdir + "/b1/afile")
    assert fs.isfile(tmpdir + "/b1/a2/a3/afile")

    fs.put(tmpdir + "/b1", f"file://{tmpdir}/c1", recursive=True)
    assert fs.isfile(tmpdir + "/c1/afile")
    assert fs.isfile(tmpdir + "/c1/a2/a3/afile")


def test_commit_discard(tmpdir):
    tmpdir = str(tmpdir)
    fs = LocalFileSystem()
    with fs.transaction:
        with fs.open(tmpdir + "/afile", "wb") as f:
            assert not fs.exists(tmpdir + "/afile")
            f.write(b"data")
        assert not fs.exists(tmpdir + "/afile")

    assert fs._transaction is None
    assert fs.cat(tmpdir + "/afile") == b"data"

    try:
        with fs.transaction:
            with fs.open(tmpdir + "/bfile", "wb") as f:
                f.write(b"data")
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        assert not fs.exists(tmpdir + "/bfile")


def test_same_permissions_with_and_without_transaction(tmpdir):
    tmpdir = str(tmpdir)

    with fsspec.open(tmpdir + "/afile", "wb") as f:
        f.write(b"data")

    fs, urlpath = fsspec.core.url_to_fs(tmpdir + "/bfile")
    with fs.transaction, fs.open(urlpath, "wb") as f:
        f.write(b"data")

    assert fs.info(tmpdir + "/afile")["mode"] == fs.info(tmpdir + "/bfile")["mode"]


def test_get_umask_uses_cache():
    get_umask.cache_clear()  # Clear the cache before the test for testing purposes.
    get_umask()  # Retrieve the current umask value.
    get_umask()  # Retrieve the umask again; this should result in a cache hit.
    assert get_umask.cache_info().hits == 1


def test_multiple_file_transaction_use_umask_cache(tmpdir):
    get_umask.cache_clear()  # Clear the cache before the test for testing purposes.
    fs = LocalFileSystem()
    with fs.transaction:
        with fs.open(tmpdir + "/afile", "wb") as f:
            f.write(b"data")
        with fs.open(tmpdir + "/bfile", "wb") as f:
            f.write(b"data")
    assert get_umask.cache_info().hits == 1


def test_multiple_transactions_use_umask_cache(tmpdir):
    get_umask.cache_clear()  # Clear the cache before the test for testing purposes.
    fs = LocalFileSystem()
    with fs.transaction:
        with fs.open(tmpdir + "/afile", "wb") as f:
            f.write(b"data")
    with fs.transaction:
        with fs.open(tmpdir + "/bfile", "wb") as f:
            f.write(b"data")
    assert get_umask.cache_info().hits == 1


def test_multiple_filesystems_use_umask_cache(tmpdir):
    get_umask.cache_clear()  # Clear the cache before the test for testing purposes.
    fs1 = LocalFileSystem()
    fs2 = LocalFileSystem()
    with fs1.transaction, fs1.open(tmpdir + "/afile", "wb") as f:
        f.write(b"data")
    with fs2.transaction, fs2.open(tmpdir + "/bfile", "wb") as f:
        f.write(b"data")
    assert get_umask.cache_info().hits == 1


def test_transaction_cross_device_but_mock_temp_dir_on_wrong_device(tmpdir):
    # If the temporary file for a transaction is not on the correct device,
    # os.rename in shutil.move will raise EXDEV and lookup('chmod') will raise
    # a PermissionError.
    fs = LocalFileSystem()
    with (
        patch(
            "os.rename",
            side_effect=OSError(errno.EXDEV, "Invalid cross-device link"),
        ),
        patch(
            "os.chmod",
            side_effect=PermissionError("Operation not permitted"),
        ),
    ):
        with fs.transaction, fs.open(tmpdir + "/afile", "wb") as f:
            f.write(b"data")


def test_make_path_posix():
    cwd = os.getcwd()
    if WIN:
        drive = cwd[0]
        assert make_path_posix("/a/posix/path") == f"{drive}:/a/posix/path"
        assert make_path_posix("/posix") == f"{drive}:/posix"
        # Windows drive requires trailing slash
        assert make_path_posix("C:\\") == "C:/"
    else:
        assert make_path_posix("/a/posix/path") == "/a/posix/path"
        assert make_path_posix("/posix") == "/posix"
    assert make_path_posix("relpath") == posixpath.join(make_path_posix(cwd), "relpath")
    assert make_path_posix("rel/path") == posixpath.join(
        make_path_posix(cwd), "rel/path"
    )
    # NT style
    if WIN:
        assert make_path_posix("C:\\path") == "C:/path"
        assert (
            make_path_posix(
                "\\\\windows-server\\someshare\\path\\more\\path\\dir\\foo.parquet",
            )
            == "//windows-server/someshare/path/more/path/dir/foo.parquet"
        )
        assert (
            make_path_posix(
                "\\\\SERVER\\UserHomeFolder$\\me\\My Documents\\proj\\data\\fname.csv",
            )
            == "//SERVER/UserHomeFolder$/me/My Documents/proj/data/fname.csv"
        )
    assert "/" in make_path_posix("rel\\path")
    # Relative
    pp = make_path_posix("./path")
    cd = make_path_posix(cwd)
    assert pp == cd + "/path"
    # Userpath
    userpath = make_path_posix("~/path")
    assert userpath.endswith("/path")


@pytest.mark.parametrize(
    "path",
    [
        "/abc/def",
        "abc/def",
        "",
        ".",
        "//server/share/",
        "\\\\server\\share\\",
        "C:\\",
        "d:/abc/def",
        "e:",
        pytest.param(
            "\\\\server\\share",
            marks=[
                pytest.mark.xfail(
                    WIN and sys.version_info < (3, 11),
                    reason="requires py3.11+ see: python/cpython#96290",
                )
            ],
        ),
        pytest.param(
            "f:foo",
            marks=[pytest.mark.xfail(WIN, reason="unsupported")],
            id="relative-path-with-drive",
        ),
    ],
)
def test_make_path_posix_returns_absolute_paths(path):
    posix_pth = make_path_posix(path)
    assert os.path.isabs(posix_pth)


@pytest.mark.parametrize("container_cls", [list, set, tuple])
def test_make_path_posix_set_list_tuple(container_cls):
    paths = container_cls(
        [
            "/foo/bar",
            "bar/foo",
        ]
    )
    posix_paths = make_path_posix(paths)
    assert isinstance(posix_paths, container_cls)
    assert posix_paths == container_cls(
        [
            make_path_posix("/foo/bar"),
            make_path_posix("bar/foo"),
        ]
    )


@pytest.mark.parametrize(
    "obj",
    [
        1,
        True,
        None,
        object(),
    ],
)
def test_make_path_posix_wrong_type(obj):
    with pytest.raises(TypeError):
        make_path_posix(obj)


def test_parent():
    if WIN:
        assert LocalFileSystem._parent("C:\\file or folder") == "C:/"
        assert LocalFileSystem._parent("C:\\") == "C:/"
    else:
        assert LocalFileSystem._parent("/file or folder") == "/"
        assert LocalFileSystem._parent("/") == "/"


@pytest.mark.parametrize(
    "path,parent",
    [
        ("C:\\", "C:/"),
        ("C:\\.", "C:/"),
        ("C:\\.\\", "C:/"),
        ("file:C:/", "C:/"),
        ("file://C:/", "C:/"),
        ("local:C:/", "C:/"),
        ("local://C:/", "C:/"),
        ("\\\\server\\share", "//server/share"),
        ("\\\\server\\share\\", "//server/share"),
        ("\\\\server\\share\\path", "//server/share"),
        ("//server/share", "//server/share"),
        ("//server/share/", "//server/share"),
        ("//server/share/path", "//server/share"),
        ("C:\\file or folder", "C:/"),
        ("C:\\file or folder\\", "C:/"),
        ("file:///", "{current_drive}/"),
        ("file:///path", "{current_drive}/"),
    ]
    if WIN
    else [
        ("/", "/"),
        ("/.", "/"),
        ("/./", "/"),
        ("file:/", "/"),
        ("file:///", "/"),
        ("local:/", "/"),
        ("local:///", "/"),
        ("/file or folder", "/"),
        ("/file or folder/", "/"),
        ("file:///path", "/"),
        ("file://c/", "{cwd}"),
    ],
)
def test_parent_edge_cases(path, parent, cwd, current_drive):
    parent = parent.format(cwd=cwd, current_drive=current_drive)

    assert LocalFileSystem._parent(path) == parent


def test_linked_files(tmpdir):
    tmpdir = str(tmpdir)
    fn0 = os.path.join(tmpdir, "target")
    fn1 = os.path.join(tmpdir, "link1")
    fn2 = os.path.join(tmpdir, "link2")
    data = b"my target data"
    with open(fn0, "wb") as f:
        f.write(data)
    try:
        os.symlink(fn0, fn1)
        os.symlink(fn0, fn2)
    except OSError:
        if WIN:
            pytest.xfail("Ran on win without admin permissions")
        else:
            raise

    fs = LocalFileSystem()
    assert fs.info(fn0)["type"] == "file"
    assert fs.info(fn1)["type"] == "file"
    assert fs.info(fn2)["type"] == "file"

    assert not fs.info(fn0)["islink"]
    assert fs.info(fn1)["islink"]
    assert fs.info(fn2)["islink"]

    assert fs.info(fn0)["size"] == len(data)
    assert fs.info(fn1)["size"] == len(data)
    assert fs.info(fn2)["size"] == len(data)

    of = fsspec.open(fn1, "rb")
    with of as f:
        assert f.read() == data

    of = fsspec.open(fn2, "rb")
    with of as f:
        assert f.read() == data


def test_linked_files_exists(tmpdir):
    origin = tmpdir / "original"
    copy_file = tmpdir / "copy"

    fs = LocalFileSystem()
    fs.touch(origin)

    try:
        os.symlink(origin, copy_file)
    except OSError:
        if WIN:
            pytest.xfail("Ran on win without admin permissions")
        else:
            raise

    assert fs.exists(copy_file)
    assert fs.lexists(copy_file)

    os.unlink(origin)

    assert not fs.exists(copy_file)
    assert fs.lexists(copy_file)

    os.unlink(copy_file)

    assert not fs.exists(copy_file)
    assert not fs.lexists(copy_file)


def test_linked_directories(tmpdir):
    tmpdir = str(tmpdir)

    subdir0 = os.path.join(tmpdir, "target")
    subdir1 = os.path.join(tmpdir, "link1")
    subdir2 = os.path.join(tmpdir, "link2")

    os.makedirs(subdir0)

    try:
        os.symlink(subdir0, subdir1)
        os.symlink(subdir0, subdir2)
    except OSError:
        if WIN:
            pytest.xfail("Ran on win without admin permissions")
        else:
            raise

    fs = LocalFileSystem()
    assert fs.info(subdir0)["type"] == "directory"
    assert fs.info(subdir1)["type"] == "directory"
    assert fs.info(subdir2)["type"] == "directory"

    assert not fs.info(subdir0)["islink"]
    assert fs.info(subdir1)["islink"]
    assert fs.info(subdir2)["islink"]


def test_isfilestore():
    fs = LocalFileSystem(auto_mkdir=False)
    assert fs._isfilestore()


def test_pickle(tmpdir):
    fs = LocalFileSystem()
    tmpdir = str(tmpdir)
    fn0 = os.path.join(tmpdir, "target")

    with open(fn0, "wb") as f:
        f.write(b"data")

    f = fs.open(fn0, "rb")
    f.seek(1)
    f2 = pickle.loads(pickle.dumps(f))
    assert f2.read() == f.read()

    f = fs.open(fn0, "wb")
    with pytest.raises(ValueError):
        pickle.dumps(f)

    # with context
    with fs.open(fn0, "rb") as f:
        f.seek(1)
        f2 = pickle.loads(pickle.dumps(f))
        assert f2.tell() == 1
        assert f2.read() == f.read()

    # with fsspec.open https://github.com/fsspec/filesystem_spec/issues/579
    with fsspec.open(fn0, "rb") as f:
        f.seek(1)
        f2 = pickle.loads(pickle.dumps(f))
        assert f2.tell() == 1
        assert f2.read() == f.read()


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("file://~/foo/bar", "{user_home}/foo/bar"),
        ("~/foo/bar", "{user_home}/foo/bar"),
        winonly("~\\foo\\bar", "{user_home}/foo/bar"),
        winonly("file://~\\foo\\bar", "{user_home}/foo/bar"),
    ],
)
def test_strip_protocol_expanduser(uri, expected, user_home):
    expected = expected.format(user_home=user_home)

    stripped = LocalFileSystem._strip_protocol(uri)
    assert expected == stripped


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("file://", "{cwd}"),
        ("file://.", "{cwd}"),
        ("file://./", "{cwd}"),
        ("./", "{cwd}"),
        ("file:path", "{cwd}/path"),
        ("file://path", "{cwd}/path"),
        ("path", "{cwd}/path"),
        ("./path", "{cwd}/path"),
        winonly(".\\", "{cwd}"),
        winonly("file://.\\path", "{cwd}/path"),
    ],
)
def test_strip_protocol_relative_paths(uri, expected, cwd):
    expected = expected.format(cwd=cwd)

    stripped = LocalFileSystem._strip_protocol(uri)
    assert expected == stripped


@pytest.mark.parametrize(
    "uri, expected",
    [
        posixonly("file:/foo/bar", "/foo/bar"),
        winonly("file:/foo/bar", "{current_drive}/foo/bar"),
        winonly("file:\\foo\\bar", "{current_drive}/foo/bar"),
        winonly("file:D:\\path\\file", "D:/path/file"),
        winonly("file:/D:\\path\\file", "D:/path/file"),
        winonly("file://D:\\path\\file", "D:/path/file"),
    ],
)
def test_strip_protocol_no_authority(uri, expected, cwd, current_drive):
    expected = expected.format(cwd=cwd, current_drive=current_drive)

    stripped = LocalFileSystem._strip_protocol(uri)
    assert expected == stripped


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("file:/path", "/path"),
        ("file:///path", "/path"),
        ("file:////path", "//path"),
        ("local:/path", "/path"),
        ("s3://bucket/key", "{cwd}/s3://bucket/key"),
        ("/path", "/path"),
        ("file:///", "/"),
    ]
    if not WIN
    else [
        ("file:c:/path", "c:/path"),
        ("file:/c:/path", "c:/path"),
        ("file:/C:/path", "C:/path"),
        ("file://c:/path", "c:/path"),
        ("file:///c:/path", "c:/path"),
        ("local:/path", "{current_drive}/path"),
        ("s3://bucket/key", "{cwd}/s3://bucket/key"),
        ("c:/path", "c:/path"),
        ("c:\\path", "c:/path"),
        ("file:///", "{current_drive}/"),
        pytest.param(
            "file://localhost/c:/path",
            "c:/path",
            marks=pytest.mark.xfail(
                reason="rfc8089 section3 'localhost uri' not supported"
            ),
        ),
    ],
)
def test_strip_protocol_absolute_paths(uri, expected, current_drive, cwd):
    expected = expected.format(current_drive=current_drive, cwd=cwd)

    stripped = LocalFileSystem._strip_protocol(uri)
    assert expected == stripped


@pytest.mark.parametrize(
    "uri, expected",
    [
        ("file:c|/path", "c:/path"),
        ("file:/D|/path", "D:/path"),
        ("file:///C|/path", "C:/path"),
    ],
)
@pytest.mark.skipif(not WIN, reason="Windows only")
@pytest.mark.xfail(WIN, reason="legacy dos uris not supported")
def test_strip_protocol_legacy_dos_uris(uri, expected):
    stripped = LocalFileSystem._strip_protocol(uri)
    assert expected == stripped


@pytest.mark.parametrize(
    "uri, stripped",
    [
        ("file://remote/share/pth", "{cwd}/remote/share/pth"),
        ("file:////remote/share/pth", "//remote/share/pth"),
        ("file://///remote/share/pth", "///remote/share/pth"),
        ("//remote/share/pth", "//remote/share/pth"),
        winonly("\\\\remote\\share\\pth", "//remote/share/pth"),
    ],
)
def test_strip_protocol_windows_remote_shares(uri, stripped, cwd):
    stripped = stripped.format(cwd=cwd)

    assert LocalFileSystem._strip_protocol(uri) == stripped


def test_mkdir_twice_faile(tmpdir):
    fn = os.path.join(tmpdir, "test")
    fs = fsspec.filesystem("file")
    fs.mkdir(fn)
    with pytest.raises(FileExistsError):
        fs.mkdir(fn)


def test_iterable(tmpdir):
    data = b"a\nhello\noi"
    fn = os.path.join(tmpdir, "test")
    with open(fn, "wb") as f:
        f.write(data)
    of = fsspec.open(f"file://{fn}", "rb")
    with of as f:
        out = list(f)
    assert b"".join(out) == data


def test_mv_empty(tmpdir):
    localfs = fsspec.filesystem("file")
    src = os.path.join(str(tmpdir), "src")
    dest = os.path.join(str(tmpdir), "dest")
    assert localfs.isdir(src) is False
    localfs.mkdir(src)
    assert localfs.isdir(src)
    localfs.move(src, dest, recursive=True)
    assert localfs.isdir(src) is False
    assert localfs.isdir(dest)
    assert localfs.info(dest)


def test_mv_recursive(tmpdir):
    localfs = fsspec.filesystem("file")
    src = os.path.join(str(tmpdir), "src")
    dest = os.path.join(str(tmpdir), "dest")
    assert localfs.isdir(src) is False
    localfs.mkdir(src)
    assert localfs.isdir(src)
    localfs.touch(os.path.join(src, "afile"))
    localfs.move(src, dest, recursive=True)
    assert localfs.isdir(src) is False
    assert localfs.isdir(dest)
    assert localfs.info(os.path.join(dest, "afile"))


@pytest.mark.xfail(WIN, reason="windows expand path to be revisited")
def test_copy_errors(tmpdir):
    localfs = fsspec.filesystem("file", auto_mkdir=True)

    dest1 = os.path.join(str(tmpdir), "dest1")
    dest2 = os.path.join(str(tmpdir), "dest2")

    src = os.path.join(str(tmpdir), "src")
    file1 = os.path.join(src, "afile1")
    file2 = os.path.join(src, "afile2")
    dne = os.path.join(str(tmpdir), "src", "notafile")

    localfs.mkdir(src)
    localfs.mkdir(dest1)
    localfs.mkdir(dest2)
    localfs.touch(file1)
    localfs.touch(file2)

    # Non recursive should raise an error unless we specify ignore
    with pytest.raises(FileNotFoundError):
        localfs.copy([file1, file2, dne], dest1)

    localfs.copy([file1, file2, dne], dest1, on_error="ignore")

    assert sorted(localfs.ls(dest1)) == [
        make_path_posix(os.path.join(dest1, "afile1")),
        make_path_posix(os.path.join(dest1, "afile2")),
    ]

    # Recursive should raise an error only if we specify raise
    # the patch simulates the filesystem finding a file that does not
    # exist in the directory
    current_files = localfs.expand_path(src, recursive=True)
    with patch.object(localfs, "expand_path", return_value=current_files + [dne]):
        with pytest.raises(FileNotFoundError):
            localfs.copy(src + "/", dest2, recursive=True, on_error="raise")

        localfs.copy(src + "/", dest2, recursive=True)
        assert sorted(localfs.ls(dest2)) == [
            make_path_posix(os.path.join(dest2, "afile1")),
            make_path_posix(os.path.join(dest2, "afile2")),
        ]


def test_transaction(tmpdir):
    file = str(tmpdir / "test.txt")
    fs = LocalFileSystem()

    with fs.transaction:
        content = "hello world"
        with fs.open(file, "w") as fp:
            fp.write(content)

    with fs.open(file, "r") as fp:
        read_content = fp.read()

    assert content == read_content


def test_delete_cwd(tmpdir):
    cwd = os.getcwd()
    fs = LocalFileSystem()
    try:
        os.chdir(tmpdir)
        with pytest.raises(ValueError):
            fs.rm(".", recursive=True)
    finally:
        os.chdir(cwd)


def test_delete_non_recursive_dir_fails(tmpdir):
    fs = LocalFileSystem()
    subdir = os.path.join(tmpdir, "testdir")
    fs.mkdir(subdir)
    with pytest.raises(ValueError):
        fs.rm(subdir)
    fs.rm(subdir, recursive=True)


@pytest.mark.parametrize(
    "opener, ext", [(bz2.open, ".bz2"), (gzip.open, ".gz"), (open, "")]
)
def test_infer_compression(tmpdir, opener, ext):
    filename = str(tmpdir / f"test{ext}")
    content = b"hello world"
    with opener(filename, "wb") as fp:
        fp.write(content)

    fs = LocalFileSystem()
    with fs.open(f"file://{filename}", "rb", compression="infer") as fp:
        read_content = fp.read()

    assert content == read_content


def test_info_path_like(tmpdir):
    path = Path(tmpdir / "test_info")
    path.write_text("fsspec")

    fs = LocalFileSystem()
    assert fs.exists(path)


def test_seekable(tmpdir):
    fs = LocalFileSystem()
    tmpdir = str(tmpdir)
    fn0 = os.path.join(tmpdir, "target")

    with open(fn0, "wb") as f:
        f.write(b"data")

    f = fs.open(fn0, "rt")
    assert f.seekable(), "file is not seekable"
    f.seek(1)
    assert f.read(1) == "a"
    assert f.tell() == 2


def test_numpy_fromfile(tmpdir):
    # Regression test for #1005.
    np = pytest.importorskip("numpy")
    fn = str(tmpdir / "test_arr.npy")
    dt = np.int64
    arr = np.arange(10, dtype=dt)
    arr.tofile(fn)
    assert np.array_equal(np.fromfile(fn, dtype=dt), arr)


def test_link(tmpdir):
    target = os.path.join(tmpdir, "target")
    link = os.path.join(tmpdir, "link")

    fs = LocalFileSystem()
    fs.touch(target)

    fs.link(target, link)
    assert fs.info(link)["nlink"] > 1


def test_symlink(tmpdir):
    target = os.path.join(tmpdir, "target")
    link = os.path.join(tmpdir, "link")

    fs = LocalFileSystem()
    fs.touch(target)
    try:
        fs.symlink(target, link)
    except OSError as e:
        if "[WinError 1314]" in str(e):
            # Windows requires developer mode to be enabled to use symbolic links
            return
        raise
    assert fs.islink(link)


# https://github.com/fsspec/filesystem_spec/issues/967
def test_put_file_to_dir(tmpdir):
    src_file = os.path.join(str(tmpdir), "src")
    target_dir = os.path.join(str(tmpdir), "target")
    target_file = os.path.join(target_dir, "src")

    fs = LocalFileSystem()
    fs.touch(src_file)
    fs.mkdir(target_dir)
    fs.put(src_file, target_dir)

    assert fs.isfile(target_file)


def test_du(tmpdir):
    file = tmpdir / "file"
    subdir = tmpdir / "subdir"
    subfile = subdir / "subfile"

    fs = LocalFileSystem()
    with open(file, "wb") as f:
        f.write(b"4444")
    fs.mkdir(subdir)
    with open(subfile, "wb") as f:
        f.write(b"7777777")

    # Switch to posix paths for comparisons
    tmpdir_posix = Path(tmpdir).as_posix()
    file_posix = Path(file).as_posix()
    subdir_posix = Path(subdir).as_posix()
    subfile_posix = Path(subfile).as_posix()

    assert fs.du(tmpdir) == 11
    assert fs.du(tmpdir, total=False) == {file_posix: 4, subfile_posix: 7}
    # Note directory size is OS-specific, but must be >= 0
    assert fs.du(tmpdir, withdirs=True) >= 11

    d = fs.du(tmpdir, total=False, withdirs=True)
    assert len(d) == 4
    assert d[file_posix] == 4
    assert d[subfile_posix] == 7
    assert d[tmpdir_posix] >= 0
    assert d[subdir_posix] >= 0

    assert fs.du(tmpdir, maxdepth=2) == 11
    assert fs.du(tmpdir, maxdepth=1) == 4
    with pytest.raises(ValueError):
        fs.du(tmpdir, maxdepth=0)

    # Size of file only.
    assert fs.du(file) == 4
    assert fs.du(file, withdirs=True) == 4


@pytest.mark.parametrize("funcname", ["cp", "get", "put"])
def test_cp_get_put_directory_recursive(tmpdir, funcname):
    # https://github.com/fsspec/filesystem_spec/issues/1062
    # Recursive cp/get/put of source directory into non-existent target directory.
    fs = LocalFileSystem()
    src = os.path.join(str(tmpdir), "src")
    fs.mkdir(src)
    fs.touch(os.path.join(src, "file"))

    target = os.path.join(str(tmpdir), "target")

    if funcname == "cp":
        func = fs.cp
    elif funcname == "get":
        func = fs.get
    elif funcname == "put":
        func = fs.put

    # cp/get/put without slash
    assert not fs.exists(target)
    for loop in range(2):
        func(src, target, recursive=True)
        assert fs.isdir(target)

        if loop == 0:
            assert fs.find(target) == [make_path_posix(os.path.join(target, "file"))]
        else:
            assert sorted(fs.find(target)) == [
                make_path_posix(os.path.join(target, "file")),
                make_path_posix(os.path.join(target, "src", "file")),
            ]

    fs.rm(target, recursive=True)

    # cp/get/put with slash
    assert not fs.exists(target)
    for loop in range(2):
        func(src + "/", target, recursive=True)
        assert fs.isdir(target)
        assert fs.find(target) == [make_path_posix(os.path.join(target, "file"))]


@pytest.mark.parametrize("funcname", ["cp", "get", "put"])
def test_cp_get_put_empty_directory(tmpdir, funcname):
    # https://github.com/fsspec/filesystem_spec/issues/1198
    # cp/get/put of empty directory.
    fs = LocalFileSystem(auto_mkdir=True)
    empty = os.path.join(str(tmpdir), "empty")
    fs.mkdir(empty)

    target = os.path.join(str(tmpdir), "target")
    fs.mkdir(target)

    if funcname == "cp":
        func = fs.cp
    elif funcname == "get":
        func = fs.get
    elif funcname == "put":
        func = fs.put

    # cp/get/put without slash, target directory exists
    assert fs.isdir(target)
    func(empty, target)
    assert fs.find(target, withdirs=True) == [make_path_posix(target)]

    # cp/get/put with slash, target directory exists
    assert fs.isdir(target)
    func(empty + "/", target)
    assert fs.find(target, withdirs=True) == [make_path_posix(target)]

    fs.rmdir(target)

    # cp/get/put without slash, target directory doesn't exist
    assert not fs.isdir(target)
    func(empty, target)
    assert not fs.isdir(target)

    # cp/get/put with slash, target directory doesn't exist
    assert not fs.isdir(target)
    func(empty + "/", target)
    assert not fs.isdir(target)


def test_cp_two_files(tmpdir):
    fs = LocalFileSystem(auto_mkdir=True)
    src = os.path.join(str(tmpdir), "src")
    file0 = os.path.join(src, "file0")
    file1 = os.path.join(src, "file1")
    fs.mkdir(src)
    fs.touch(file0)
    fs.touch(file1)

    target = os.path.join(str(tmpdir), "target")
    assert not fs.exists(target)

    fs.cp([file0, file1], target)

    assert fs.isdir(target)
    assert sorted(fs.find(target)) == [
        make_path_posix(os.path.join(target, "file0")),
        make_path_posix(os.path.join(target, "file1")),
    ]


@pytest.mark.skipif(WIN, reason="Windows does not support colons in filenames")
def test_issue_1447():
    files_with_colons = {
        ".local:file:with:colons.txt": b"content1",
        ".colons-after-extension.txt:after": b"content2",
        ".colons-after-extension/file:colon.txt:before/after": b"content3",
    }
    with filetexts(files_with_colons, mode="b"):
        for file, contents in files_with_colons.items():
            with fsspec.filesystem("file").open(file, "rb") as f:
                assert f.read() == contents

            fs, urlpath = fsspec.core.url_to_fs(file)
            assert isinstance(fs, fsspec.implementations.local.LocalFileSystem)
            with fs.open(urlpath, "rb") as f:
                assert f.read() == contents
