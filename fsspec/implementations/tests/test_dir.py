import asyncio
import os
import os.path
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

import pytest

import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import make_path_posix

from .test_http import data, realfile, server  # noqa: F401

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
    ".test.fakedata.1.csv": b"a,b\n1,2\n",
    ".test.fakedata.2.csv": b"a,b\n3,4\n",
    "a/b/c/.test.fakedata.3.csv": b"a,b\n3,4,5\n",
}
odir = os.getcwd()


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
            filename = Path(filename)

            if not filename.parent.exists():
                filename.parent.mkdir(parents=True, exist_ok=True)

            f = open(filename, "w" + mode)
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
                except (IOError, OSError):
                    pass
    finally:
        os.chdir(odir)


def test_open():
    with filetexts(csv_files, mode="b"):
        fs = DirFileSystem("a", fs=fsspec.filesystem("file"))
        with fs.open("b/c/.test.fakedata.3.csv") as f:
            assert f.read() == b"a,b\n3,4,5\n"


def test_prefix_root():
    with filetexts(csv_files, mode="b"):
        fs = DirFileSystem("/", fs=fsspec.filesystem("file"))
        abs_path_file = os.path.abspath("a/b/c/.test.fakedata.3.csv")

        # Risk double root marker (in path and in prefix)
        with fs.open(abs_path_file) as f:
            assert f.read() == b"a,b\n3,4,5\n"

        # no root marker in windows paths
        if os.name != "nt":
            # Without root marker
            with fs.open(abs_path_file[1:]) as f:
                assert f.read() == b"a,b\n3,4,5\n"


def test_cats():
    with filetexts(csv_files, mode="b"):
        fs = DirFileSystem(".", fs=fsspec.filesystem("file"))
        assert fs.cat(".test.fakedata.1.csv") == b"a,b\n" b"1,2\n"
        out = set(fs.cat([".test.fakedata.1.csv", ".test.fakedata.2.csv"]).values())
        assert out == {b"a,b\n" b"1,2\n", b"a,b\n" b"3,4\n"}
        assert fs.cat(".test.fakedata.1.csv", None, None) == b"a,b\n" b"1,2\n"
        assert fs.cat(".test.fakedata.1.csv", start=1, end=6) == b"a,b\n" b"1,2\n"[1:6]
        assert fs.cat(".test.fakedata.1.csv", start=-1) == b"a,b\n" b"1,2\n"[-1:]
        assert (
            fs.cat(".test.fakedata.1.csv", start=1, end=-2) == b"a,b\n" b"1,2\n"[1:-2]
        )
        out = set(
            fs.cat(
                [".test.fakedata.1.csv", ".test.fakedata.2.csv"], start=1, end=-1
            ).values()
        )
        assert out == {b"a,b\n" b"1,2\n"[1:-1], b"a,b\n" b"3,4\n"[1:-1]}


def test_not_found():
    fn = "not-a-file"
    fs = DirFileSystem(".", fs=fsspec.filesystem("file"))
    with pytest.raises((FileNotFoundError, OSError)):
        with OpenFile(fs, fn, mode="rb"):
            pass


def test_isfile():
    with filetexts(files, mode="b"):
        fs = DirFileSystem(os.getcwd(), fs=fsspec.filesystem("file"))
        for f in files.keys():
            assert fs.isfile(f)
        assert not fs.isfile("not-a-file")


def test_isdir():
    with filetexts(files, mode="b"):
        fs = DirFileSystem(os.getcwd(), fs=fsspec.filesystem("file"))
        for f in files.keys():
            assert fs.isfile(f)
            assert not fs.isdir(f)
        assert not fs.isdir("not-a-dir")


@pytest.mark.parametrize("dirname", ["/dir", "dir"])
def test_directories(tmpdir, dirname):
    import posixpath

    tmpdir = make_path_posix(str(tmpdir))
    prefix = posixpath.join(tmpdir, "a/b/c/d/e")

    fs = DirFileSystem(prefix, fs=fsspec.filesystem("file"))
    fs.mkdir(dirname)
    assert not os.path.exists(os.path.join(tmpdir, "dir"))
    assert os.path.exists(os.path.join(prefix, "dir"))
    assert fs.ls(".", detail=False) == ["./dir"]
    fs.rmdir(dirname)
    assert not os.path.exists(os.path.join(prefix, "dir"))

    fs = DirFileSystem(f"{tmpdir}/a", fs=fsspec.filesystem("file"))
    assert fs.ls(".", detail=False) == ["./b"]
    fs.rm("b", recursive=True)
    assert fs.ls(".", detail=False) == []


@pytest.mark.parametrize(
    "ls_arg, expected_out",
    [
        (".", ["./b"]),
        ("./", ["./b"]),
        ("./b", ["./b/c"]),
        ("./b/", ["./b/c"]),
        ("b", ["b/c"]),
        ("b/", ["b/c"]),
        ("./b/c/d", ["./b/c/d/e"]),
        ("./b/c/d/", ["./b/c/d/e"]),
        ("b/c/d", ["b/c/d/e"]),
        ("b/c/d/", ["b/c/d/e"]),
        ("b/c/d/e", []),
        ("b/c/d/e/", []),
    ],
)
def test_ls(tmpdir, ls_arg, expected_out):
    os.makedirs(os.path.join(make_path_posix(str(tmpdir)), "a/b/c/d/e/"))
    fs = DirFileSystem(f"{tmpdir}/a", fs=fsspec.filesystem("file"))
    assert fs.ls(ls_arg, detail=False) == expected_out


def test_async_fs_list(server):  # noqa: F811
    h = fsspec.filesystem("http")
    fs = DirFileSystem(server + "/index", fs=h)
    out = fs.glob("*")
    assert out == ["realfile"]


@pytest.mark.skipif(sys.version_info < (3, 7), reason="no asyncio.run in py36")
def test_async_this_thread(server):  # noqa: F811
    async def _test():
        h = fsspec.filesystem("http", asynchronous=True)
        fs = DirFileSystem(server + "/index", fs=h)

        session = await fs.set_session()  # creates client

        url = "realfile"
        with pytest.raises((NotImplementedError, RuntimeError)):
            fs.cat([url])
        out = await fs._cat([url])
        del fs
        assert out == {url: data}
        await session.close()

    asyncio.run(_test())
