from __future__ import absolute_import, division, print_function

import os
import os.path
import tempfile
from contextlib import contextmanager
from pathlib import Path

import pytest

import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.local import make_path_posix
from fsspec.implementations.prefix import (
    PrefixFileSystem,
    remove_root_marker,
    remove_trailing_sep,
)

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
        fs = PrefixFileSystem(prefix="a", fs=fsspec.filesystem("file"))
        with fs.open("b/c/.test.fakedata.3.csv") as f:
            assert f.read() == b"a,b\n3,4,5\n"


def test_cats():
    with filetexts(csv_files, mode="b"):
        fs = PrefixFileSystem(prefix=".", fs=fsspec.filesystem("file"))
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
    fs = PrefixFileSystem(prefix=".", fs=fsspec.filesystem("file"))
    with pytest.raises((FileNotFoundError, OSError)):
        with OpenFile(fs, fn, mode="rb"):
            pass


def test_isfile():
    fs = PrefixFileSystem(prefix=".", fs=fsspec.filesystem("file"))
    with filetexts(files, mode="b"):
        for f in files.keys():
            assert fs.isfile(f)
            assert fs.isfile("file://" + f)
        assert not fs.isfile("not-a-file")
        assert not fs.isfile("file://not-a-file")


def test_isdir():
    fs = PrefixFileSystem(prefix=".", fs=fsspec.filesystem("file"))
    with filetexts(files, mode="b"):
        for f in files.keys():
            assert fs.isfile(f)
            assert not fs.isdir(f)
        assert not fs.isdir("not-a-dir")


@pytest.mark.parametrize("dirname", ["/dir", "dir"])
@pytest.mark.parametrize("prefix", ["a/b/c/d/e", "a/b/c/d/e/"])
def test_directories(tmpdir, prefix, dirname):
    tmpdir = make_path_posix(str(tmpdir))
    prefix = os.path.join(tmpdir, prefix)

    fs = PrefixFileSystem(prefix=prefix, fs=fsspec.filesystem("file"))
    fs.mkdir(dirname)
    assert not os.path.exists(os.path.join(tmpdir, "dir"))
    assert os.path.exists(os.path.join(prefix, "dir"))
    assert fs.ls(".") == ["./dir"]
    fs.rmdir(dirname)
    assert not os.path.exists(os.path.join(prefix, "dir"))

    fs = PrefixFileSystem(prefix=f"{tmpdir}/a", fs=fsspec.filesystem("file"))
    assert fs.ls(".") == ["./b"]
    fs.rm("b", recursive=True)
    assert fs.ls(".") == []


def test_emtpy_prefix():
    with pytest.raises(ValueError):
        PrefixFileSystem(prefix="", fs=fsspec.filesystem("file"))

    with pytest.raises(ValueError):
        PrefixFileSystem(prefix=None, fs=fsspec.filesystem("file"))


@pytest.mark.parametrize(
    "prefix",
    [
        ("/", "/"),
        ("a", "a"),
        ("/a", "/a"),
        ("a/", "a"),
        ("/a/", "/a"),
        ("/a/b/c/", "/a/b/c"),
    ],
)
def test_remove_trailing_sep(prefix):
    fs = fsspec.filesystem("file")
    prefix, normalized_prefix = prefix
    assert (
        remove_trailing_sep(prefix, sep=fs.sep, root_marker=fs.root_marker)
        == normalized_prefix
    )


@pytest.mark.parametrize(
    "prefix",
    [
        ("/", ""),
        ("a", "a"),
        ("/a", "a"),
        ("a/", "a/"),
        ("/a/", "a/"),
        ("/a/b/c/", "a/b/c/"),
    ],
)
def test_remove_root_marker(prefix):
    fs = fsspec.filesystem("file")
    prefix, normalized_prefix = prefix
    assert remove_root_marker(prefix, root_marker=fs.root_marker) == normalized_prefix
