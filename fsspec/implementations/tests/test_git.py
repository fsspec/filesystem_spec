import os
import shutil
import subprocess
import tempfile

import pytest

import fsspec
from fsspec.implementations.local import make_path_posix

pygit2 = pytest.importorskip("pygit2")


@pytest.fixture()
def repo():
    orig_dir = os.getcwd()
    d = tempfile.mkdtemp()
    try:
        os.chdir(d)
        subprocess.call("git init -b master", shell=True, cwd=d)
        subprocess.call("git init -b master", shell=True, cwd=d)
        subprocess.call('git config user.email "you@example.com"', shell=True, cwd=d)
        subprocess.call('git config user.name "Your Name"', shell=True, cwd=d)
        open(os.path.join(d, "file1"), "wb").write(b"data0")
        subprocess.call("git add file1", shell=True, cwd=d)
        subprocess.call('git commit -m "init"', shell=True, cwd=d)
        sha = open(os.path.join(d, ".git/refs/heads/master"), "r").read().strip()
        open(os.path.join(d, "file1"), "wb").write(b"data00")
        subprocess.check_output('git commit -a -m "tagger"', shell=True, cwd=d)
        subprocess.call('git tag -a thetag -m "make tag"', shell=True, cwd=d)
        open(os.path.join(d, "file2"), "wb").write(b"data000")
        subprocess.call("git add file2", shell=True)
        subprocess.call('git commit -m "master tip"', shell=True, cwd=d)
        subprocess.call("git checkout -b abranch", shell=True, cwd=d)
        os.mkdir("inner")
        open(os.path.join(d, "inner", "file1"), "wb").write(b"data3")
        subprocess.call("git add inner/file1", shell=True, cwd=d)
        subprocess.call('git commit -m "branch tip"', shell=True, cwd=d)
        os.chdir(orig_dir)
        yield d, sha
    finally:
        os.chdir(orig_dir)
        shutil.rmtree(d)


def test_refs(repo):
    d, sha = repo
    with fsspec.open("git://file1", path=d, ref=sha) as f:
        assert f.read() == b"data0"

    with fsspec.open("git://file1", path=d, ref="thetag") as f:
        assert f.read() == b"data00"

    with fsspec.open("git://file2", path=d, ref="master") as f:
        assert f.read() == b"data000"

    with fsspec.open("git://file2", path=d, ref=None) as f:
        assert f.read() == b"data000"

    with fsspec.open("git://inner/file1", path=d, ref="abranch") as f:
        assert f.read() == b"data3"


def _check_FileNotFoundError(f, *args, **kwargs):
    with pytest.raises(FileNotFoundError):
        f(*args, **kwargs)


def test_file_existence_checks(repo):
    d, sha = repo

    fs, _ = fsspec.url_to_fs(f"git://{d}:abranch@")

    assert fs.lexists("inner")
    assert fs.exists("inner")
    assert fs.isdir("inner")
    assert fs.info("inner")
    assert fs.ls("inner")

    assert fs.lexists("inner/file1")
    assert fs.exists("inner/file1")
    assert fs.info("inner/file1")
    assert fs.ls("inner/file1")

    assert not fs.lexists("non-existing-file")
    assert not fs.exists("non-existing-file")

    assert not fs.isfile("non-existing-file")
    assert not fs.isdir("non-existing-file")

    _check_FileNotFoundError(fs.info, "non-existing-file")
    _check_FileNotFoundError(fs.size, "non-existing-file")
    _check_FileNotFoundError(fs.ls, "non-existing-file")
    _check_FileNotFoundError(fs.open, "non-existing-file")


def test_url(repo):
    d, sha = repo
    fs, _, paths = fsspec.core.get_fs_token_paths(f"git://file1::file://{d}")
    assert make_path_posix(d) in make_path_posix(fs.repo.path)
    assert paths == ["file1"]
    with fsspec.open(f"git://file1::file://{d}") as f:
        assert f.read() == b"data00"

    fs, _, paths = fsspec.core.get_fs_token_paths(f"git://{d}:master@file1")
    assert make_path_posix(d) in make_path_posix(fs.repo.path)
    assert paths == ["file1"]
    with fsspec.open(f"git://{d}:master@file1") as f:
        assert f.read() == b"data00"
