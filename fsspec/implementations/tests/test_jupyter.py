import os
import shlex
import subprocess
import time
from unittest.mock import Mock

import pytest

import fsspec

pytest.importorskip("notebook")
requests = pytest.importorskip("requests")


@pytest.fixture()
def jupyter(tmpdir):
    tmpdir = str(tmpdir)
    os.environ["JUPYTER_TOKEN"] = "blah"
    try:
        cmd = f'jupyter notebook --notebook-dir="{tmpdir}" --no-browser --port=5566'
        P = subprocess.Popen(shlex.split(cmd))
    except FileNotFoundError:
        pytest.skip("notebook not installed correctly")
    try:
        timeout = 15
        while True:
            try:
                r = requests.get("http://localhost:5566/?token=blah")
                r.raise_for_status()
                break
            except (requests.exceptions.BaseHTTPError, OSError):
                time.sleep(0.1)
                timeout -= 0.1
                if timeout < 0:
                    pytest.xfail("Timed out for jupyter")
        yield "http://localhost:5566/?token=blah", tmpdir
    finally:
        P.terminate()


def test_simple(jupyter):
    url, d = jupyter
    fs = fsspec.filesystem("jupyter", url=url)
    assert fs.ls("") == []

    with pytest.raises(FileNotFoundError):
        fs.ls("not-exist")
    with pytest.raises(FileNotFoundError):
        fs.cat("not-exist")

    fs.pipe("afile", b"data")
    assert fs.cat("afile") == b"data"
    assert "afile" in os.listdir(d)

    with fs.open("bfile", "wb") as f:
        f.write(b"more")
    with fs.open("bfile", "rb") as f:
        assert f.read() == b"more"

    assert fs.info("bfile")["size"] == 4
    fs.rm("afile")

    assert "afile" not in os.listdir(d)

    fs.mv("bfile", "cfile")
    assert "cfile" in os.listdir(d)
    assert "bfile" not in os.listdir(d)


def test_notebook_contents(jupyter):
    # The contents API returns notebooks as parsed JSON (format "json") and
    # files it cannot decode as UTF-8 as base64; cat must give the bytes on disk.
    url, d = jupyter
    fs = fsspec.filesystem("jupyter", url=url)

    nb = (
        b'{\n "cells": [],\n "metadata": {},\n'
        b' "nbformat": 4,\n "nbformat_minor": 5\n}\n'
    )
    with open(os.path.join(d, "nb.ipynb"), "wb") as f:
        f.write(nb)
    assert fs.cat("nb.ipynb") == nb
    with fs.open("nb.ipynb", "rb") as f:
        assert f.read() == nb
    assert fs.info("nb.ipynb")["type"] == "file"


@pytest.mark.parametrize(
    "name",
    ["hash#name", "query?name", "percent%23name", "space name", "unicode-\u00e9"],
)
def test_reserved_path_characters(jupyter, name):
    url, root = jupyter
    fs = fsspec.filesystem("jupyter", url=url, skip_instance_cache=True)
    folder = f"{name}/nested"
    fs.mkdir(folder)
    path = f"{folder}/file.txt"
    fs.pipe_file(path, b"contents")
    assert os.path.isfile(os.path.join(root, path))
    assert fs.cat_file(path) == b"contents"
    assert fs.cat_file(path, start=1, end=4) == b"ont"
    assert fs.ls(folder, detail=False) == [path]
    moved = f"{folder}/moved.txt"
    fs.mv(path, moved)
    assert not os.path.exists(os.path.join(root, path))
    assert fs.cat_file(moved) == b"contents"
    fs.rm(moved)
    assert not os.path.exists(os.path.join(root, moved))


@pytest.mark.parametrize("protocol", ["jupyter", "jlab"])
def test_move_protocol_paths(protocol):
    fs = fsspec.filesystem(
        "jupyter", url="http://localhost:8888", skip_instance_cache=True
    )
    fs.session = Mock()
    fs.mv(f"{protocol}://folder/a#b", f"{protocol}://folder/c%d")
    fs.session.patch.assert_called_once_with(
        "http://localhost:8888/api/contents/folder/a%23b", json={"path": "folder/c%d"}
    )


@pytest.mark.parametrize("protocol", ["jupyter", "jlab"])
def test_move_protocol_same_path(protocol):
    fs = fsspec.filesystem(
        "jupyter", url="http://localhost:8888", skip_instance_cache=True
    )
    fs.session = Mock()
    fs.mv(f"{protocol}://folder/a#b", "folder/a#b")
    fs.session.patch.assert_not_called()


def test_move_bare_paths():
    fs = fsspec.filesystem(
        "jupyter", url="http://localhost:8888", skip_instance_cache=True
    )
    fs.session = Mock()
    fs.mv("folder/a#b", "folder/c%d")
    fs.session.patch.assert_called_once_with(
        "http://localhost:8888/api/contents/folder/a%23b", json={"path": "folder/c%d"}
    )


@pytest.mark.parametrize("protocol", ["jupyter", "jlab"])
def test_move_protocol_paths_on_server(jupyter, protocol):
    url, root = jupyter
    fs = fsspec.filesystem("jupyter", url=url, skip_instance_cache=True)
    fs.mkdir("folder")
    source = "folder/a#b"
    target = "folder/c%d"
    fs.pipe_file(source, b"contents")
    fs.mv(f"{protocol}://{source}", f"{protocol}://{target}")
    assert not os.path.exists(os.path.join(root, source))
    assert fs.cat_file(target) == b"contents"
    fs.mv(f"{protocol}://{target}", target)
    assert fs.cat_file(target) == b"contents"
