import os
import shlex
import subprocess
import time

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
