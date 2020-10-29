import os
import re
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
    try:
        P = subprocess.Popen(
            shlex.split(
                f"jupyter notebook --notebook-dir={tmpdir}" f" --no-browser --port=5566"
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        pytest.skip("notebook not installed correctly")
    try:
        timeout = 5
        while True:
            try:
                r = requests.get("http://127.0.0.1:5566/")
                r.raise_for_status()
                break
            except (requests.exceptions.BaseHTTPError, IOError):
                time.sleep(0.1)
                timeout -= 0.1
                if timeout < 0:
                    pytest.skip("Timed out for jupyter")
        txt = P.stdout.read(900).decode()

        try:
            url = re.findall("(http[s]*://[^\\n]+)", txt)[0]
        except IndexError:
            pytest.skip("No notebook URL: " + txt)  # debug on fail
        yield url, tmpdir
    finally:
        P.terminate()


def test_simple(jupyter):
    url, d = jupyter
    fs = fsspec.filesystem("jupyter", url=url)
    assert fs.ls("") == []

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
