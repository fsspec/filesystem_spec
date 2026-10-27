import subprocess
import sys

import pytest


def test_dbfs_does_not_require_typing_extensions():
    pytest.importorskip("requests")
    script = """
import sys
from unittest.mock import patch

sys.modules["typing_extensions"] = None

from fsspec import get_filesystem_class

with patch("requests.Session.request", side_effect=AssertionError("Unexpected request")):
    cls = get_filesystem_class("dbfs")
    fs = cls(instance="example.invalid", token="synthetic-token", skip_instance_cache=True)
    try:
        assert fs.instance == "example.invalid"
        assert fs._ls_from_cache("/missing") is None
    finally:
        fs.session.close()
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
