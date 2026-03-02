import pytest
import fsspec
from fsspec.implementations.http import HTTPFileSystem


def test_http_output(tmp_path):
    """Test that HTTP get with query params doesn't create malformed dest path"""
    kwargs = {}
    fs = fsspec.implementations.http.HTTPFileSystem(fsspec.filesystem("https", **kwargs))
    expected_output_path = str(tmp_path / "outputfile.txt")
    rpath = "https://httpbin.org/gzip?test=value"
    lpath = expected_output_path
    fs.get(rpath, lpath)
    
    # Verify the file was created at the exact expected path
    assert (tmp_path / "outputfile.txt").exists()
    assert not (tmp_path / "outputfile.txt" / "gzip?test=value").exists()
