import pytest

import fsspec


@pytest.mark.parametrize("gist_id", ["9406787cc6b3aa55e38a54dd6d4c0a28"])
def test_gist_public(gist_id):
    fs = fsspec.filesystem("gist", gist_id=gist_id)
    # Listing
    all_files = fs.ls("")
    assert len(all_files) > 0
    # Cat
    data = fs.cat(all_files)
    assert set(data.keys()) == set(all_files)
    for k, v in data.items():
        assert isinstance(v, bytes)
