import pytest

import fsspec


@pytest.mark.parametrize("gist_id", ["16bee4256595d3b6814be139ab1bd54e"])
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
