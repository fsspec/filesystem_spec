import pytest

panel = pytest.importorskip('panel')


def test_basic():
    import fsspec.gui

    gui = fsspec.gui.FileSelector()
    assert 'url' in str(gui.panel)
