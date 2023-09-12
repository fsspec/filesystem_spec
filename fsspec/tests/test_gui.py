import pytest

panel = pytest.importorskip("panel")


def test_basic():
    import fsspec.gui

    gui = fsspec.gui.FileSelector()
    assert "url" in str(gui.panel)


def test_kwargs(tmpdir):
    """ confirm kwargs are passed to the filesystem instance"""
    import fsspec.gui

    gui = fsspec.gui.FileSelector(f"file://{tmpdir}", 
                                  kwargs="{'auto_mkdir': True}")
    
    assert gui.fs.auto_mkdir
