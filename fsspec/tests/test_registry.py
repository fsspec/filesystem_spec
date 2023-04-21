import sys
from importlib.metadata import EntryPoint
from unittest.mock import create_autospec, patch

import pytest

import fsspec
from fsspec.implementations.zip import ZipFileSystem
from fsspec.registry import (
    _registry,
    filesystem,
    get_filesystem_class,
    known_implementations,
    register_implementation,
    registry,
)
from fsspec.spec import AbstractFileSystem


@pytest.fixture()
def clear_registry():
    try:
        yield
    finally:
        _registry.clear()
        known_implementations.pop("test", None)


@pytest.fixture()
def clean_imports():
    try:
        real_module = sys.modules["fsspec"]
        del sys.modules["fsspec"]
        yield
    finally:
        sys.modules["fsspec"] = real_module


def test_registry_readonly():
    get_filesystem_class("file")
    assert "file" in registry
    assert "file" in list(registry)
    with pytest.raises(TypeError):
        del registry["file"]
    with pytest.raises(TypeError):
        registry["file"] = None
    with pytest.raises(AttributeError):
        registry.clear()


def test_register_cls(clear_registry):
    with pytest.raises(ValueError):
        get_filesystem_class("test")
    register_implementation("test", AbstractFileSystem)
    cls = get_filesystem_class("test")
    assert cls is AbstractFileSystem


def test_register_str(clear_registry):
    with pytest.raises(ValueError):
        get_filesystem_class("test")
    register_implementation("test", "fsspec.AbstractFileSystem")
    assert "test" not in registry
    cls = get_filesystem_class("test")
    assert cls is AbstractFileSystem
    assert "test" in registry


def test_register_fail(clear_registry):
    register_implementation("test", "doesntexist.AbstractFileSystem")
    with pytest.raises(ImportError):
        get_filesystem_class("test")

    # NOOP
    register_implementation("test", "doesntexist.AbstractFileSystem", clobber=False)
    with pytest.raises(ValueError):
        register_implementation(
            "test", "doesntexist.AbstractFileSystemm", clobber=False
        )

    # by default we do not allow clobbering
    with pytest.raises(ValueError):
        register_implementation("test", "doesntexist.AbstractFileSystemm")

    register_implementation(
        "test", "doesntexist.AbstractFileSystem", errtxt="hiho", clobber=True
    )
    with pytest.raises(ImportError) as e:
        get_filesystem_class("test")
    assert "hiho" in str(e.value)
    register_implementation("test", AbstractFileSystem)

    # NOOP
    register_implementation("test", AbstractFileSystem)
    with pytest.raises(ValueError):
        register_implementation("test", ZipFileSystem)
    register_implementation("test", AbstractFileSystem, clobber=True)
    assert isinstance(fsspec.filesystem("test"), AbstractFileSystem)


def test_entry_points_registered_on_import(clear_registry, clean_imports):
    mock_ep = create_autospec(EntryPoint, module="fsspec.spec.AbstractFileSystem")
    mock_ep.name = "test"  # this can't be set in the constructor...
    mock_ep.value = "fsspec.spec.AbstractFileSystem"
    import_location = "importlib.metadata.entry_points"
    with patch(import_location, return_value={"fsspec.specs": [mock_ep]}):
        assert "test" not in registry
        import fsspec  # noqa

        get_filesystem_class("test")
        assert "test" in registry


def test_filesystem_warning_arrow_hdfs_deprecated(clear_registry, clean_imports):
    mock_ep = create_autospec(EntryPoint, module="fsspec.spec.AbstractFileSystem")
    mock_ep.name = "arrow_hdfs"  # this can't be set in the constructor...
    mock_ep.value = "fsspec.spec.AbstractFileSystem"
    import_location = "importlib.metadata.entry_points"
    with patch(import_location, return_value={"fsspec.specs": [mock_ep]}):
        import fsspec  # noqa

        with pytest.warns(DeprecationWarning):
            filesystem("arrow_hdfs")
