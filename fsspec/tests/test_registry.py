import pytest
from fsspec.registry import (
    get_filesystem_class,
    _registry,
    registry,
    register_implementation,
    ReadOnlyError,
    known_implementations,
)
from fsspec.spec import AbstractFileSystem


@pytest.mark.parametrize(
    "protocol,module,minversion,oldversion",
    [("s3", "s3fs", "0.3.0", "0.1.0"), ("gs", "gcsfs", "0.3.0", "0.1.0")],
)
def test_minversion_s3fs(protocol, module, minversion, oldversion, monkeypatch):
    _registry.clear()
    mod = pytest.importorskip(module, minversion)

    assert get_filesystem_class("s3") is not None
    _registry.clear()

    monkeypatch.setattr(mod, "__version__", oldversion)
    with pytest.raises(RuntimeError, match=minversion):
        get_filesystem_class(protocol)


def test_registry_readonly():
    get_filesystem_class("file")
    assert "file" in registry
    assert "file" in list(registry)
    with pytest.raises(ReadOnlyError):
        del registry["file"]
    with pytest.raises(ReadOnlyError):
        registry["file"] = None
    with pytest.raises(ReadOnlyError):
        registry.clear()


def test_register_cls():
    try:
        with pytest.raises(ValueError):
            get_filesystem_class("test")
        register_implementation("test", AbstractFileSystem)
        cls = get_filesystem_class("test")
        assert cls is AbstractFileSystem
    finally:
        _registry.clear()


def test_register_str():
    try:
        with pytest.raises(ValueError):
            get_filesystem_class("test")
        register_implementation("test", "fsspec.AbstractFileSystem")
        assert "test" not in registry
        cls = get_filesystem_class("test")
        assert cls is AbstractFileSystem
        assert "test" in registry
    finally:
        _registry.clear()
        known_implementations.pop("test", None)
