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


@pytest.fixture()
def clear_registry():
    try:
        yield
    finally:
        _registry.clear()
        known_implementations.pop("test", None)


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

    with pytest.raises(ValueError):
        register_implementation("test", "doesntexist.AbstractFileSystem")

    register_implementation(
        "test", "doesntexist.AbstractFileSystem", errtxt="hiho", clobber=True
    )
    with pytest.raises(ImportError) as e:
        get_filesystem_class("test")
    assert "hiho" in str(e.value)
    register_implementation("test", AbstractFileSystem)

    with pytest.raises(ValueError):
        register_implementation("test", AbstractFileSystem)
    register_implementation("test", AbstractFileSystem, clobber=True)
