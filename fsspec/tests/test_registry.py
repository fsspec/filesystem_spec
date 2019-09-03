import pytest
from fsspec.registry import get_filesystem_class, registry


@pytest.mark.parametrize("protocol,module,minversion,oldversion", [
    ("s3", "s3fs", "0.3.0", "0.1.0"),
    ("gs", "gcsfs", "0.3.0", "0.1.0"),
])
def test_minversion_s3fs(protocol, module, minversion, oldversion,
                         monkeypatch):
    registry.clear()
    mod = pytest.importorskip(module, minversion)

    assert get_filesystem_class("s3") is not None
    registry.clear()

    monkeypatch.setattr(mod, "__version__", oldversion)
    with pytest.raises(RuntimeError, match=minversion):
        get_filesystem_class(protocol)
