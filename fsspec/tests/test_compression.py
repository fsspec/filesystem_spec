import builtins
import pathlib

import pytest

import fsspec.core
from fsspec.compression import compr, register_compression
from fsspec.utils import compressions, infer_compression


def test_infer_custom_compression():
    """Inferred compression gets values from fsspec.compression.compr."""
    assert infer_compression("fn.zip") == "zip"
    assert infer_compression("fn.gz") == "gzip"
    assert infer_compression("fn.unknown") is None
    assert infer_compression("fn.test_custom") is None
    assert infer_compression("fn.tst") is None

    register_compression("test_custom", lambda f, **kwargs: f, "tst")

    try:
        assert infer_compression("fn.zip") == "zip"
        assert infer_compression("fn.gz") == "gzip"
        assert infer_compression("fn.unknown") is None
        assert infer_compression("fn.test_custom") is None
        assert infer_compression("fn.tst") == "test_custom"

        # Duplicate registration in name or extension raises a value error.
        with pytest.raises(ValueError):
            register_compression("test_custom", lambda f, **kwargs: f, "tst")

        with pytest.raises(ValueError):
            register_compression("test_conflicting", lambda f, **kwargs: f, "tst")
        assert "test_conflicting" not in compr

        # ...but can be forced.
        register_compression(
            "test_conflicting", lambda f, **kwargs: f, "tst", force=True
        )
        assert infer_compression("fn.zip") == "zip"
        assert infer_compression("fn.gz") == "gzip"
        assert infer_compression("fn.unknown") is None
        assert infer_compression("fn.test_custom") is None
        assert infer_compression("fn.tst") == "test_conflicting"

    finally:
        del compr["test_custom"]
        del compr["test_conflicting"]
        del compressions["tst"]


def test_infer_uppercase_compression():
    assert infer_compression("fn.ZIP") == "zip"
    assert infer_compression("fn.GZ") == "gzip"
    assert infer_compression("fn.UNKNOWN") is None
    assert infer_compression("fn.TEST_UPPERCASE") is None
    assert infer_compression("fn.TEST") is None


def test_lzma_compression_name():
    pytest.importorskip("lzma")
    assert infer_compression("fn.xz") == "xz"


@pytest.mark.parametrize(
    "variant_name", ("gzip", "snappy", "lz4", "zstd", "brotli", "deflate")
)
@pytest.mark.parametrize("infer", (True, False), ids=lambda v: f"infer:{v}")
def test_compression_variant(tmpdir, variant_name, infer):
    cramjam = pytest.importorskip("cramjam")
    variant = getattr(cramjam, variant_name)
    variant_suffixes = {v: k for k, v in compressions.items()}
    suffix = variant_suffixes[variant_name]

    tmp_path = pathlib.Path(str(tmpdir))
    tmp_path.mkdir(exist_ok=True)
    f_path = tmp_path / f"out.{suffix}"

    data = "foobar" * 100
    compression = "infer" if infer else variant_name

    with fsspec.core.open(f_path, mode="wt", compression=compression) as outfile:
        outfile.write(data)

    decompressed = variant.decompress(f_path.read_bytes())
    assert bytes(decompressed).decode() == data

    with fsspec.core.open(f_path, mode="rt", compression=compression) as infile:
        assert infile.read() == data


@pytest.mark.parametrize("variant", ("snappy", "lz4", "zstd", "brotli", "deflate"))
def test_optional_compression_variants(monkeypatch, tmpdir, variant):
    """
    When attempting compression using an optional algorithm, a sensible message
    is raised in how to remedy the error.
    """
    # ensure cramjam looks like it's not present
    def importer(name, *args, **kwargs):
        if name == "cramjam":
            raise ImportError()
        return original_importer(name, *args, **kwargs)

    original_importer = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", importer)

    f_path = pathlib.Path(str(tmpdir)).joinpath("out")
    f_path.touch()

    with pytest.raises(ImportError) as e:
        with fsspec.core.open(f_path, mode="wb", compression=variant) as f:
            f.write(b"data")
        expected = (
            f"`{variant}` not available, install `cramjam` "
            "to access it and other compression algorithms."
        )
        assert expected == e.value.msg
