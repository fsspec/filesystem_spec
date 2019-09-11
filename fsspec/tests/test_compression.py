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


def test_lz4_compression(tmp_path):
    # type: (pathlib.Path) -> None
    """Infer lz4 compression for .lz4 files if lz4 is available."""

    lz4 = pytest.importorskip("lz4")

    tmp_path.mkdir(exist_ok=True)

    tdat = "foobar" * 100

    with fsspec.core.open(
        str(tmp_path / "out.lz4"), mode="wt", compression="infer"
    ) as outfile:
        outfile.write(tdat)

    compressed = (tmp_path / "out.lz4").open("rb").read()
    assert lz4.frame.decompress(compressed).decode() == tdat

    with fsspec.core.open(
        str(tmp_path / "out.lz4"), mode="rt", compression="infer"
    ) as infile:
        assert infile.read() == tdat

    with fsspec.core.open(
        str(tmp_path / "out.lz4"), mode="rt", compression="lz4"
    ) as infile:
        assert infile.read() == tdat


def test_zstd_compression(tmp_path):
    # type: (pathlib.Path) -> None
    """Infer zstd compression for .zst files if zstandard is available."""

    zstd = pytest.importorskip("zstandard")

    tmp_path.mkdir(exist_ok=True)

    tdat = "foobar" * 100

    with fsspec.core.open(
        str(tmp_path / "out.zst"), mode="wt", compression="infer"
    ) as outfile:
        outfile.write(tdat)

    compressed = (tmp_path / "out.zst").open("rb").read()
    assert zstd.ZstdDecompressor().decompress(compressed, len(tdat)).decode() == tdat

    with fsspec.core.open(
        str(tmp_path / "out.zst"), mode="rt", compression="infer"
    ) as infile:
        assert infile.read() == tdat

    with fsspec.core.open(
        str(tmp_path / "out.zst"), mode="rt", compression="zstd"
    ) as infile:
        assert infile.read() == tdat


def test_snappy_compression(tmp_path):
    """No registered compression for snappy, but can be specified."""
    # type: (pathlib.Path) -> None

    snappy = pytest.importorskip("snappy")

    tmp_path.mkdir(exist_ok=True)

    tdat = "foobar" * 100

    # Snappy isn't inferred.
    with fsspec.core.open(
        str(tmp_path / "out.snappy"), mode="wt", compression="infer"
    ) as outfile:
        outfile.write(tdat)
    assert (tmp_path / "out.snappy").open("rb").read().decode() == tdat

    # but can be specified.
    with fsspec.core.open(
        str(tmp_path / "out.snappy"), mode="wt", compression="snappy"
    ) as outfile:
        outfile.write(tdat)

    compressed = (tmp_path / "out.snappy").open("rb").read()
    assert snappy.StreamDecompressor().decompress(compressed).decode() == tdat

    with fsspec.core.open(
        str(tmp_path / "out.snappy"), mode="rb", compression="infer"
    ) as infile:
        assert infile.read() == compressed

    with fsspec.core.open(
        str(tmp_path / "out.snappy"), mode="rt", compression="snappy"
    ) as infile:
        assert infile.read() == tdat
