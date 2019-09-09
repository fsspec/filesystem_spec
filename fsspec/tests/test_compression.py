from fsspec.compression import infer_compression, compr, compr_extensions


def test_infer_custom_compression():
    """Inferred compression gets values from fsspec.compression.compr."""
    assert infer_compression("fn.zip") == "zip"
    assert infer_compression("fn.gz") == "gzip"
    assert infer_compression("fn.gzip") == "gzip"
    assert infer_compression("fn.unknown") is None
    assert infer_compression("fn.test_custom") is None
    assert infer_compression("fn.tst") is None

    compr["test_custom"] = lambda f, **kwargs: f
    compr_extensions["tst"] = "test_custom"

    try:
        assert infer_compression("fn.zip") == "zip"
        assert infer_compression("fn.gz") == "gzip"
        assert infer_compression("fn.gzip") == "gzip"
        assert infer_compression("fn.unknown") is None
        assert infer_compression("fn.test_custom") == "test_custom"
        assert infer_compression("fn.tst") == "test_custom"
    finally:
        del compr["test_custom"]
        del compr_extensions["tst"]
