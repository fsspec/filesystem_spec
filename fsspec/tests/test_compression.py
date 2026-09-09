import io
import pathlib
import sys

import pytest

import fsspec.core
from fsspec.compression import compr, register_compression
from fsspec.utils import compressions, infer_compression


@pytest.mark.parametrize("compression", ["gzip", "bz2", "lzma"])
@pytest.mark.parametrize("mode", ["rb", "rt", "wb", "wt"])
@pytest.mark.parametrize("use_context", [False, True])
def test_fs_open_closes_raw_file(compression, mode, use_context, monkeypatch):
    codec = pytest.importorskip(compression)
    data = b"hello\nworld\n"
    raw = io.BytesIO(codec.compress(data) if "r" in mode else b"")
    fs = fsspec.AbstractFileSystem()
    monkeypatch.setattr(fs, "_open", lambda *args, **kwargs: raw)

    f = fs.open("test", mode, compression=compression)
    try:
        value = data if "b" in mode else data.decode()
        if "r" in mode:
            assert f.read() == value
        else:
            f.write(value)
        if use_context:
            with f:
                pass
        else:
            f.close()
        assert f.closed
        assert raw.closed
        f.close()
    finally:
        f.close()
        raw.close()


@pytest.mark.parametrize("compression", ["gzip", "bz2", "lzma"])
@pytest.mark.parametrize("mode", ["wb", "wt"])
def test_fs_open_propagates_raw_close_error(compression, mode, monkeypatch):
    pytest.importorskip(compression)

    class FailingCloseFile(io.BytesIO):
        def close(self):
            if not self.closed:
                super().close()
                raise OSError("upload failed on close")

    raw = FailingCloseFile()
    fs = fsspec.AbstractFileSystem()
    monkeypatch.setattr(fs, "_open", lambda *args, **kwargs: raw)

    try:
        with pytest.raises(OSError, match="upload failed on close"):
            with fs.open("test", mode, compression=compression) as f:
                f.write(b"data" if "b" in mode else "data")
        assert raw.closed
    finally:
        # Suppress the expected error while cleaning up on the unfixed version.
        if not raw.closed:
            with pytest.raises(OSError, match="upload failed on close"):
                raw.close()


@pytest.mark.parametrize("compression", ["gzip", "bz2", "lzma"])
def test_fs_open_closes_raw_file_on_compression_error(compression, monkeypatch):
    pytest.importorskip(compression)
    raw = io.BytesIO()
    fs = fsspec.AbstractFileSystem()
    monkeypatch.setattr(fs, "_open", lambda *args, **kwargs: raw)
    f = fs.open("test", "wb", compression=compression)
    f.write(b"data")

    def fail_write(data):
        raise OSError("writing compression trailer failed")

    monkeypatch.setattr(raw, "write", fail_write)
    try:
        with pytest.raises(OSError, match="writing compression trailer failed"):
            f.close()
        assert raw.closed
    finally:
        raw.close()


@pytest.mark.parametrize("compression", ["gzip", "bz2", "lzma"])
@pytest.mark.parametrize("commit", [False, True])
def test_fs_open_compression_transaction(compression, commit, tmp_path):
    codec = pytest.importorskip(compression)
    fs = fsspec.filesystem("file")
    path = tmp_path / "test"
    fs.start_transaction()
    transaction = fs.transaction
    try:
        with fs.open(path, "wt", compression=compression, newline="\n") as f:
            f.write("hello\nworld\n")
        assert not path.exists()
        assert len(transaction.files) == 1
        assert transaction.files[0].closed
    finally:
        transaction.complete(commit=commit)
    assert path.exists() == commit
    if commit:
        assert codec.decompress(path.read_bytes()) == b"hello\nworld\n"


def test_fs_open_infer_no_compression(monkeypatch):
    raw = io.BytesIO(b"data")
    fs = fsspec.AbstractFileSystem()
    monkeypatch.setattr(fs, "_open", lambda *args, **kwargs: raw)
    with fs.open("test.unknown", compression="infer") as f:
        assert f is raw
        assert f.read() == b"data"


@pytest.mark.parametrize("compression", ["gzip", "bz2", "lzma"])
def test_fs_open_compression_file_methods(compression, monkeypatch):
    codec = pytest.importorskip(compression)
    data = b"hello\nworld\n"
    raw = io.BytesIO(codec.compress(data))
    fs = fsspec.AbstractFileSystem()
    monkeypatch.setattr(fs, "_open", lambda *args, **kwargs: raw)
    with fs.open("test", "rb", compression=compression) as f:
        assert list(f) == [b"hello\n", b"world\n"]
        f.seek(0)
        buffer = bytearray(len(data))
        assert f.readinto(buffer) == len(data)
        assert buffer == data
    with pytest.raises(ValueError):
        with f:
            pass


@pytest.mark.parametrize("compression", list(compr))
@pytest.mark.parametrize("mode", ["b", "t"])
def test_fs_open_compression_roundtrip(compression, mode, tmp_path):
    fs = fsspec.filesystem("file")
    path = tmp_path / "test"
    data = b"hello\nworld\n" if mode == "b" else "hello\nworld\n"
    with fs.open(path, "w" + mode, compression=compression) as f:
        f.write(data)
    with fs.open(path, "r" + mode, compression=compression) as f:
        assert f.read() == data


@pytest.mark.parametrize("mode", ["rb", "rt", "wb", "wt"])
def test_fs_open_zstandard_closes_raw_once(mode, monkeypatch):
    zstd = pytest.importorskip("zstandard")

    def compress(raw, mode):
        if mode == "r":
            return zstd.ZstdDecompressor().stream_reader(raw)
        return zstd.ZstdCompressor().stream_writer(raw)

    class CountingFile(io.BytesIO):
        close_calls = 0

        def close(self):
            self.close_calls += 1
            super().close()

    data = b"data"
    raw = CountingFile(zstd.ZstdCompressor().compress(data) if "r" in mode else b"")
    fs = fsspec.AbstractFileSystem()
    monkeypatch.setattr(fs, "_open", lambda *args, **kwargs: raw)
    monkeypatch.setitem(compr, "zstd", compress)
    with fs.open("test", mode, compression="zstd") as f:
        value = data if "b" in mode else data.decode()
        if "r" in mode:
            assert f.read() == value
        else:
            f.write(value)
    f.close()
    assert raw.closed
    assert raw.close_calls == 1


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
    assert infer_compression("fn.lzma") == "lzma"


def test_lz4_compression(tmpdir):
    """Infer lz4 compression for .lz4 files if lz4 is available."""
    tmp_path = pathlib.Path(str(tmpdir))

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


def test_zstd_compression(tmpdir):
    """Infer zstd compression for .zst files if zstandard is available."""
    tmp_path = pathlib.Path(str(tmpdir))

    try:
        if sys.version_info >= (3, 14):
            from compression import zstd
        else:
            zstd = pytest.importorskip("backports.zstd")
    except ImportError:
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

    # fails in https://github.com/fsspec/filesystem_spec/issues/725
    infile = fsspec.core.open(
        str(tmp_path / "out.zst"), mode="rb", compression="infer"
    ).open()

    infile.close()


def test_snappy_compression(tmpdir):
    """No registered compression for snappy, but can be specified."""
    tmp_path = pathlib.Path(str(tmpdir))

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
