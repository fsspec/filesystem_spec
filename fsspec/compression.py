"""Helper functions for a standard streaming compression API"""
from bz2 import BZ2File
from lzma import LZMAFile
from zipfile import ZipFile

import cramjam

import fsspec.utils
from fsspec.spec import AbstractBufferedFile


def noop_file(file, mode, **kwargs):
    return file


# TODO: files should also be available as contexts
# should be functions of the form func(infile, mode=, **kwargs) -> file-like
compr = {None: noop_file}


def register_compression(name, callback, extensions, force=False):
    """Register an "inferable" file compression type.

    Registers transparent file compression type for use with fsspec.open.
    Compression can be specified by name in open, or "infer"-ed for any files
    ending with the given extensions.

    Args:
        name: (str) The compression type name. Eg. "gzip".
        callback: A callable of form (infile, mode, **kwargs) -> file-like.
            Accepts an input file-like object, the target mode and kwargs.
            Returns a wrapped file-like object.
        extensions: (str, Iterable[str]) A file extension, or list of file
            extensions for which to infer this compression scheme. Eg. "gz".
        force: (bool) Force re-registration of compression type or extensions.

    Raises:
        ValueError: If name or extensions already registered, and not force.

    """
    if isinstance(extensions, str):
        extensions = [extensions]

    # Validate registration
    if name in compr and not force:
        raise ValueError("Duplicate compression registration: %s" % name)

    for ext in extensions:
        if ext in fsspec.utils.compressions and not force:
            raise ValueError(
                "Duplicate compression file extension: %s (%s)" % (ext, name)
            )

    compr[name] = callback

    for ext in extensions:
        fsspec.utils.compressions[ext] = name


def unzip(infile, mode="rb", filename=None, **kwargs):
    if "r" not in mode:
        filename = filename or "file"
        z = ZipFile(infile, mode="w", **kwargs)
        fo = z.open(filename, mode="w")
        fo.close = lambda closer=fo.close: closer() or z.close()
        return fo
    z = ZipFile(infile)
    if filename is None:
        filename = z.namelist()[0]
    return z.open(filename, mode="r", **kwargs)


def buffered_file_factory(codec):
    """
    Factory for cramjam submodule to BufferedFile interface.
    """

    class BufferedFile(AbstractBufferedFile):
        def __init__(self, infile, mode="rb", **kwargs):
            super().__init__(
                fs=None, path="", mode=mode.strip("b") + "b", size=999999999, **kwargs
            )
            self.infile = infile
            self.codec = codec
            self.compressor = codec.Compressor()

        def _upload_chunk(self, final=False):
            self.buffer.seek(0)
            n_bytes = self.compressor.compress(self.buffer.read())
            out = self.compressor.finish() if final else self.compressor.flush()
            self.infile.write(out)
            return n_bytes

        def _fetch_range(self, start, end):
            """Get the specified set of bytes from remote"""
            data = self.infile.read(end - start)
            if not data:
                return b""
            return bytes(self.codec.decompress(data))

    return type(f"{codec.__name__.capitalize()}File", (BufferedFile,), dict())


register_compression("zip", unzip, "zip")
register_compression("lzma", LZMAFile, "xz")
register_compression("xz", LZMAFile, "xz", force=True)
register_compression("bz2", BZ2File, "bz2")
register_compression("gzip", buffered_file_factory(cramjam.gzip), "gz")
register_compression("snappy", buffered_file_factory(cramjam.snappy), "snappy")
register_compression("lz4", buffered_file_factory(cramjam.lz4), "lz4")
register_compression("zstd", buffered_file_factory(cramjam.zstd), "zst")
register_compression("brotli", buffered_file_factory(cramjam.brotli), "br")


try:  # pragma: no cover
    from isal import igzip

    # igzip is meant to be used as a faster drop in replacement to gzip
    # so its api and functions are the same as the stdlib’s module. Except
    # where ISA-L does not support the same calls as zlib
    # (See https://python-isal.readthedocs.io/).
    register_compression("gzip", igzip.IGzipFile, "gz", force=True)
except ImportError:
    pass

try:
    import lzmaffi

    register_compression("lzma", lzmaffi.LZMAFile, "xz", force=True)
    register_compression("xz", lzmaffi.LZMAFile, "xz", force=True)
except ImportError:
    pass


def available_compressions():
    """Return a list of the implemented compressions."""
    return list(compr)
