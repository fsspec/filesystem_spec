"""Helper functions for a standard streaming compression API"""
from bz2 import BZ2File
from gzip import GzipFile
from zipfile import ZipFile
from fsspec.spec import AbstractBufferedFile


def noop_file(file, **kwargs):
    return file


def unzip(infile, mode='rb', filename=None, **kwargs):
    if 'r' not in mode:
        raise ValueError("zip only supported in read mode")
    z = ZipFile(infile)
    if filename is None:
        filename = z.namelist()[0]
    return z.open(filename, mode='r', **kwargs)


# should be functions of the form func(infile, mode=, **kwargs) -> file-like
compr = {'gzip': lambda f, **kwargs: GzipFile(fileobj=f, **kwargs),
         None: noop_file,
         'bz2': BZ2File,
         'zip': unzip}

try:
    import lzma
    compr['xz'] = lzma.LZMAFile
except ImportError:
    pass

try:
    import lzmaffi
    compr['xz'] = lzmaffi.LZMAFile
except ImportError:
    pass


class SnappyFile(AbstractBufferedFile):

    def __init__(self, infile, mode, **kwargs):
        import snappy
        self.details = {'size': 999999999}   # not true, but OK if we don't seek
        super().__init__(fs=None, path='snappy', mode=mode.strip('b') + 'b',
                         **kwargs)
        self.infile = infile
        if 'r' in mode:
            self.codec = snappy.StreamDecompressor()
        else:
            self.codec = snappy.StreamCompressor()

    def _upload_chunk(self, final=False):
        self.buffer.seek(0)
        out = self.codec.add_chunk(self.buffer.read())
        self.infile.write(out)
        return True

    def seek(self, loc, whence=0):
        raise NotImplementedError("SnappyFile is not seekable")

    def seekable(self):
        return False

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        data = self.infile.read(end - start)
        return self.codec.decompress(data)


try:
    import snappy
    snappy.compress
    compr['snappy'] = SnappyFile

except (ImportError, NameError):
    pass

try:
    import lz4.frame
    compr['lz4'] = lz4.frame.open
except ImportError:
    pass

try:
    import zstandard as zstd

    def zstandard_file(infile, mode='rb'):
        if 'r' in mode:
            cctx = zstd.ZstdDecompressor()
            return cctx.stream_reader(infile)
        else:
            cctx = zstd.ZstdCompressor(level=10)
            return cctx.stream_writer(infile)

    compr['zstd'] = zstandard_file
except ImportError:
    pass
