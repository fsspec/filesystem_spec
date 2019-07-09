"""Helper functions for a standard streaming compression API"""
from __future__ import print_function, division, absolute_import

from bz2 import BZ2File
from gzip import GzipFile
from zipfile import ZipFile


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
