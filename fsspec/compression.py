"""Helper functions for a standard streaming compression API"""
from __future__ import print_function, division, absolute_import

from bz2 import BZ2File
from gzip import GzipFile


def noop_file(file, **kwargs):
    return file


# should be functions of the form func(infile, mode=, **kwargs) -> file-like
compr = {'gzip': lambda f, **kwargs: GzipFile(fileobj=f, **kwargs),
         None: noop_file,
         'bz2': BZ2File}

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
