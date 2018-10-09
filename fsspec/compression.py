from __future__ import print_function, division, absolute_import

import bz2
from gzip import GzipFile


def noop_file(file, **kwargs):
    return file


compr = {'gzip': lambda f, **kwargs: GzipFile(fileobj=f, **kwargs),
         None: noop_file, 'bz2': bz2.BZ2File}

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
