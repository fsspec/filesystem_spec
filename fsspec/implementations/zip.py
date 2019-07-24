from __future__ import print_function, division, absolute_import

import zipfile
from fsspec import AbstractFileSystem, open_files
from fsspec.utils import tokenize, DEFAULT_BLOCK_SIZE


class ZipFileSystem(AbstractFileSystem):
    """Read contents of ZIP archive as a file-system

    Keeps file object open while instance lives
    """
    root_marker = ""

    def __init__(self, fo='', mode='r', **storage_options):
        """
        Parameters
        ----------
        fo: str or file-like
            Contains ZIP, and must exist. If a str, will fetch file using
            `open_files()`, which must return one file exactly.
        mode: str
            Currently, only 'r' accepted
        storage_options: key-value
            May be credentials, e.g., `{'auth': ('username', 'pword')}` or any
            other parameters for requests
        """
        AbstractFileSystem.__init__(self)
        if mode != 'r':
            raise ValueError("Only read from zip files accepted")
        if isinstance(fo, str):
            files = open_files(fo)
            if len(files) != 1:
                raise ValueError('Path "{}" did not resolve to exactly'
                                 'one file: "{}"'.format(fo, files))
            fo = files[0]
        self.fo = fo.__enter__()  # the whole instance is a context
        self.zip = zipfile.ZipFile(self.fo)
        self.block_size = storage_options.pop('block_size', DEFAULT_BLOCK_SIZE)
        self.kwargs = storage_options
        self.dir_cache = None

    @classmethod
    def _strip_protocol(cls, path):
        # zip file paths are always relative to the archive root
        return super()._strip_protocol(path).lstrip('/')

    def _get_dirs(self):
        if self.dir_cache is None:
            files = self.zip.infolist()
            self.dir_cache = {}
            for z in files:
                f = {s: getattr(z, s) for s in zipfile.ZipInfo.__slots__}
                f.update({'name': z.filename, 'size': z.file_size,
                          'type': ('directory' if z.is_dir() else 'file')})
                self.dir_cache[f['name']] = f

    def ls(self, path, detail=False):
        self._get_dirs()
        paths = {}
        for p, f in self.dir_cache.items():
            p = p.rstrip('/')
            if '/' in p:
                root = p.rsplit('/', 1)[0]
            else:
                root = ''
            if root == path.rstrip('/'):
                paths[p] = f
            elif path and all(
                    (a == b) for a, b
                    in zip(path.split('/'), p.strip('/').split('/'))):
                # implicit directory
                ppath = '/'.join(p.split('/')[:len(path.split('/')) + 1])
                if ppath not in paths:
                    out = {'name': ppath + '/', 'size': 0, 'type': 'directory'}
                    paths[ppath] = out

            elif all((a == b) for a, b
                     in zip(path.split('/'), [''] + p.strip('/').split('/'))):
                # root directory entry
                ppath = p.rstrip('/').split('/', 1)[0]
                if ppath not in paths:
                    out = {'name': ppath + '/', 'size': 0, 'type': 'directory'}
                    paths[ppath] = out
        out = list(paths.values())
        if detail:
            return out
        else:
            return list(sorted(f['name'] for f in out))

    def cat(self, path):
        return self.zip.read(path)

    def _open(self, path, mode='rb', **kwargs):
        if mode != 'rb':
            raise NotImplementedError
        info = self.info(path)
        out = self.zip.open(path, 'r')
        out.size = info['size']
        out.name = info['name']
        return out

    def ukey(self, path):
        return tokenize(path, self.kwargs, self.protocol)
