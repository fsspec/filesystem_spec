from ftplib import FTP
from ..spec import AbstractBufferedFile, AbstractFileSystem


class FTPFileSystem(AbstractFileSystem):

    def __init__(self, host, port=21, username=None, password=None,
                 acct=None, block_size=None):
        super(FTPFileSystem, self).__init__()
        self.ftp = FTP()
        self.host = host
        self.port = port
        self.dircache = {}
        if block_size is not None:
            self.blocksize = block_size
        self.ftp.connect(host, port)
        self.ftp.login(username, password, acct)

    def invalidate_cache(self, path=None):
        if path is not None:
            self.dircache.pop(path, None)
        else:
            self.dircache.clear()

    def ls(self, path, detail=True):
        path = path.rstrip('/')
        if path not in self.dircache:
            self.dircache[path] = list(self.ftp.mlsd(path))
        files = self.dircache[path]
        if not detail:
            return sorted(['/'.join([path, f[0]]) for f in files])
        out = []
        for fn, details in sorted(files):
            if fn in ['.', '..']:
                continue
            details['name'] = '/'.join([path, fn])
            if details['type'] == 'file':
                details['size'] = int(details['size'])
            else:
                details['size'] = 0
            out.append(details)
        return out

    def info(self, path):
        # implement with direct method
        parent = path.rsplit('/', 1)[0]
        files = self.ls(parent, True)
        return [f for f in files if f['name'] == path][0]

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        block_size = block_size or self.blocksize
        if mode == 'rb':
            return FTPFile(self, path, mode, block_size=block_size)


class TransferDone(Exception):
    """Internal exception to break out of transfer"""
    pass


class FTPFile(AbstractBufferedFile):

    def _fetch_range(self, start, end):
        out = []
        total = [0]

        def callback(x):
            total[0] += len(x)
            if total[0] > end - start:
                out.append(x[:(end - start) - total[0]])
                self.fs.ftp.abort()
                raise TransferDone
            else:
                out.append(x)

            if total[0] == end - start:
                raise TransferDone

        try:
            self.fs.ftp.retrbinary('RETR %s' % self.path, blocksize=2**16,
                                   rest=start, callback=callback)
        except TransferDone:
            pass
        return b''.join(out)
