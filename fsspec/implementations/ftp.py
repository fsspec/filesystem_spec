from ftplib import FTP, Error
import uuid
from ..spec import AbstractBufferedFile, AbstractFileSystem
from ..utils import infer_storage_options


class FTPFileSystem(AbstractFileSystem):
    """A filesystem over classic """
    root_marker = '/'

    def __init__(self, host, port=21, username=None, password=None,
                 acct=None, block_size=None, tempdir='/tmp', **kwargs):
        """
        You can use _get_kwargs_from_urls to get some kwargs from
        a reasonable FTP url.

        Authentication will be anonymous if username/password are not
        given.

        Parameters
        ----------
        host: str
            The remote server name/ip to connect to
        port: int
            Port to connect with
        username: str or None
            If authenticating, the user's identifier
        password: str of None
            User's password on the server, if using
        acct: str or None
            Some servers also need an "account" string for auth
        block_size: int or None
            If given, the read-ahead or write buffer size.
        tempdir: str
            Directory on remote to put temporary files when in a transaction
        """
        super(FTPFileSystem, self).__init__(**kwargs)
        self.host = host
        self.port = port
        self.tempdir = tempdir
        self.cred = username, password, acct
        if block_size is not None:
            self.blocksize = block_size
        self._connect()

    def _connect(self):
        self.ftp = FTP()
        self.ftp.connect(self.host, self.port)
        self.ftp.login(*self.cred)

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('ftp')
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._connect()

    @classmethod
    def _strip_protocol(cls, path):
        return '/' + infer_storage_options(path)['path'].lstrip('/').rstrip('/')

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        out = infer_storage_options(urlpath)
        out.pop('path', None)
        out.pop('protocol', None)
        return out

    def invalidate_cache(self, path=None):
        if path is not None:
            self.dircache.pop(path, None)
        else:
            self.dircache.clear()

    def ls(self, path, detail=True):
        path = self._strip_protocol(path)
        out = []
        if path not in self.dircache:
            try:
                out = list(self.ftp.mlsd(path))
                for fn, details in out:
                    if path == '/':
                        path = ''  # just for forming the names, below
                    if fn in ['.', '..']:
                        continue
                    details['name'] = '/'.join([path, fn.lstrip('/')])
                    if details['type'] == 'file':
                        details['size'] = int(details['size'])
                    else:
                        details['size'] = 0
                self.dircache[path] = out
            except Error:
                try:
                    info = self.info(path)
                    if info['type'] == 'file':
                        out = [(path, info)]
                except (Error, IndexError):
                    raise FileNotFoundError
        files = self.dircache.get(path, out)
        if not detail:
            return sorted([fn for fn, details in files])
        return [details for fn, details in files]

    def info(self, path, **kwargs):
        # implement with direct method
        path = self._strip_protocol(path)
        files = self.ls(self._parent(path), True)
        return [f for f in files if f['name'] == path][0]

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        path = self._strip_protocol(path)
        block_size = block_size or self.blocksize
        return FTPFile(self, path, mode=mode, block_size=block_size,
                       tempdir=self.tempdir, autocommit=autocommit)

    def _rm(self, path):
        path = self._strip_protocol(path)
        self.ftp.delete(path)
        self.invalidate_cache(path.rsplit('/', 1)[0])

    def mkdir(self, path, **kwargs):
        path = self._strip_protocol(path)
        self.ftp.mkd(path)

    def rmdir(self, path):
        path = self._strip_protocol(path)
        self.ftp.rmd(path)

    def mv(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        self.ftp.rename(path1, path2)
        self.invalidate_cache(self._parent(path1))
        self.invalidate_cache(self._parent(path2))

    def __del__(self):
        self.ftp.close()


class TransferDone(Exception):
    """Internal exception to break out of transfer"""
    pass


class FTPFile(AbstractBufferedFile):
    """Interact with a remote FTP file with read/write buffering"""

    def __init__(self, fs, path, **kwargs):
        super().__init__(fs, path, **kwargs)
        if kwargs.get('autocommit', False) is False:
            self.target = self.path
            self.path = '/'.join([kwargs['tempdir'], str(uuid.uuid4())])

    def commit(self):
        self.fs.mv(self.path, self.target)

    def discard(self):
        self.fs.rm(self.path)

    def _fetch_range(self, start, end):
        """Get bytes between given byte limits

        Implemented by raising an exception in the fetch callback when the
        number of bytes received reaches the requested amount.

        With fail if the server does not respect the REST command on
        retrieve requests.
        """
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
            self.fs.ftp.abort()
            self.fs.ftp.voidresp()
        return b''.join(out)

    def _upload_chunk(self, final=False):
        self.buffer.seek(0)
        self.fs.ftp.storbinary("STOR " + self.path, self.buffer,
                               blocksize=2**16, rest=self.offset)
        return True
