import paramiko
from fsspec import AbstractFileSystem
from stat import S_ISDIR, S_ISLNK
import urllib.parse


class SFTPFileSystem(AbstractFileSystem):

    def __init__(self, hostname, ssh_kwargs={}):
        """

        Parameters
        ----------
        hostname: str
            Hostname or IP as a string
        ssh_kwargs: dict
            Parameters passed on to connection. See details in
            http://docs.paramiko.org/en/2.4/api/client.html#paramiko.client.SSHClient.connect
        """
        self.client = paramiko.SSHClient()
        self.ftp = self.client.connect(hostname, **ssh_kwargs).open_sftp()

    @staticmethod
    def _get_kwargs_from_urls(path):
        """If kwargs can be encoded in the paths, extract them here

        This should happen before instantiation of the class; incoming paths
        then should be amended to strip the options in methods.

        Examples may look like an sftp path "sftp://user@host:/my/path", where
        the user and host should become kwargs and later get stripped.
        """
        if not isinstance(path, str):
            path = path[0]
        results = urllib.parse.urlparse(path)
        out = {}
        for att in ['username', 'password', 'hostname', 'port']:
            if getattr(results, att):
                out[att] = getattr(results, att)
        return out

    def mkdir(self, path, mode=511):
        self.ftp.mkdir(path, mode)

    def mkdirs(self, path, mode=511):
        parts = path.split('/')
        path = '/'
        for part in parts:
            path += part
            try:
                self.info(path)
            except paramiko.SSHException:
                self.mkdir(path, mode)

    def info(self, path):
        s = self.ftp.stat(path)
        if S_ISDIR(s.st_mode):
            t = 'directory'
        elif S_ISLNK(s.st_mode):
            t = 'link'
        else:
            t = 'file'
        return {'name': path, 'size': s.st_size, 'type': t, 'uid': s.st_uid,
                'gui': s.st_gid, 'time': s.st_atime, 'mtime': s.st_mtime}

    def ls(self, path, detail=False):
        out = self.ftp.listdir(path)
        if detail is False:
            return out
        out2 = []
        for o in out:
            i = self.info(o)
            i['name'] = o
            out2.append(i)
        return out2

    def put(self, lpath, rpath):
        self.ftp.put(lpath, rpath)

    def get(self, rpath, lpath):
        self.ftp.get(rpath, lpath)

    def _open(self, path, mode='rb', block_size=None, **kwargs):
        return self.ftp.open(path, mode,
                             bufsize=block_size if block_size else -1)

    def _rm(self, path):
        if self.isdir(path):
            self.ftp.rmdir(path)
        else:
            self.ftp.remove(path)

    def mv(self, old, new):
        self.ftp.posix_rename(old, new)
