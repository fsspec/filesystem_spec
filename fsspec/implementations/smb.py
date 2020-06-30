import smbclient
from stat import S_ISDIR, S_ISLNK
import types
import uuid
from .. import AbstractFileSystem
from ..utils import infer_storage_options


class SMBFileSystem(AbstractFileSystem):
    """Downloads or uploads to Windows and Samba network drives.

    The argument `path` (str) must have a URI with format:
    `smb://workgroup;user:password@server:port/share/folder/file.csv`.

    Note that you need to pass in a valid hostname or IP address for the host
    component of the URL. Do not use the Windows/NetBIOS machine name for the
    host component.

    The first component of the path in the URL points to the name of the shared
    folder. Subsequent path components will point to the directory/folder/file.

    The URL components `workgroup` , `user`, `password` and `port` may be
    optional.

    .. note::

        For working this source require `smbprotocol`_ to be installed, e.g.::

            $ pip install smbprotocol[kerberos]

    .. _smbprotocol: https://github.com/jborean93/smbprotocol#requirements

    Note: if using this with the ``open`` or ``open_files``, with full URLs,
    there is no way to tell if a path is relative, so all paths are assumed
    to be absolute.
    """

    protocol = "smb"

    def __init__(
        self,
        host,
        port=None,
        username=None,
        password=None,
        timeout=60,
        encrypt=None,
        **kwargs
    ):
        """
        You can use _get_kwargs_from_urls to get some kwargs from
        a reasonable SMB url.

        Authentication will be anonymous or integrated if username/password are not
        given.

        Parameters
        ----------
        host: str
            The remote server name/ip to connect to
        port: int
            Port to connect with. Usually 445, sometimes 139.
        username: str or None
            Username to connect with. Required if Kerberos auth is not being used.
        password: str of None
            User's password on the server, if using username
        timeout: int
            Connection timeout in seconds
        encrypt: bool
            Whether to force encryption or not, once this has been set to True
            the session cannot be changed back to False.
        """
        super(SMBFileSystem, self).__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.encrypt = encrypt
        self.temppath = kwargs.pop("temppath", "")
        self._connect()

    def _connect(self):
        smbclient.register_session(
            self.host,
            username=self.username,
            password=self.password,
            port=self.port,
            encrypt=self.encrypt,
            connection_timeout=self.timeout,
        )

    def _as_unc_path(self, path):
        rpath = path.replace("/", "\\")
        unc = "\\\\{}{}".format(self.host, rpath)
        return unc

    @classmethod
    def _share_has_path(cls, path):
        parts = path.count("/")
        if path.endswith("/"):
            return parts > 2
        return parts > 1

    @classmethod
    def _strip_protocol(cls, path):
        return infer_storage_options(path)["path"]

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        # smb://workgroup;user:password@host:port/share/folder/file.csv
        out = infer_storage_options(urlpath)
        out.pop("path", None)
        out.pop("protocol", None)
        return out

    def mkdir(self, path, create_parents=True, **kwargs):
        wpath = self._as_unc_path(path)
        if create_parents:
            smbclient.makedirs(wpath, exist_ok=False, **kwargs)
        else:
            smbclient.mkdir(wpath, **kwargs)

    def makedirs(self, path, exist_ok=False):
        if self._share_has_path(path):
            wpath = self._as_unc_path(path)
            smbclient.makedirs(wpath, exist_ok=exist_ok)

    def rmdir(self, path):
        if self._share_has_path(path):
            wpath = self._as_unc_path(path)
            smbclient.rmdir(wpath)

    def info(self, path):
        wpath = self._as_unc_path(path)
        s = smbclient.stat(wpath)
        if S_ISDIR(s.st_mode):
            t = "directory"
        elif S_ISLNK(s.st_mode):
            t = "link"
        else:
            t = "file"
        res = {
            "name": path + "/" if t == "directory" else path,
            "size": s.st_size,
            "type": t,
            "uid": s.st_uid,
            "gid": s.st_gid,
            "time": s.st_atime,
            "mtime": s.st_mtime,
        }
        return res

    def ls(self, path, detail=False):
        unc = self._as_unc_path(path)
        dirs = ["/".join([path.rstrip("/"), p]) for p in smbclient.listdir(unc)]
        if detail:
            dirs = [self.info(d) for d in dirs]
        return dirs

    # def put(self, lpath, rpath):
    #     self.ftp.put(lpath, rpath)

    # def get(self, rpath, lpath):
    #     self.ftp.get(rpath, lpath)

    def _open(
        self,
        path,
        mode="rb",
        block_size=-1,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        """
        block_size: int or None
            If 0, no buffering, if 1, line buffering, if >1, buffer that many
            bytes, if None use default from paramiko.
        """
        buffering = block_size if block_size is not None and block_size >= 0 else -1
        wpath = self._as_unc_path(path)
        if autocommit is False:
            # writes to temporary file, move on commit
            # TODO: use transaction support in SMB protocol
            share = path.split("/")[1]
            path2 = "/{}{}/{}".format(share, self.temppath, uuid.uuid4())
            wpath2 = self._as_unc_path(path2)
            f = smbclient.open_file(wpath2, mode, buffering=buffering, **kwargs)
            f.temppath = path2
            f.targetpath = path
            f.fs = self
            f.commit = types.MethodType(commit_a_file, f)
            f.discard = types.MethodType(discard_a_file, f)
        else:
            f = smbclient.open_file(wpath, mode, buffering=buffering, **kwargs)
        return f

    def copy(self, path1, path2, **kwargs):
        """ Copy within two locations in the same filesystem"""
        wpath1 = self._as_unc_path(path1)
        wpath2 = self._as_unc_path(path2)
        smbclient.copyfile(wpath1, wpath2, **kwargs)

    def _rm(self, path):
        if self._share_has_path(path):
            wpath = self._as_unc_path(path)
            s = smbclient.stat(wpath)
            if S_ISDIR(s.st_mode):
                smbclient.rmdir(wpath)
            else:
                smbclient.remove(wpath)

    def mv(self, old, new):
        wold = self._as_unc_path(old)
        wnew = self._as_unc_path(new)
        smbclient.rename(wold, wnew)


def commit_a_file(self):
    self.fs.mv(self.temppath, self.targetpath)


def discard_a_file(self):
    self.fs._rm(self.temppath)
