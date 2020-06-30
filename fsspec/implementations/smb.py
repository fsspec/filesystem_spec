# -*- coding: utf-8 -*-
"""
This module contains SMBFileSystem class responsible for handling access to
Windows Samba network shares by using package smbprotocol
"""

from stat import S_ISDIR, S_ISLNK
import datetime
import types
import uuid

import smbclient

from .. import AbstractFileSystem
from ..utils import infer_storage_options


class SMBFileSystem(AbstractFileSystem):
    """Allow reading and writing to Windows and Samba network shares.

    When using `fsspec.open()` for getting a file-like object the URI
    should be specified as this format:
    `smb://workgroup;user:password@server:port/share/folder/file.csv`.

    Example::
        >>> import fsspec
        >>> with fsspec.open('smb://myuser:mypassword@myserver.com/'
        ...                  'share/folder/file.csv') as smbfile:
        ...     df = pd.read_csv(smbfile, sep='|', header=None)

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

    # pylint: disable=too-many-arguments
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

    @classmethod
    def _strip_protocol(cls, path):
        return infer_storage_options(path)["path"]

    @staticmethod
    def _get_kwargs_from_urls(path):
        # smb://workgroup;user:password@host:port/share/folder/file.csv
        out = infer_storage_options(path)
        out.pop("path", None)
        out.pop("protocol", None)
        return out

    def mkdir(self, path, create_parents=True, **kwargs):
        wpath = _as_unc_path(self.host, path)
        if create_parents:
            smbclient.makedirs(wpath, exist_ok=False, **kwargs)
        else:
            smbclient.mkdir(wpath, **kwargs)

    def makedirs(self, path, exist_ok=False):
        if _share_has_path(path):
            wpath = _as_unc_path(self.host, path)
            smbclient.makedirs(wpath, exist_ok=exist_ok)

    def rmdir(self, path):
        if _share_has_path(path):
            wpath = _as_unc_path(self.host, path)
            smbclient.rmdir(wpath)

    def info(self, path, **kwargs):
        wpath = _as_unc_path(self.host, path)
        stats = smbclient.stat(wpath, **kwargs)
        if S_ISDIR(stats.st_mode):
            stype = "directory"
        elif S_ISLNK(stats.st_mode):
            stype = "link"
        else:
            stype = "file"
        res = {
            "name": path + "/" if stype == "directory" else path,
            "size": stats.st_size,
            "type": stype,
            "uid": stats.st_uid,
            "gid": stats.st_gid,
            "time": stats.st_atime,
            "mtime": stats.st_mtime,
        }
        return res

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        wpath = _as_unc_path(self.host, path)
        stats = smbclient.stat(wpath)
        return datetime.datetime.utcfromtimestamp(stats.st_ctime)

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        wpath = _as_unc_path(self.host, path)
        stats = smbclient.stat(wpath)
        return datetime.datetime.utcfromtimestamp(stats.st_mtime)

    def ls(self, path, detail=True, **kwargs):
        unc = _as_unc_path(self.host, path)
        listed = smbclient.listdir(unc, **kwargs)
        dirs = ["/".join([path.rstrip("/"), p]) for p in listed]
        if detail:
            dirs = [self.info(d) for d in dirs]
        return dirs

    # pylint: disable=too-many-arguments
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
        wpath = _as_unc_path(self.host, path)
        if autocommit is False:
            # writes to temporary file, move on commit
            # TODO: use transaction support in SMB protocol
            share = path.split("/")[1]
            path2 = "/{}{}/{}".format(share, self.temppath, uuid.uuid4())
            wpath2 = _as_unc_path(self.host, path2)
            smbf = smbclient.open_file(wpath2, mode, buffering=buffering, **kwargs)
            smbf.temppath = path2
            smbf.targetpath = path
            smbf.fs = self
            smbf.commit = types.MethodType(_commit_a_file, smbf)
            smbf.discard = types.MethodType(_discard_a_file, smbf)
        else:
            smbf = smbclient.open_file(wpath, mode, buffering=buffering, **kwargs)
        return smbf

    def copy(self, path1, path2, **kwargs):
        """ Copy within two locations in the same filesystem"""
        wpath1 = _as_unc_path(self.host, path1)
        wpath2 = _as_unc_path(self.host, path2)
        smbclient.copyfile(wpath1, wpath2, **kwargs)

    def _rm(self, path):
        if _share_has_path(path):
            wpath = _as_unc_path(self.host, path)
            stats = smbclient.stat(wpath)
            if S_ISDIR(stats.st_mode):
                smbclient.rmdir(wpath)
            else:
                smbclient.remove(wpath)

    def mv(self, path1, path2, **kwargs):
        wpath1 = _as_unc_path(self.host, path1)
        wpath2 = _as_unc_path(self.host, path2)
        smbclient.rename(wpath1, wpath2, **kwargs)


def _commit_a_file(self):
    self.fs.mv(self.temppath, self.targetpath)


def _discard_a_file(self):
    self.fs.rm(self.temppath)


def _as_unc_path(host, path):
    rpath = path.replace("/", "\\")
    unc = "\\\\{}{}".format(host, rpath)
    return unc


def _share_has_path(path):
    parts = path.count("/")
    if path.endswith("/"):
        return parts > 2
    return parts > 1
