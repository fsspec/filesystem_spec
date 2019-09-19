# https://hadoop.apache.org/docs/r1.0.4/webhdfs.html

import requests
from urllib.parse import quote
import uuid
from ..spec import AbstractFileSystem, AbstractBufferedFile
from ..utils import infer_storage_options
import logging

logger = logging.getLogger("webhdfs")


class WebHDFS(AbstractFileSystem):
    """
    Interface to HDFS over HTTP

    Three auth mechanisms are supported:

    insecure: no auth is done, and the user is assumed to be whoever they
        say they are (parameter `user`), or a predefined value such as
        "dr.who" if not given
    spnego: when kerberos authentication is enabled, auth is negotiated by
        requests_kerberos https://github.com/requests/requests-kerberos .
        This establishes a session based on existing kinit login and/or
        specified principal/password; paraneters are passed with ``kerb_kwargs``
    token: uses an existing Hadoop delegation token from another secured
        service. Indeed, this client can also generate such tokens when
        not insecure. Note that tokens expire, but can be renewed (by a
        previously specified user) and may allow for proxying.

    """

    tempdir = "/tmp"
    protocol = "webhdfs", "webHDFS"

    def __init__(
        self,
        host,
        port=50070,
        kerberos=False,
        token=None,
        user=None,
        proxy_to=None,
        kerb_kwargs=None,
        data_proxy=None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        host: str
            Name-node address
        port: int
            Port for webHDFS
        kerberos: bool
            Whether to authenticate with kerberos for this connection
        token: str or None
            If given, use this token on every call to authenticate. A user
            and user-proxy may be encoded in the token and should not be also
            given
        user: str or None
            If given, assert the user name to connect with
        proxy_to: str or None
            If given, the user has the authority to proxy, and this value is
            the user in who's name actions are taken
        kerb_kwargs: dict
            Any extra arguments for HTTPKerberosAuth, see
            https://github.com/requests/requests-kerberos/blob/master/requests_kerberos/kerberos_.py
        data_proxy: dict, callable or None
            If given, map data-node addresses. This can be necessary if the
            HDFS cluster is behind a proxy, running on Docker or otherwise has
            a mismatch between the host-names given by the name-node and the
            address by which to refer to them from the client. If a dict,
            maps host names `host->data_proxy[host]`; if a callable, full
            URLs are passed, and function must conform to
            `url->data_proxy(url)`.
        kwargs
        """
        super().__init__(**kwargs)
        self.url = f"http://{host}:{port}/webhdfs/v1"
        self.kerb = kerberos
        self.kerb_kwargs = kerb_kwargs or {}
        self.pars = {}
        self.proxy = data_proxy or {}
        if token is not None:
            if user is not None or proxy_to is not None:
                raise ValueError(
                    "If passing a delegation token, must not set "
                    "user or proxy_to, as these are encoded in the"
                    " token"
                )
            self.pars["delegation"] = token
        if user is not None:
            self.pars["user.name"] = user
        if proxy_to is not None:
            self.pars["doas"] = proxy_to
        if kerberos and user is not None:
            raise ValueError(
                "If using Kerberos auth, do not specify the "
                "user, this is handled by kinit."
            )
        self._connect()

    def _connect(self):
        self.session = requests.Session()
        if self.kerb:
            from requests_kerberos import HTTPKerberosAuth

            self.session.auth = HTTPKerberosAuth(**self.kerb_kwargs)

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop("session", None)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._connect()

    def _call(self, op, method="get", path=None, data=None, redirect=True, **kwargs):
        url = self.url + quote(path or "")
        args = kwargs.copy()
        args.update(self.pars)
        args["op"] = op.upper()
        logger.debug(url, method, args)
        out = self.session.request(
            method=method.upper(),
            url=url,
            params=args,
            data=data,
            allow_redirects=redirect,
        )
        if out.status_code == 404:
            raise FileNotFoundError(path)
        if out.status_code == 403:
            raise PermissionError(path or "")
        if out.status_code == 401:
            raise PermissionError  # not specific to path
        out.raise_for_status()
        return out

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        replication=None,
        permissions=None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        path: str
            File location
        mode: str
            'rb', 'wb', etc.
        block_size: int
            Client buffer size for read-ahead or write buffer
        autocommit: bool
            If False, writes to temporary file that only gets put in final
            location upon commit
        replication: int
            Number of copies of file on the cluster, write mode only
        permissions: str or int
            posix permissions, write mode only
        kwargs

        Returns
        -------
        WebHDFile instance
        """
        block_size = block_size or self.blocksize
        return WebHDFile(
            self,
            path,
            mode=mode,
            block_size=block_size,
            tempdir=self.tempdir,
            autocommit=autocommit,
            replication=replication,
            permissions=permissions,
        )

    @staticmethod
    def _process_info(info):
        info["type"] = info["type"].lower()
        info["size"] = info["length"]
        return info

    @classmethod
    def _strip_protocol(cls, path):
        return infer_storage_options(path)["path"]

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        out = infer_storage_options(urlpath)
        out.pop("path", None)
        out.pop("protocol", None)
        if "username" in out:
            out["user"] = out.pop("username")
        return out

    def info(self, path):
        out = self._call("GETFILESTATUS", path=path)
        info = out.json()["FileStatus"]
        info["name"] = path
        return self._process_info(info)

    def ls(self, path, detail=False):
        out = self._call("LISTSTATUS", path=path)
        infos = out.json()["FileStatuses"]["FileStatus"]
        for info in infos:
            self._process_info(info)
            info["name"] = path.rstrip("/") + "/" + info["pathSuffix"]
        if detail:
            return sorted(infos, key=lambda i: i["name"])
        else:
            return sorted(info["name"] for info in infos)

    def content_summary(self, path):
        """Total numbers of files, directories and bytes under path"""
        out = self._call("GETCONTENTSUMMARY", path=path)
        return out.json()["ContentSummary"]

    def ukey(self, path):
        """Checksum info of file, giving method and result"""
        out = self._call("GETFILECHECKSUM", path=path, redirect=False)
        location = self._apply_proxy(out.headers["Location"])
        out2 = self.session.get(location)
        out2.raise_for_status()
        return out2.json()["FileChecksum"]

    def home_directory(self):
        """Get user's home directory"""
        out = self._call("GETHOMEDIRECTORY")
        return out.json()["Path"]

    def get_delegation_token(self, renewer=None):
        """Retrieve token which can give the same authority to other uses

        Parameters
        ----------
        renewer: str or None
            User who may use this token; if None, will be current user
        """
        if renewer:
            out = self._call("GETDELEGATIONTOKEN", renewer=renewer)
        else:
            out = self._call("GETDELEGATIONTOKEN")
        t = out.json()["Token"]
        if t is None:
            raise ValueError("No token available for this " "user/security context")
        return t["urlString"]

    def renew_delegation_token(self, token):
        """Make token live longer. Returns new expiry time"""
        out = self._call("RENEWDELEGATIONTOKEN", method="put", token=token)
        return out.json()["long"]

    def cancel_delegation_token(self, token):
        """Stop the token from being useful"""
        self._call("CANCELDELEGATIONTOKEN", method="put", token=token)

    def chmod(self, path, mod):
        """Set the permission at path

        Parameters
        ----------
        path: str
            location to set (file or directory)
        mod: str or int
            posix epresentation or permission, give as oct string, e.g, '777'
            or 0o777
        """
        self._call("SETPERMISSION", method="put", path=path, permission=mod)

    def chown(self, path, owner=None, group=None):
        """Change owning user and/or group"""
        kwargs = {}
        if owner is not None:
            kwargs["owner"] = owner
        if group is not None:
            kwargs["group"] = group
        self._call("SETOWNER", method="put", path=path, **kwargs)

    def set_replication(self, path, replication):
        """
        Set file replication factor

        Parameters
        ----------
        path: str
            File location (not for directories)
        replication: int
            Number of copies of file on the cluster. Should be smaller than
            number of data nodes; normally 3 on most systems.
        """
        self._call("SETREPLICATION", path=path, method="put", replication=replication)

    def mkdir(self, path, **kwargs):
        self._call("MKDIRS", method="put", path=path)

    def makedirs(self, path, exist_ok=False):
        if exist_ok is False and self.exists(path):
            raise FileExistsError(path)
        self.mkdir(path)

    def mv(self, path1, path2, **kwargs):
        self._call("RENAME", method="put", path=path1, destination=path2)

    def rm(self, path, recursive=False, **kwargs):
        self._call(
            "DELETE",
            method="delete",
            path=path,
            recursive="true" if recursive else "false",
        )

    def _apply_proxy(self, location):
        if self.proxy and callable(self.proxy):
            location = self.proxy(location)
        elif self.proxy:
            # as a dict
            for k, v in self.proxy.items():
                location = location.replace(k, v, 1)
        return location


class WebHDFile(AbstractBufferedFile):
    """A file living in HDFS over webHDFS"""

    def __init__(self, fs, path, **kwargs):
        super().__init__(fs, path, **kwargs)
        kwargs = kwargs.copy()
        if kwargs.get("permissions", None) is None:
            kwargs.pop("permissions", None)
        if kwargs.get("replication", None) is None:
            kwargs.pop("replication", None)
        self.permissions = kwargs.pop("permissions", 511)
        tempdir = kwargs.pop("tempdir")
        if kwargs.pop("autocommit", False) is False:
            self.target = self.path
            self.path = "/".join([tempdir, str(uuid.uuid4())])

    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        out = self.fs.session.post(self.location, data=self.buffer.getvalue())
        out.raise_for_status()
        return True

    def _initiate_upload(self):
        """ Create remote file/upload """
        if "a" in self.mode:
            op, method = "APPEND", "POST"
        else:
            op, method = "CREATE", "PUT"
            if self.fs.exists(self.path):
                # no "truncate" or "create empty"
                self.fs.rm(self.path)
        out = self.fs._call(op, method, self.path, redirect=False, **self.kwargs)
        location = self.fs._apply_proxy(out.headers["Location"])
        if "w" in self.mode:
            # create empty file to append to
            out2 = self.fs.session.put(location)
            out2.raise_for_status()
        self.location = location.replace("CREATE", "APPEND")

    def _fetch_range(self, start, end):
        out = self.fs._call(
            "OPEN", path=self.path, offset=start, length=end - start, redirect=False
        )
        out.raise_for_status()
        location = out.headers["Location"]
        out2 = self.fs.session.get(self.fs._apply_proxy(location))
        return out2.content

    def commit(self):
        self.fs.mv(self.path, self.target)

    def discard(self):
        self.fs.rm(self.path)
