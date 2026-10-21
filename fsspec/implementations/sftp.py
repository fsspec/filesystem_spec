import contextlib
import datetime
import locale
import logging
import os
import types
import uuid
from stat import S_ISDIR, S_ISLNK

import paramiko
from paramiko.hostkeys import HostKeyEntry

try:
    import msvcrt
except ImportError:  # not Windows
    msvcrt = None

from .. import AbstractFileSystem
from ..utils import infer_storage_options

logger = logging.getLogger("fsspec.sftp")


@contextlib.contextmanager
def _locked(f):
    """Keep other processes from adding a line to the open file meanwhile.

    On POSIX a write to a file in append mode is atomic. Windows seeks to the
    end of the file first, so two processes can write to the same place. The
    lock is on a byte far beyond the end, which does not keep anyone from
    reading the file.
    """
    if msvcrt is None:
        yield
        return
    f.seek(1 << 30)
    msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
    try:
        yield
    finally:
        f.seek(1 << 30)
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)


def _append_known_host(path, hostname, key):
    """Add one line to the ``known_hosts`` file, as OpenSSH does.

    The line is written in one call to the file in append mode, so that
    processes adding keys at the same time do not overwrite each other.
    """
    # paramiko reads the file with the default encoding of open()
    encoding = locale.getpreferredencoding(False)
    line = HostKeyEntry([hostname], key).to_line().encode(encoding)
    with open(path, "a+b") as f, _locked(f):
        size = f.seek(0, os.SEEK_END)
        if size:
            f.seek(size - 1)
            if f.read(1) != b"\n":
                line = b"\n" + line
        f.write(line)
        # On Windows, write the line out while the lock is still held.
        # Otherwise it would sit in the write buffer until the file is closed,
        # which is after the unlock.
        f.flush()


class _AutoAddPolicy(paramiko.AutoAddPolicy):
    """Accept an unknown host key, remember it in ``known_hosts`` and say so.

    Without a ``known_hosts`` file nothing can be remembered. The key is
    accepted anyway, and the warning says that it is not remembered.
    """

    def __init__(self, known_hosts=None):
        self.known_hosts = known_hosts

    @property
    def persists(self):
        return self.known_hosts is not None

    def missing_host_key(self, client, hostname, key):
        what = f"{key.get_name()} {key.fingerprint}"
        if self.known_hosts is None:
            logger.warning(
                "Unknown host key of %r accepted without verification and NOT "
                "remembered: %s. Anyone between you and the server can "
                "impersonate it. Check the fingerprint. To remember keys pass "
                "known_hosts=<file>; the safest setup is a known_hosts file "
                "filled beforehand, with host_key_policy='reject'.",
                hostname,
                what,
            )
        else:
            try:
                _append_known_host(self.known_hosts, hostname, key)
            except (OSError, ValueError) as exc:
                raise paramiko.SSHException(
                    f"Unknown host key of {hostname!r} ({what}) refused: it "
                    f"cannot be added to {self.known_hosts!r} ({exc}), so "
                    "nothing guarantees that the fingerprint stays known."
                ) from exc
            logger.warning(
                "Unknown host key of %r accepted without verification and "
                "added to %s: %s. Check the fingerprint now: the next "
                "connection trusts this key.",
                hostname,
                self.known_hosts,
                what,
            )
        super().missing_host_key(client, hostname, key)


#: Short names for the missing host key policies built into paramiko, accepted
#: as the ``host_key_policy`` argument of ``SFTPFileSystem``.
HOST_KEY_POLICIES = {
    "auto_add": _AutoAddPolicy,  # created by _resolve_host_key_policy
    "reject": paramiko.RejectPolicy,
    "warning": paramiko.WarningPolicy,
}


def _resolve_host_key_policy(policy, known_hosts=None):
    """Turn a ``host_key_policy`` argument into a paramiko policy instance."""
    if policy is None or policy == "auto_add":
        return _AutoAddPolicy(known_hosts)
    if isinstance(policy, str):
        try:
            return HOST_KEY_POLICIES[policy]()
        except KeyError:
            raise ValueError(
                f"Unknown host_key_policy {policy!r}; expected one of "
                f"{sorted(HOST_KEY_POLICIES)} or a paramiko.MissingHostKeyPolicy"
            ) from None
    if isinstance(policy, type) and issubclass(policy, paramiko.MissingHostKeyPolicy):
        return policy()
    if isinstance(policy, paramiko.MissingHostKeyPolicy):
        return policy
    raise TypeError(
        "host_key_policy must be one of "
        f"{sorted(HOST_KEY_POLICIES)}, a paramiko.MissingHostKeyPolicy subclass "
        f"or instance, not {type(policy).__name__}"
    )


class SFTPFileSystem(AbstractFileSystem):
    """Files over SFTP/SSH

    Peer-to-peer filesystem over SSH using paramiko.

    Note: if using this with the ``open`` or ``open_files``, with full URLs,
    there is no way to tell if a path is relative, so all paths are assumed
    to be absolute.
    """

    protocol = "sftp", "ssh"

    def __init__(self, host, *, known_hosts=None, host_key_policy=None, **ssh_kwargs):
        """

        Parameters
        ----------
        host: str
            Hostname or IP as a string
        temppath: str
            Location on the server to put files, when within a transaction
        host_key_policy: str or paramiko.MissingHostKeyPolicy, optional
            What to do with a host that is not in the known hosts file, which is
            ``~/.ssh/known_hosts`` or ``known_hosts``. Passed to
            ``paramiko.SSHClient.set_missing_host_key_policy``. One of the
            strings

            * ``"reject"``: refuse to connect (``paramiko.RejectPolicy``).
              Nothing is ever added to a file.
            * ``"auto_add"`` (default): accept the key. With ``known_hosts``, the
              key is added to that file, so that the next connection checks it.
              A warning with the fingerprint is always logged. Without
              ``known_hosts`` the key is accepted and not remembered, and the
              warning says so.
            * ``"warning"``: log a warning and accept
              (``paramiko.WarningPolicy``). Nothing is remembered.

            or a ``paramiko.MissingHostKeyPolicy`` subclass or instance.

            With ``"auto_add"`` and ``"warning"``, a
            man-in-the-middle attacker can impersonate the server on the first
            connection to a host.

            A host that is listed in a known hosts file is always checked,
            whatever the policy: a different key raises
            ``paramiko.BadHostKeyException`` before any password is sent.

            The safest setup is a ``known_hosts`` file that is filled
            beforehand, from a source that can be trusted, together with
            ``host_key_policy="reject"``. Then every key is checked, an
            unknown host or a different key is refused before authentication,
            and the file is never written.
        known_hosts: str, optional
            Path of a ``known_hosts`` file that is used instead of
            ``~/.ssh/known_hosts``, like ``UserKnownHostsFile`` of OpenSSH.
            It is only written by the ``"auto_add"`` policy, with one line
            added at the end. It has to exist, unless the policy adds keys,
            which creates it.
            paramiko ignores patterns such as ``*.example.com``.
        ssh_kwargs: dict
            Parameters passed on to connection. See details in
            https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect
            May include port, username, password...
        """
        if self._cached:
            return
        super().__init__(**ssh_kwargs)
        self.temppath = ssh_kwargs.pop("temppath", "/tmp")  # remote temp directory
        self.known_hosts = (
            None if known_hosts is None else os.path.expanduser(known_hosts)
        )
        self.host_key_policy = _resolve_host_key_policy(
            host_key_policy, self.known_hosts
        )
        self.host = host
        self.ssh_kwargs = ssh_kwargs
        self._connect()

    def _connect(self):
        logger.debug("Connecting to SFTP server %s", self.host)
        self.client = paramiko.SSHClient()
        # Without the known hosts, every host is unknown to paramiko: the
        # policy accepts whatever key it is shown and a changed key goes unnoticed.
        self._load_known_hosts()
        self.client.set_missing_host_key_policy(self.host_key_policy)
        try:
            self.client.connect(self.host, **self.ssh_kwargs)
        except paramiko.BadHostKeyException as exc:
            logger.error(
                "The host key of %r has CHANGED (%s). This can be a "
                "man-in-the-middle attack. The connection was refused before "
                "authentication.",
                self.host,
                exc,
            )
            raise
        self.ftp = self.client.open_sftp()

    def _load_known_hosts(self):
        # Like UserKnownHostsFile of OpenSSH: a given file replaces the default.
        # paramiko calls the file of the user "system host keys". Not
        # load_host_keys(): paramiko would then rewrite the whole file whenever
        # the policy accepts a key.
        path = self.known_hosts or os.path.expanduser("~/.ssh/known_hosts")
        try:
            # with a file name, paramiko raises when it cannot read the file
            self.client.load_system_host_keys(path)
        except (FileNotFoundError, NotADirectoryError):
            # No default file is fine, for example with HOME=/dev/null. A given
            # file that does not exist yet is created when the policy adds the
            # first key. Otherwise it is an error.
            if self.known_hosts is not None and not getattr(
                self.host_key_policy, "persists", False
            ):
                raise

    @classmethod
    def _strip_protocol(cls, path):
        return infer_storage_options(path)["path"]

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        out = infer_storage_options(urlpath)
        out.pop("path", None)
        out.pop("protocol", None)
        return out

    def mkdir(self, path, create_parents=True, mode=511):
        path = self._strip_protocol(path)
        logger.debug("Creating folder %s", path)
        if self.exists(path):
            raise FileExistsError(f"File exists: {path}")

        if create_parents:
            self.makedirs(path)
        else:
            self.ftp.mkdir(path, mode)

    def makedirs(self, path, exist_ok=False, mode=511):
        path = self._strip_protocol(path)
        if self.exists(path) and not exist_ok:
            raise FileExistsError(f"File exists: {path}")

        parts = path.split("/")
        new_path = "/" if path[:1] == "/" else ""

        for part in parts:
            if part:
                new_path = f"{new_path}/{part}" if new_path else part
                if not self.exists(new_path):
                    self.ftp.mkdir(new_path, mode)

    def rmdir(self, path):
        path = self._strip_protocol(path)
        logger.debug("Removing folder %s", path)
        self.ftp.rmdir(path)

    def info(self, path, follow_symlinks=True):
        """Return metadata for a remote path.

        Parameters
        ----------
        path: str
            Path or SFTP/SSH URL to inspect.
        follow_symlinks: bool, default True
            Follow symbolic links and return metadata for their targets.
            If False, return metadata for the link itself, as for symlink
            entries in ``ls(path, detail=True)``. This also works for dangling
            links, whose targets do not exist.
        """
        path = self._strip_protocol(path)
        if follow_symlinks:
            stat = self.ftp.stat(path)
        else:
            stat = self.ftp.lstat(path)
        stat = self._decode_stat(stat)
        stat["name"] = path
        return stat

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        # The API does not provide creation time, so we use modification time
        info = self.info(path)
        mtime = info.get("mtime", None)
        if mtime is not None:
            return mtime
        raise RuntimeError("Could not retrieve creation time (modification time).")

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        info = self.info(path)
        mtime = info.get("mtime", None)
        if mtime is not None:
            return mtime
        raise RuntimeError("Could not retrieve modification time.")

    @staticmethod
    def _decode_stat(stat, parent_path=None):
        if S_ISDIR(stat.st_mode):
            t = "directory"
        elif S_ISLNK(stat.st_mode):
            t = "link"
        else:
            t = "file"
        out = {
            "name": "",
            "size": stat.st_size,
            "type": t,
            "uid": stat.st_uid,
            "gid": stat.st_gid,
            "time": datetime.datetime.fromtimestamp(
                stat.st_atime, tz=datetime.timezone.utc
            ),
            "mtime": datetime.datetime.fromtimestamp(
                stat.st_mtime, tz=datetime.timezone.utc
            ),
        }
        if parent_path:
            out["name"] = "/".join([parent_path.rstrip("/"), stat.filename])
        return out

    def ls(self, path, detail=False):
        path = self._strip_protocol(path)
        logger.debug("Listing folder %s", path)
        stats = [self._decode_stat(stat, path) for stat in self.ftp.listdir_iter(path)]
        if detail:
            return stats
        else:
            paths = [stat["name"] for stat in stats]
            return sorted(paths)

    def put_file(self, lpath, rpath, callback=None, **kwargs):
        self.mkdirs(self._parent(os.fspath(rpath)), exist_ok=True)
        logger.debug("Put file %s into %s", lpath, rpath)
        self.ftp.put(lpath, rpath)

    def get_file(self, rpath, lpath, **kwargs):
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
        else:
            self.ftp.get(self._strip_protocol(rpath), lpath)

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        """
        block_size: int or None
            If 0, no buffering, if 1, line buffering, if >1, buffer that many
            bytes, if None use default from paramiko.
        """
        logger.debug("Opening file %s", path)
        if kwargs.get("autocommit", True) is False:
            # writes to temporary file, move on commit
            path2 = "/".join([self.temppath, str(uuid.uuid4())])
            f = self.ftp.open(path2, mode, bufsize=block_size if block_size else -1)
            f.temppath = path2
            f.targetpath = path
            f.fs = self
            f.commit = types.MethodType(commit_a_file, f)
            f.discard = types.MethodType(discard_a_file, f)
        else:
            f = self.ftp.open(path, mode, bufsize=block_size if block_size else -1)
        return f

    def _rm(self, path):
        if self.isdir(path):
            self.ftp.rmdir(path)
        else:
            self.ftp.remove(path)

    def mv(self, old, new):
        new = self._strip_protocol(new)
        old = self._strip_protocol(old)
        logger.debug("Renaming %s into %s", old, new)
        self.ftp.posix_rename(old, new)


def commit_a_file(self):
    self.fs.mv(self.temppath, self.targetpath)


def discard_a_file(self):
    self.fs._rm(self.temppath)
