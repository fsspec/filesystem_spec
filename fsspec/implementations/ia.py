"""Files in Internet Archive items, ``ia://<identifier>/<filename>``."""

import configparser
import os
from dataclasses import dataclass

import aiohttp
from aiohttp import hdrs

from ..utils import stringify_path
from .http import HTTPFileSystem

DOWNLOAD_URL = "https://archive.org/download/"
AUTH_DOMAIN = ".archive.org"
# The environment variable names the ``internetarchive`` package uses.
ENV_ACCESS_KEY = "IA_ACCESS_KEY_ID"
ENV_SECRET_KEY = "IA_SECRET_ACCESS_KEY"
ENV_CONFIG_FILE = "IA_CONFIG_FILE"


def ia_config_path():
    """The ``ia.ini`` written by ``ia configure``, or None.

    Searched the way the ``internetarchive`` package searches: ``$IA_CONFIG_FILE``,
    ``$XDG_CONFIG_HOME/internetarchive/ia.ini`` (``XDG_CONFIG_HOME`` defaulting to
    ``~/.config``), ``~/.config/ia.ini``, ``~/.ia``. The first existing file wins.
    """
    home = os.path.expanduser("~")
    xdg = os.environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config")
    candidates = [
        os.environ.get(ENV_CONFIG_FILE),
        os.path.join(xdg, "internetarchive", "ia.ini"),
        os.path.join(home, ".config", "ia.ini"),
        os.path.join(home, ".ia"),
    ]
    return next((p for p in candidates if p and os.path.isfile(p)), None)


@dataclass(frozen=True)
class IACredentials:
    """The IA-S3 key pair that logs a request in at archive.org; None means anonymous."""

    access_key: "str | None" = None
    secret_key: "str | None" = None
    config_file: "str | None" = None  # where they were read from

    @property
    def anonymous(self):
        return not self.access_key

    @property
    def authorization(self):
        """The ``Authorization`` header value, or None when anonymous."""
        if self.anonymous:
            return None
        return f"LOW {self.access_key}:{self.secret_key}"


def load_ia_credentials(config_file=None):
    """Credentials from ``ia.ini`` and the environment; anonymous when there are none.

    ``[s3] access/secret`` are read from ``config_file`` (default:
    :func:`ia_config_path`); the ``[cookies]`` section ``ia configure`` also writes is
    ignored, as the keys log a request in on their own. ``IA_ACCESS_KEY_ID`` /
    ``IA_SECRET_ACCESS_KEY`` override the file's keys and must be set together. A
    missing file or section is not an error: public items need nothing.
    """
    path = config_file or ia_config_path()
    access_key = secret_key = None
    if path and os.path.isfile(path):
        # RawConfigParser: the file's cookie values contain '%' (URL-encoded
        # emails), which the default interpolation would reject.
        parser = configparser.RawConfigParser()
        parser.read(path, encoding="utf-8")
        access_key = parser.get("s3", "access", fallback="").strip() or None
        secret_key = parser.get("s3", "secret", fallback="").strip() or None

    env_access = os.environ.get(ENV_ACCESS_KEY)
    env_secret = os.environ.get(ENV_SECRET_KEY)
    if bool(env_access) != bool(env_secret):
        raise ValueError(f"{ENV_ACCESS_KEY} and {ENV_SECRET_KEY} must be set together")
    if env_access:
        access_key, secret_key = env_access, env_secret
    if not (access_key and secret_key):
        access_key = secret_key = None
    found = path if path and os.path.isfile(path) else None
    return IACredentials(access_key, secret_key, found)


def in_domain(host, domain):
    """Whether ``host`` is ``domain`` (leading dot optional) or a subdomain of it."""
    if not host:
        return False
    domain = domain.lower().lstrip(".")
    host = host.lower().rstrip(".")
    return host == domain or host.endswith("." + domain)


def authorized_request_class(authorization, domain, base=aiohttp.ClientRequest):
    """A ``ClientRequest`` that sends ``authorization`` to every host in ``domain``.

    aiohttp drops ``Authorization`` when a redirect changes origin, and every
    archive.org download is such a redirect (to a data node). It builds a new
    request from the session's ``request_class`` for each hop, so setting the header
    here puts it back on the data-node hop; testing the host keeps it from leaving
    ``domain`` should a data node redirect elsewhere.
    """

    class AuthorizedRequest(base):
        def update_headers(self, headers):
            super().update_headers(headers)
            if in_domain(self.url.host, domain):
                self.headers[hdrs.AUTHORIZATION] = authorization

    return AuthorizedRequest


class InternetArchiveFileSystem(HTTPFileSystem):
    """Files in Internet Archive items, addressed as ``ia://<identifier>/<filename>``.

    Every path is read from ``https://archive.org/download/<identifier>/<filename>``,
    which redirects to a data node that honours HTTP Range requests, so this is
    ``HTTPFileSystem`` with a path mapping and archive.org credentials. (IA's
    S3-like API at ``s3.us.archive.org`` is not used: it ignores Range headers and
    redirects GET to plain-http data nodes, so block reads would fetch whole files.)

    Public items need no credentials. Restricted items need the account's IA-S3
    keys, as written by ``ia configure`` from the ``internetarchive`` package to
    ``ia.ini``; that file is found the way the package finds it (``$IA_CONFIG_FILE``,
    ``$XDG_CONFIG_HOME/internetarchive/ia.ini``, ``~/.config/ia.ini``, ``~/.ia``).
    Explicit arguments win over the file.

    Parameters
    ----------
    access_key, secret_key: str, optional
        IA S3 keys, sent as ``Authorization: LOW <access>:<secret>`` to every
        archive.org host, including the data node the download URL redirects to
        (aiohttp drops the header on a cross-origin redirect; the session's request
        class puts it back, and only for ``.archive.org`` hosts). Both or neither;
        ``IA_ACCESS_KEY_ID`` / ``IA_SECRET_ACCESS_KEY`` in the environment override
        ``ia.ini``. Pass empty strings to stay anonymous despite an ``ia.ini``.
    config_file: str, optional
        Path to an ``ia.ini`` to read credentials from instead of the default lookup.
    kwargs:
        Passed to ``HTTPFileSystem``.
    """

    protocol = "ia"
    download_url = DOWNLOAD_URL
    auth_domain = AUTH_DOMAIN

    def __init__(self, access_key=None, secret_key=None, config_file=None, **kwargs):
        if access_key is None and secret_key is None:
            found = load_ia_credentials(config_file)
            access_key, secret_key = found.access_key, found.secret_key
        elif bool(access_key) != bool(secret_key):
            raise ValueError("access_key and secret_key must be given together")
        credentials = IACredentials(access_key or None, secret_key or None)
        self.access_key = credentials.access_key
        self.authorization = credentials.authorization
        if self.authorization:
            client_kwargs = dict(kwargs.pop("client_kwargs", None) or {})
            client_kwargs["request_class"] = authorized_request_class(
                self.authorization,
                self.auth_domain,
                client_kwargs.get("request_class", aiohttp.ClientRequest),
            )
            kwargs["client_kwargs"] = client_kwargs
        super().__init__(**kwargs)

    @property
    def fsid(self):
        return "ia"

    @classmethod
    def _strip_protocol(cls, path):
        """``ia://item/file`` (or bare ``item/file``) becomes the download URL.

        A URL passes through unchanged, so the mapping is idempotent.
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        if path.startswith("ia://"):
            path = path[5:]
        elif "://" in path:
            return path
        return cls.download_url + path.lstrip("/")

    def unstrip_protocol(self, name):
        if name.startswith(self.download_url):
            return "ia://" + name[len(self.download_url) :]
        if name.startswith("ia://"):
            return name
        return "ia://" + name.lstrip("/")

    # archive.org answers a restricted item with 403 (401 for a bad LOW key).
    # HTTPFileSystem reports a failed HEAD+GET as FileNotFoundError and any other
    # status through raise_for_status(); both become PermissionError so callers can
    # tell "no such file" from "log in". The two methods callers reach with an
    # ia:// path directly (open() strips before _open) map it here too.
    async def _info(self, url, **kwargs):
        url = self._strip_protocol(url)
        try:
            return await super()._info(url, **kwargs)
        except FileNotFoundError as exc:
            cause = exc.__cause__
            if isinstance(cause, aiohttp.ClientResponseError) and cause.status in (
                401,
                403,
            ):
                raise PermissionError(url) from cause
            raise

    async def _cat_file(self, url, start=None, end=None, **kwargs):
        url = self._strip_protocol(url)
        return await super()._cat_file(url, start=start, end=end, **kwargs)

    def _raise_not_found_for_status(self, response, url):
        if response.status in (401, 403):
            raise PermissionError(url)
        super()._raise_not_found_for_status(response, url)
