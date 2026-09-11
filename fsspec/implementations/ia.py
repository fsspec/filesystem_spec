"""Files in Internet Archive items, ``ia://<identifier>/<filename>``."""

import configparser
import functools
import os
from dataclasses import dataclass, field
from http.cookies import SimpleCookie

import aiohttp
import yarl

from ..utils import stringify_path
from .http import HTTPFileSystem, get_client

DOWNLOAD_URL = "https://archive.org/download/"
COOKIE_DOMAIN = ".archive.org"
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
    """What logs a request in at archive.org; all-empty means anonymous."""

    access_key: "str | None" = None
    secret_key: "str | None" = None
    cookies: dict = field(default_factory=dict)  # logged-in-user / logged-in-sig
    config_file: "str | None" = None  # where they were read from

    @property
    def anonymous(self):
        return not (self.access_key or self.cookies)


def load_ia_credentials(config_file=None):
    """Credentials from ``ia.ini`` and the environment; anonymous when there are none.

    ``[s3] access/secret`` and ``[cookies] logged-in-user/logged-in-sig`` are read
    from ``config_file`` (default: :func:`ia_config_path`). Cookie values in the file
    carry their attributes (``x; expires=...; path=/; domain=.archive.org``), which
    are parsed off. ``IA_ACCESS_KEY_ID`` / ``IA_SECRET_ACCESS_KEY`` override the
    file's keys and must be set together. A missing file or section is not an
    error: public items need nothing.
    """
    path = config_file or ia_config_path()
    access_key = secret_key = None
    cookies = {}
    if path and os.path.isfile(path):
        # RawConfigParser: cookie values contain '%' (URL-encoded emails), which
        # the default interpolation would reject.
        parser = configparser.RawConfigParser()
        parser.read(path, encoding="utf-8")
        access_key = parser.get("s3", "access", fallback="").strip() or None
        secret_key = parser.get("s3", "secret", fallback="").strip() or None
        if parser.has_section("cookies"):
            for name, raw in parser.items("cookies"):
                morsels = SimpleCookie()
                morsels.load(f"{name}={raw}")
                if name in morsels and morsels[name].value:
                    cookies[name] = morsels[name].value

    env_access = os.environ.get(ENV_ACCESS_KEY)
    env_secret = os.environ.get(ENV_SECRET_KEY)
    if bool(env_access) != bool(env_secret):
        raise ValueError(f"{ENV_ACCESS_KEY} and {ENV_SECRET_KEY} must be set together")
    if env_access:
        access_key, secret_key = env_access, env_secret
    if not (access_key and secret_key):
        access_key = secret_key = None
    found = path if path and os.path.isfile(path) else None
    return IACredentials(access_key, secret_key, cookies, found)


class InternetArchiveFileSystem(HTTPFileSystem):
    """Files in Internet Archive items, addressed as ``ia://<identifier>/<filename>``.

    Every path is read from ``https://archive.org/download/<identifier>/<filename>``,
    which redirects to a data node that honours HTTP Range requests, so this is
    ``HTTPFileSystem`` with a path mapping and archive.org credentials. (IA's
    S3-like API at ``s3.us.archive.org`` is not used: it ignores Range headers and
    redirects GET to plain-http data nodes, so block reads would fetch whole files.)

    Public items need no credentials. Restricted items need the account's cookies
    (and, optionally, its S3 keys) as written by ``ia configure`` from the
    ``internetarchive`` package to ``ia.ini``; that file is found the way the
    package finds it (``$IA_CONFIG_FILE``, ``$XDG_CONFIG_HOME/internetarchive/ia.ini``,
    ``~/.config/ia.ini``, ``~/.ia``). Explicit arguments win over the file.

    Parameters
    ----------
    access_key, secret_key: str, optional
        IA S3 keys, sent as ``Authorization: LOW <access>:<secret>`` on the first
        request. ``IA_ACCESS_KEY_ID`` / ``IA_SECRET_ACCESS_KEY`` in the environment
        override ``ia.ini``.
    cookies: dict, optional
        ``{"logged-in-user": ..., "logged-in-sig": ...}``. They go into the session's
        cookie jar for ``.archive.org`` rather than into a ``Cookie`` header: every
        download is a redirect to another origin, and aiohttp drops ``Cookie`` and
        ``Authorization`` headers when a redirect changes origin, while the jar
        re-attaches its cookies to any archive.org host.
    config_file: str, optional
        Path to an ``ia.ini`` to read credentials from instead of the default lookup.
    kwargs:
        Passed to ``HTTPFileSystem``.
    """

    protocol = "ia"
    download_url = DOWNLOAD_URL
    cookie_domain = COOKIE_DOMAIN

    def __init__(
        self,
        access_key=None,
        secret_key=None,
        cookies=None,
        config_file=None,
        **kwargs,
    ):
        if access_key is None and secret_key is None and cookies is None:
            found = load_ia_credentials(config_file)
            access_key, secret_key, cookies = (
                found.access_key,
                found.secret_key,
                found.cookies,
            )
        self.access_key = access_key
        self.cookies = dict(cookies or {})
        headers = dict(kwargs.pop("headers", None) or {})
        if access_key and secret_key:
            headers["Authorization"] = f"LOW {access_key}:{secret_key}"
        if headers:
            kwargs["headers"] = headers
        make_client = kwargs.pop("get_client", get_client)
        super().__init__(
            get_client=functools.partial(
                self._client_with_cookies, make_client, self.cookies, self.cookie_domain
            ),
            **kwargs,
        )

    @property
    def fsid(self):
        return "ia"

    @staticmethod
    async def _client_with_cookies(make_client, cookies, domain, **kwargs):
        session = await make_client(**kwargs)
        if cookies:
            morsels = SimpleCookie()
            for name, value in cookies.items():
                morsels[name] = value
                morsels[name]["domain"] = domain
                morsels[name]["path"] = "/"
            session.cookie_jar.update_cookies(
                morsels, yarl.URL(f"https://{domain.lstrip('.')}/")
            )
        return session

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
