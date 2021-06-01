from __future__ import absolute_import, division, print_function

import asyncio
import logging
import re
import weakref
from copy import copy
from urllib.parse import urlparse

import aiohttp
import requests

from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import DEFAULT_BLOCK_SIZE, tokenize

from ..caching import AllBytes

# https://stackoverflow.com/a/15926317/3821154
ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
ex2 = re.compile(r"""(?P<url>http[s]?://[-a-zA-Z0-9@:%_+.~#?&/=]+)""")
logger = logging.getLogger("fsspec.http")


async def get_client(**kwargs):
    return aiohttp.ClientSession(**kwargs)


class HTTPFileSystem(AsyncFileSystem):
    """
    Simple File-System for fetching data via HTTP(S)

    ``ls()`` is implemented by loading the parent page and doing a regex
    match on the result. If simple_link=True, anything of the form
    "http(s)://server.com/stuff?thing=other"; otherwise only links within
    HTML href tags will be used.
    """

    sep = "/"

    def __init__(
        self,
        simple_links=True,
        block_size=None,
        same_scheme=True,
        size_policy=None,
        cache_type="bytes",
        cache_options=None,
        asynchronous=False,
        loop=None,
        client_kwargs=None,
        **storage_options,
    ):
        """
        NB: if this is called async, you must await set_client

        Parameters
        ----------
        block_size: int
            Blocks to read bytes; if 0, will default to raw requests file-like
            objects instead of HTTPFile instances
        simple_links: bool
            If True, will consider both HTML <a> tags and anything that looks
            like a URL; if False, will consider only the former.
        same_scheme: True
            When doing ls/glob, if this is True, only consider paths that have
            http/https matching the input URLs.
        size_policy: this argument is deprecated
        client_kwargs: dict
            Passed to aiohttp.ClientSession, see
            https://docs.aiohttp.org/en/stable/client_reference.html
            For example, ``{'auth': aiohttp.BasicAuth('user', 'pass')}``
        storage_options: key-value
            Any other parameters passed on to requests
        cache_type, cache_options: defaults used in open
        """
        super().__init__(self, asynchronous=asynchronous, loop=loop, **storage_options)
        self.block_size = block_size if block_size is not None else DEFAULT_BLOCK_SIZE
        self.simple_links = simple_links
        self.same_schema = same_scheme
        self.cache_type = cache_type
        self.cache_options = cache_options
        self.client_kwargs = client_kwargs or {}
        self.kwargs = storage_options
        self._session = None

        # Clean caching-related parameters from `storage_options`
        # before propagating them as `request_options` through `self.kwargs`.
        # TODO: Maybe rename `self.kwargs` to `self.request_options` to make
        #       it clearer.
        request_options = copy(storage_options)
        self.use_listings_cache = request_options.pop("use_listings_cache", False)
        request_options.pop("listings_expiry_time", None)
        request_options.pop("max_paths", None)
        request_options.pop("skip_instance_cache", None)
        self.kwargs = request_options

        if not asynchronous:
            sync(self.loop, self.set_session)

    @staticmethod
    def close_session(loop, session):
        if loop is not None and loop.is_running():
            try:
                sync(loop, session.close, timeout=0.1)
                return
            except (TimeoutError, FSTimeoutError):
                pass
        if session._connector is not None:
            # close after loop is dead
            session._connector._close()

    async def set_session(self):
        if self._session is None:
            self._session = await get_client(loop=self.loop, **self.client_kwargs)
            if not self.asynchronous:
                weakref.finalize(self, self.close_session, self.loop, self._session)
        return self._session

    @classmethod
    def _strip_protocol(cls, path):
        """For HTTP, we always want to keep the full URL"""
        return path

    @classmethod
    def _parent(cls, path):
        # override, since _strip_protocol is different for URLs
        par = super()._parent(path)
        if len(par) > 7:  # "http://..."
            return par
        return ""

    async def _ls_real(self, url, detail=True, **kwargs):
        # ignoring URL-encoded arguments
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(url)
        session = await self.set_session()
        async with session.get(url, **self.kwargs) as r:
            self._raise_not_found_for_status(r, url)
            text = await r.text()
        if self.simple_links:
            links = ex2.findall(text) + [u[2] for u in ex.findall(text)]
        else:
            links = [u[2] for u in ex.findall(text)]
        out = set()
        parts = urlparse(url)
        for l in links:
            if isinstance(l, tuple):
                l = l[1]
            if l.startswith("/") and len(l) > 1:
                # absolute URL on this server
                l = parts.scheme + "://" + parts.netloc + l
            if l.startswith("http"):
                if self.same_schema and l.startswith(url.rstrip("/") + "/"):
                    out.add(l)
                elif l.replace("https", "http").startswith(
                    url.replace("https", "http").rstrip("/") + "/"
                ):
                    # allowed to cross http <-> https
                    out.add(l)
            else:
                if l not in ["..", "../"]:
                    # Ignore FTP-like "parent"
                    out.add("/".join([url.rstrip("/"), l.lstrip("/")]))
        if not out and url.endswith("/"):
            out = await self._ls_real(url.rstrip("/"), detail=False)
        if detail:
            return [
                {
                    "name": u,
                    "size": None,
                    "type": "directory" if u.endswith("/") else "file",
                }
                for u in out
            ]
        else:
            return list(sorted(out))
        return out

    async def _ls(self, url, detail=True, **kwargs):

        if self.use_listings_cache and url in self.dircache:
            out = self.dircache[url]
        else:
            out = await self._ls_real(url, detail=detail, **kwargs)
            self.dircache[url] = out
        return out

    ls = sync_wrapper(_ls)

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError if the status code is a 404, otherwise uses raise_for_status.
        """
        if response.status == 404:
            raise FileNotFoundError(url)
        response.raise_for_status()

    async def _cat_file(self, url, start=None, end=None, **kwargs):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(url)

        # TODO: extract into testable utility function?
        if start is not None or end is not None:
            headers = kw.pop("headers", {}).copy()
            size = None
            suff = False
            if start is not None and start < 0:
                # if start is negative and end None, end is the "suffix length"
                if end is None:
                    end = -start
                    start = ""
                    suff = True
                else:
                    size = size or (await self._info(url))["size"]
                    start = size + start
            elif start is None:
                start = 0
            if not suff:
                if end is not None and end < 0:
                    if start is not None:
                        size = size or (await self._info(url))["size"]
                        end = size + end
                elif end is None:
                    end = ""
                if isinstance(end, int):
                    end -= 1  # bytes range is inclusive

            headers["Range"] = "bytes=%s-%s" % (start, end)
            kw["headers"] = headers
        session = await self.set_session()
        async with session.get(url, **kw) as r:
            self._raise_not_found_for_status(r, url)
            out = await r.read()
        return out

    async def _get_file(self, rpath, lpath, chunk_size=5 * 2 ** 20, **kwargs):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(rpath)
        session = await self.set_session()
        async with session.get(rpath, **self.kwargs) as r:
            self._raise_not_found_for_status(r, url)
            with open(lpath, "wb") as fd:
                chunk = True
                while chunk:
                    chunk = await r.content.read(chunk_size)
                    fd.write(chunk)

    async def _exists(self, path, **kwargs):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        try:
            logger.debug(path)
            session = await self.set_session()
            r = await session.get(path, **kw)
            async with r:
                return r.status < 400
        except (requests.HTTPError, aiohttp.ClientError):
            return False

    async def _isfile(self, path, **kwargs):
        return await self._exists(path, **kwargs)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=None,  # XXX: This differs from the base class.
        cache_type=None,
        cache_options=None,
        size=None,
        **kwargs,
    ):
        """Make a file-like object

        Parameters
        ----------
        path: str
            Full URL with protocol
        mode: string
            must be "rb"
        block_size: int or None
            Bytes to download in one request; use instance value if None. If
            zero, will return a streaming Requests file-like instance.
        kwargs: key-value
            Any other parameters, passed to requests calls
        """
        if mode != "rb":
            raise NotImplementedError
        block_size = block_size if block_size is not None else self.block_size
        kw = self.kwargs.copy()
        kw["asynchronous"] = self.asynchronous
        kw.update(kwargs)
        size = size or self.size(path)
        session = sync(self.loop, self.set_session)
        if block_size and size:
            return HTTPFile(
                self,
                path,
                session=session,
                block_size=block_size,
                mode=mode,
                size=size,
                cache_type=cache_type or self.cache_type,
                cache_options=cache_options or self.cache_options,
                loop=self.loop,
                **kw,
            )
        else:
            return HTTPStreamFile(
                self, path, mode=mode, loop=self.loop, session=session, **kw
            )

    def ukey(self, url):
        """Unique identifier; assume HTTP files are static, unchanging"""
        return tokenize(url, self.kwargs, self.protocol)

    async def _info(self, url, **kwargs):
        """Get info of URL

        Tries to access location via HEAD, and then GET methods, but does
        not fetch the data.

        It is possible that the server does not supply any size information, in
        which case size will be given as None (and certain operations on the
        corresponding file will not work).
        """
        size = False
        for policy in ["head", "get"]:
            try:
                session = await self.set_session()
                size = await _file_size(
                    url, size_policy=policy, session=session, **self.kwargs
                )
                if size:
                    break
            except Exception:
                pass
        else:
            # get failed, so conclude URL does not exist
            if size is False:
                raise FileNotFoundError(url)
        return {"name": url, "size": size or None, "type": "file"}

    async def _glob(self, path, **kwargs):
        """
        Find files by glob-matching.

        This implementation is idntical to the one in AbstractFileSystem,
        but "?" is not considered as a character for globbing, because it is
        so common in URLs, often identifying the "query" part.
        """
        import re

        ends = path.endswith("/")
        path = self._strip_protocol(path)
        indstar = path.find("*") if path.find("*") >= 0 else len(path)
        indbrace = path.find("[") if path.find("[") >= 0 else len(path)

        ind = min(indstar, indbrace)

        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            root = path
            depth = 1
            if ends:
                path += "/*"
            elif await self._exists(path):
                if not detail:
                    return [path]
                else:
                    return {path: await self._info(path)}
            else:
                if not detail:
                    return []  # glob of non-existent returns empty
                else:
                    return {}
        elif "/" in path[:ind]:
            ind2 = path[:ind].rindex("/")
            root = path[: ind2 + 1]
            depth = None if "**" in path else path[ind2 + 1 :].count("/") + 1
        else:
            root = ""
            depth = None if "**" in path else path[ind + 1 :].count("/") + 1

        allpaths = await self._find(
            root, maxdepth=depth, withdirs=True, detail=True, **kwargs
        )
        # Escape characters special to python regex, leaving our supported
        # special characters in place.
        # See https://www.gnu.org/software/bash/manual/html_node/Pattern-Matching.html
        # for shell globbing details.
        pattern = (
            "^"
            + (
                path.replace("\\", r"\\")
                .replace(".", r"\.")
                .replace("+", r"\+")
                .replace("//", "/")
                .replace("(", r"\(")
                .replace(")", r"\)")
                .replace("|", r"\|")
                .replace("^", r"\^")
                .replace("$", r"\$")
                .replace("{", r"\{")
                .replace("}", r"\}")
                .rstrip("/")
            )
            + "$"
        )
        pattern = re.sub("[*]{2}", "=PLACEHOLDER=", pattern)
        pattern = re.sub("[*]", "[^/]*", pattern)
        pattern = re.compile(pattern.replace("=PLACEHOLDER=", ".*"))
        out = {
            p: allpaths[p]
            for p in sorted(allpaths)
            if pattern.match(p.replace("//", "/").rstrip("/"))
        }
        if detail:
            return out
        else:
            return list(out)

    async def _isdir(self, path):
        # override, since all URLs are (also) files
        return bool(await self._ls(path))


class HTTPFile(AbstractBufferedFile):
    """
    A file-like object pointing to a remove HTTP(S) resource

    Supports only reading, with read-ahead of a predermined block-size.

    In the case that the server does not supply the filesize, only reading of
    the complete file in one go is supported.

    Parameters
    ----------
    url: str
        Full URL of the remote resource, including the protocol
    session: requests.Session or None
        All calls will be made within this session, to avoid restarting
        connections where the server allows this
    block_size: int or None
        The amount of read-ahead to do, in bytes. Default is 5MB, or the value
        configured for the FileSystem creating this file
    size: None or int
        If given, this is the size of the file in bytes, and we don't attempt
        to call the server to find the value.
    kwargs: all other key-values are passed to requests calls.
    """

    def __init__(
        self,
        fs,
        url,
        session=None,
        block_size=None,
        mode="rb",
        cache_type="bytes",
        cache_options=None,
        size=None,
        loop=None,
        asynchronous=False,
        **kwargs,
    ):
        if mode != "rb":
            raise NotImplementedError("File mode not supported")
        self.asynchronous = asynchronous
        self.url = url
        self.session = session
        self.details = {"name": url, "size": size, "type": "file"}
        super().__init__(
            fs=fs,
            path=url,
            mode=mode,
            block_size=block_size,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )
        self.loop = loop

    def read(self, length=-1):
        """Read bytes from file

        Parameters
        ----------
        length: int
            Read up to this many bytes. If negative, read all content to end of
            file. If the server has not supplied the filesize, attempting to
            read only part of the data will raise a ValueError.
        """
        if (
            (length < 0 and self.loc == 0)  # explicit read all
            # but not when the size is known and fits into a block anyways
            and not (self.size is not None and self.size <= self.blocksize)
        ):
            self._fetch_all()
        if self.size is None:
            if length < 0:
                self._fetch_all()
        else:
            length = min(self.size - self.loc, length)
        return super().read(length)

    async def async_fetch_all(self):
        """Read whole file in one shot, without caching

        This is only called when position is still at zero,
        and read() is called without a byte-count.
        """
        logger.debug(f"Fetch all for {self}")
        if not isinstance(self.cache, AllBytes):
            r = await self.session.get(self.url, **self.kwargs)
            async with r:
                r.raise_for_status()
                out = await r.read()
                self.cache = AllBytes(
                    size=len(out), fetcher=None, blocksize=None, data=out
                )
                self.size = len(out)

    _fetch_all = sync_wrapper(async_fetch_all)

    async def async_fetch_range(self, start, end):
        """Download a block of data

        The expectation is that the server returns only the requested bytes,
        with HTTP code 206. If this is not the case, we first check the headers,
        and then stream the output - if the data size is bigger than we
        requested, an exception is raised.
        """
        logger.debug(f"Fetch range for {self}: {start}-{end}")
        kwargs = self.kwargs.copy()
        headers = kwargs.pop("headers", {}).copy()
        headers["Range"] = "bytes=%i-%i" % (start, end - 1)
        logger.debug(self.url + " : " + headers["Range"])
        r = await self.session.get(self.url, headers=headers, **kwargs)
        async with r:
            if r.status == 416:
                # range request outside file
                return b""
            r.raise_for_status()
            if r.status == 206:
                # partial content, as expected
                out = await r.read()
            elif "Content-Length" in r.headers:
                cl = int(r.headers["Content-Length"])
                if cl <= end - start:
                    # data size OK
                    out = await r.read()
                else:
                    raise ValueError(
                        "Got more bytes (%i) than requested (%i)" % (cl, end - start)
                    )
            else:
                cl = 0
                out = []
                while True:
                    chunk = await r.content.read(2 ** 20)
                    # data size unknown, let's see if it goes too big
                    if chunk:
                        out.append(chunk)
                        cl += len(chunk)
                        if cl > end - start:
                            raise ValueError(
                                "Got more bytes so far (>%i) than requested (%i)"
                                % (cl, end - start)
                            )
                    else:
                        break
                out = b"".join(out)
            return out

    _fetch_range = sync_wrapper(async_fetch_range)

    def close(self):
        pass

    def __reduce__(self):
        return reopen, (
            self.fs,
            self.url,
            self.mode,
            self.blocksize,
            self.cache.name,
            self.size,
        )


def reopen(fs, url, mode, blocksize, cache_type, size=None):
    return fs.open(
        url, mode=mode, block_size=blocksize, cache_type=cache_type, size=size
    )


magic_check = re.compile("([*[])")


def has_magic(s):
    match = magic_check.search(s)
    return match is not None


class HTTPStreamFile(AbstractBufferedFile):
    def __init__(self, fs, url, mode="rb", loop=None, session=None, **kwargs):
        self.asynchronous = kwargs.pop("asynchronous", False)
        self.url = url
        self.loop = loop
        self.session = session
        if mode != "rb":
            raise ValueError
        self.details = {"name": url, "size": None}
        super().__init__(fs=fs, path=url, mode=mode, cache_type="none", **kwargs)

        async def cor():
            r = await self.session.get(url, **kwargs).__aenter__()
            return r

        self.r = sync(self.loop, cor)

    def seek(self, *args, **kwargs):
        raise ValueError("Cannot seek streaming HTTP file")

    async def _read(self, num=-1):
        out = await self.r.content.read(num)
        self.loc += len(out)
        return out

    read = sync_wrapper(_read)

    async def _close(self):
        self.r.close()

    def close(self):
        asyncio.run_coroutine_threadsafe(self._close(), self.loop)

    def __reduce__(self):
        return reopen, (self.fs, self.url, self.mode, self.blocksize, self.cache.name)


async def get_range(session, url, start, end, file=None, **kwargs):
    # explicit get a range when we know it must be safe
    kwargs = kwargs.copy()
    headers = kwargs.pop("headers", {}).copy()
    headers["Range"] = "bytes=%i-%i" % (start, end - 1)
    r = await session.get(url, headers=headers, **kwargs)
    r.raise_for_status()
    async with r:
        out = await r.read()
    if file:
        with open(file, "rb+") as f:
            f.seek(start)
            f.write(out)
    else:
        return out


async def _file_size(url, session=None, size_policy="head", **kwargs):
    """Call HEAD on the server to get file size

    Default operation is to explicitly allow redirects and use encoding
    'identity' (no compression) to get the true size of the target.
    """
    logger.debug("Retrieve file size for %s" % url)
    kwargs = kwargs.copy()
    ar = kwargs.pop("allow_redirects", True)
    head = kwargs.get("headers", {}).copy()
    head["Accept-Encoding"] = "identity"
    kwargs["headers"] = head
    session = session or await get_client()
    if size_policy == "head":
        r = await session.head(url, allow_redirects=ar, **kwargs)
    elif size_policy == "get":
        r = await session.get(url, allow_redirects=ar, **kwargs)
    else:
        raise TypeError('size_policy must be "head" or "get", got %s' "" % size_policy)
    async with r:
        # TODO:
        #  recognise lack of 'Accept-Ranges', or  'Accept-Ranges': 'none' (not 'bytes')
        #  to mean streaming only, no random access => return None
        if "Content-Length" in r.headers:
            return int(r.headers["Content-Length"])
        elif "Content-Range" in r.headers:
            return int(r.headers["Content-Range"].split("/")[1])
        r.close()


file_size = sync_wrapper(_file_size)
