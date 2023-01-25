from __future__ import absolute_import, division, print_function

import io
import logging
import re
import weakref
from copy import copy
from urllib.parse import urlparse

import requests
import yarl

from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import DEFAULT_BLOCK_SIZE, isfilelike, nullcontext, tokenize

from ..caching import AllBytes

# https://stackoverflow.com/a/15926317/3821154
ex = re.compile(r"""<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)""")
ex2 = re.compile(r"""(?P<url>http[s]?://[-a-zA-Z0-9@:%_+.~#?&/=]+)""")
logger = logging.getLogger("fsspec.http")


class HTTPFileSystem(AbstractFileSystem):
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
        cache_type="bytes",
        cache_options=None,
        client_kwargs=None,
        encoded=False,
        **storage_options,
    ):
        """

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
        super().__init__(self, **storage_options)
        self.block_size = block_size if block_size is not None else DEFAULT_BLOCK_SIZE
        self.simple_links = simple_links
        self.same_schema = same_scheme
        self.cache_type = cache_type
        self.cache_options = cache_options
        self.client_kwargs = client_kwargs or {}
        self.encoded = encoded
        self.kwargs = storage_options

        session = requests.Session(**(client_kwargs or {}))
        weakref.finalize(self, session.close)
        self.session = session

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

    @property
    def fsid(self):
        return "http"

    def encode_url(self, url):
        return yarl.URL(url, encoded=self.encoded)

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

    def _ls_real(self, url, detail=True, **kwargs):
        # ignoring URL-encoded arguments
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(url)
        r = self.session.get(self.encode_url(url), **self.kwargs)
        self._raise_not_found_for_status(r, url)
        text = r.text()
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
            out = self._ls_real(url.rstrip("/"), detail=False)
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

    def ls(self, url, detail=True, **kwargs):

        if self.use_listings_cache and url in self.dircache:
            out = self.dircache[url]
        else:
            out = self._ls_real(url, detail=detail, **kwargs)
            self.dircache[url] = out
        return out

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError for 404s, otherwise uses raise_for_status.
        """
        if response.status_code == 404:
            raise FileNotFoundError(url)
        response.raise_for_status()

    def cat_file(self, url, start=None, end=None, **kwargs):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(url)

        if start is not None or end is not None:
            if start == end:
                return b""
            headers = kw.pop("headers", {}).copy()

            headers["Range"] = self._process_limits(url, start, end)
            kw["headers"] = headers
        r = self.session.get(self.encode_url(url), **kw)
        self._raise_not_found_for_status(r, url)
        return r.content

    def get_file(
        self, rpath, lpath, chunk_size=5 * 2**20, callback=_DEFAULT_CALLBACK, **kwargs
    ):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        logger.debug(rpath)
        r = self.session.get(self.encode_url(rpath), **kw)
        try:
            size = int(r.headers["content-length"])
        except (ValueError, KeyError):
            size = None

        callback.set_size(size)
        self._raise_not_found_for_status(r, rpath)
        if not isfilelike(lpath):
            lpath = open(lpath, "wb")
        chunk = True
        while chunk:
            r.raw.decode_content = True
            chunk = r.raw.read(chunk_size)
            lpath.write(chunk)
            callback.relative_update(len(chunk))

    def put_file(
        self,
        lpath,
        rpath,
        chunk_size=5 * 2**20,
        callback=_DEFAULT_CALLBACK,
        method="post",
        **kwargs,
    ):
        def gen_chunks():
            # Support passing arbitrary file-like objects
            # and use them instead of streams.
            if isinstance(lpath, io.IOBase):
                context = nullcontext(lpath)
                use_seek = False  # might not support seeking
            else:
                context = open(lpath, "rb")
                use_seek = True

            with context as f:
                if use_seek:
                    callback.set_size(f.seek(0, 2))
                    f.seek(0)
                else:
                    callback.set_size(getattr(f, "size", None))

                chunk = f.read(chunk_size)
                while chunk:
                    yield chunk
                    callback.relative_update(len(chunk))
                    chunk = f.read(chunk_size)

        kw = self.kwargs.copy()
        kw.update(kwargs)

        method = method.lower()
        if method not in ("post", "put"):
            raise ValueError(
                f"method has to be either 'post' or 'put', not: {method!r}"
            )

        meth = getattr(self.ession, method)
        resp = meth(rpath, data=gen_chunks(), **kw)
        self._raise_not_found_for_status(resp, rpath)

    def exists(self, path, **kwargs):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        try:
            logger.debug(path)
            session = self.set_session()
            r = session.get(self.encode_url(path), **kw)
            return r.status_code < 400
        except requests.HTTPError:
            return False

    def isfile(self, path, **kwargs):
        return self.exists(path, **kwargs)

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
        kw.update(kwargs)
        size = size or self.info(path, **kwargs)["size"]
        if block_size and size:
            return HTTPFile(
                self,
                path,
                session=self.session,
                block_size=block_size,
                mode=mode,
                size=size,
                cache_type=cache_type or self.cache_type,
                cache_options=cache_options or self.cache_options,
                **kw,
            )
        else:
            return HTTPStreamFile(
                self,
                path,
                mode=mode,
                session=self.session,
                **kw,
            )

    def ukey(self, url):
        """Unique identifier; assume HTTP files are static, unchanging"""
        return tokenize(url, self.kwargs, self.protocol)

    def info(self, url, **kwargs):
        """Get info of URL

        Tries to access location via HEAD, and then GET methods, but does
        not fetch the data.

        It is possible that the server does not supply any size information, in
        which case size will be given as None (and certain operations on the
        corresponding file will not work).
        """
        info = {}
        for policy in ["head", "get"]:
            try:
                info.update(
                    _file_info(
                        self.encode_url(url),
                        size_policy=policy,
                        session=self.session,
                        **self.kwargs,
                        **kwargs,
                    )
                )
                if info.get("size") is not None:
                    break
            except Exception as exc:
                if policy == "get":
                    # If get failed, then raise a FileNotFoundError
                    raise FileNotFoundError(url) from exc
                logger.debug(str(exc))

        return {"name": url, "size": None, **info, "type": "file"}

    def glob(self, path, **kwargs):
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
            elif self.exists(path):
                if not detail:
                    return [path]
                else:
                    return {path: self.info(path)}
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

        allpaths = self.find(root, maxdepth=depth, withdirs=True, detail=True, **kwargs)
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

    def isdir(self, path):
        # override, since all URLs are (also) files
        try:
            return bool(self._ls(path))
        except (FileNotFoundError, ValueError):
            return False


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
        **kwargs,
    ):
        if mode != "rb":
            raise NotImplementedError("File mode not supported")
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

    def _fetch_all(self):
        """Read whole file in one shot, without caching

        This is only called when position is still at zero,
        and read() is called without a byte-count.
        """
        logger.debug(f"Fetch all for {self}")
        if not isinstance(self.cache, AllBytes):
            r = self.session.get(self.fs.encode_url(self.url), **self.kwargs)
            r.raise_for_status()
            out = r.content
            self.cache = AllBytes(size=len(out), fetcher=None, blocksize=None, data=out)
            self.size = len(out)

    def _parse_content_range(self, headers):
        """Parse the Content-Range header"""
        s = headers.get("Content-Range", "")
        m = re.match(r"bytes (\d+-\d+|\*)/(\d+|\*)", s)
        if not m:
            return None, None, None

        if m[1] == "*":
            start = end = None
        else:
            start, end = [int(x) for x in m[1].split("-")]
        total = None if m[2] == "*" else int(m[2])
        return start, end, total

    def _fetch_range(self, start, end):
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
        logger.debug(str(self.url) + " : " + headers["Range"])
        r = self.session.get(self.fs.encode_url(self.url), headers=headers, **kwargs)
        if r.status_code == 416:
            # range request outside file
            return b""
        r.raise_for_status()

        # If the server has handled the range request, it should reply
        # with status 206 (partial content). But we'll guess that a suitable
        # Content-Range header or a Content-Length no more than the
        # requested range also mean we have got the desired range.
        response_is_range = (
            r.status_code == 206
            or self._parse_content_range(r.headers)[0] == start
            or int(r.headers.get("Content-Length", end + 1)) <= end - start
        )

        if response_is_range:
            # partial content, as expected
            out = r.content
        elif start > 0:
            raise ValueError(
                "The HTTP server doesn't appear to support range requests. "
                "Only reading this file from the beginning is supported. "
                "Open with block_size=0 for a streaming file interface."
            )
        else:
            # Response is not a range, but we want the start of the file,
            # so we can read the required amount anyway.
            cl = 0
            out = []
            while True:
                r.raw.decode_content = True
                chunk = r.raw.read(2**20)
                # data size unknown, let's read until we have enough
                if chunk:
                    out.append(chunk)
                    cl += len(chunk)
                    if cl > end - start:
                        break
                else:
                    break
            r.raw.close()
            out = b"".join(out)[: end - start]
        return out


magic_check = re.compile("([*[])")


def has_magic(s):
    match = magic_check.search(s)
    return match is not None


class HTTPStreamFile(AbstractBufferedFile):
    def __init__(self, fs, url, mode="rb", session=None, **kwargs):
        self.url = url
        self.session = session
        if mode != "rb":
            raise ValueError
        self.details = {"name": url, "size": None}
        super().__init__(fs=fs, path=url, mode=mode, cache_type="readahead", **kwargs)

        r = self.session.get(self.fs.encode_url(url), stream=True, **kwargs)
        r.raw.decode_content = True
        self.fs._raise_not_found_for_status(r, url)

        self.r = r

    def seek(self, *args, **kwargs):
        raise ValueError("Cannot seek streaming HTTP file")

    def read(self, num=-1):
        if num < 0:
            return self.content
        bufs = []
        leng = 0
        while not self.r.raw.closed and leng < num:
            out = self.r.raw.read(num)
            if out:
                bufs.append(out)
            leng += len(out)
        self.loc += leng
        return b"".join(bufs)

    def close(self):
        self.r.close()


def get_range(session, url, start, end, **kwargs):
    # explicit get a range when we know it must be safe
    kwargs = kwargs.copy()
    headers = kwargs.pop("headers", {}).copy()
    headers["Range"] = "bytes=%i-%i" % (start, end - 1)
    r = session.get(url, headers=headers, **kwargs)
    r.raise_for_status()
    return r.content


def _file_info(url, session, size_policy="head", **kwargs):
    """Call HEAD on the server to get details about the file (size/checksum etc.)

    Default operation is to explicitly allow redirects and use encoding
    'identity' (no compression) to get the true size of the target.
    """
    logger.debug("Retrieve file size for %s" % url)
    kwargs = kwargs.copy()
    ar = kwargs.pop("allow_redirects", True)
    head = kwargs.get("headers", {}).copy()
    head["Accept-Encoding"] = "identity"
    kwargs["headers"] = head

    info = {}
    if size_policy == "head":
        r = session.head(url, allow_redirects=ar, **kwargs)
    elif size_policy == "get":
        r = session.get(url, allow_redirects=ar, **kwargs)
    else:
        raise TypeError('size_policy must be "head" or "get", got %s' "" % size_policy)
        r.raise_for_status()

        # TODO:
        #  recognise lack of 'Accept-Ranges',
        #                 or 'Accept-Ranges': 'none' (not 'bytes')
        #  to mean streaming only, no random access => return None
    if "Content-Length" in r.headers:
        info["size"] = int(r.headers["Content-Length"])
    elif "Content-Range" in r.headers:
        info["size"] = int(r.headers["Content-Range"].split("/")[1])

    for checksum_field in ["ETag", "Content-MD5", "Digest"]:
        if r.headers.get(checksum_field):
            info[checksum_field] = r.headers[checksum_field]

    return info


def _file_size(url, session, *args, **kwargs):
    info = _file_info(url, session=session, *args, **kwargs)
    return info.get("size")
