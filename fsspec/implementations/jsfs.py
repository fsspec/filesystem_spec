import asyncio
import urllib.error
import urllib.parse
from json import dumps, loads

from js import Blob, XMLHttpRequest, window
from pyodide.http import FetchResponse, pyfetch

# from fsspec.implementations.http import HTTPFileSystem
from fsspec.registry import known_implementations

# https://pyodide.org/en/stable/usage/api/python-api.html#pyodide.http.pyfetch

# https://pyodide.org/en/stable/usage/api/python-api.html#pyodide.http.FetchResponse


class AioHTTPShim(FetchResponse):
    @classmethod
    def from_response(cls, resp):
        ob = object().__new__(cls)
        ob.__dict__ = resp.__dict__
        return ob

    @property
    def headers(self):
        # TODO: reveal the Response.headers, a Headers object
        #  https://developer.mozilla.org/en-US/docs/Web/API/Headers
        return {}

    def __aenter__(self):
        return self

    def __await__(self):
        yield
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        return self

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(str(self.status_text))

    def read(self):
        return self.bytes()

    def __str__(self):
        return f"Response from {self.url}"


class JSSession:
    def __init__(self, headers=None):
        self.headers = headers

    async def _call(self, url, method, headers=None, body=None, **kwargs):
        kwargs.update({"method": method, "mode": "cors"})
        if body and method not in ["GET", "HEAD"]:
            kwargs["body"] = body
        if headers:
            kwargs["headers"] = headers or self.headers
        return AioHTTPShim.from_response(await pyfetch(url, **kwargs))

    async def get(self, url, headers=None, **kwargs):
        return await self._call(url, "GET", headers, **kwargs)

    async def head(self, url, headers=None, **kwargs):
        return await self._call(url, "HEAD", headers, **kwargs)

    async def post(self, url, headers=None, **kwargs):
        return await self._call(url, "POST", headers, **kwargs)

    async def delete(self, url, headers=None, **kwargs):
        return await self._call(url, "DELETE", headers, **kwargs)

    async def put(self, url, headers=None, **kwargs):
        return await self._call(url, "PUT", headers, **kwargs)

    def __str__(self):
        return """session-like"""


class PyodideFileSystem:  # (HTTPFileSystem):
    def __init__(self, asynchronous=True, **kwargs):
        super().__init__(asynchronous=True, **kwargs)
        if not asynchronous:
            raise TypeError("Can only be run in async mode")
        self._session = JSSession()
        self._loop = asyncio.get_event_loop()

    async def set_session(self):
        return self._session

    @staticmethod
    def close_session(*args):
        pass

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        headers = kwargs.pop("headers", {})
        if start or end:
            headers["Range"] = f"bytes={str(start or '') - str(end or '')}"
        r = await self.session.get(path, **headers)
        r.raise_for_status()
        async with r as r:
            return await r.read()

    def __str__(self):
        return "JSFS"


class JsHttpException(urllib.error.HTTPError):
    ...


class ResponseProxy:
    def __init__(self, req):
        self.request = req
        self._data = None
        self._headers = None

    @property
    def raw(self):
        if self._data is None:
            self._data = str(self.request.response).encode()
        return self._data

    @property
    def headers(self):
        if self._headers is None:
            self._headers = dict(
                [
                    _.split(": ")
                    for _ in self.request.getAllResponseHeaders().strip().split("\r\n")
                ]
            )
        return self._headers

    @property
    def status_code(self):
        return int(self.request.status)

    def raise_for_status(self):
        if not self.ok:
            raise JsHttpException(
                self.url, self.status_code, self.reason, self.headers, None
            )

    @property
    def reason(self):
        return self.request.statusText

    @property
    def ok(self):
        return self.status_code < 400

    @property
    def url(self):
        return self.request.response.responseURL

    @property
    def text(self):
        # TODO: encoding from headers
        return self.raw.decode()

    @property
    def json(self):
        return loads(self.text)


class RequestsSessionShim:
    def __init__(self):
        self.headers = {}

    def request(
        self,
        method,
        url,
        params=None,
        data=None,
        headers=None,
        cookies=None,
        files=None,
        auth=None,
        timeout=None,
        allow_redirects=None,
        proxies=None,
        hooks=None,
        stream=None,
        verify=None,
        cert=None,
        json=None,
    ):
        if (
            cert
            or verify
            or proxies
            or files
            or cookies
            or hooks
            or stream
            or allow_redirects
        ):
            raise NotImplementedError
        if data and json:
            raise ValueError("Use json= or data=, not both")
        req = XMLHttpRequest.new()
        extra = auth if auth else ()
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        req.open(method, url, False, *extra)
        if timeout:
            req.timeout = timeout
        if headers:
            for k, v in headers.items():
                req.setRequestHeader(k, v)

        req.setRequestHeader("Accept", "application/octet-stream")
        if json:
            blob = Blob.new([dumps(data)], {type: "application/json"})
            req.send(blob)
        elif data:
            blob = Blob.new([data], {type: "application/octet-stream"})
            req.send(blob)
        else:
            req.send(None)
        window.req = req
        return ResponseProxy(req)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def head(self, url, **kwargs):
        return self.request("HEAD", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST}", url, **kwargs)

    def put(self, url, **kwargs):
        return self.request("PUT", url, **kwargs)

    def patch(self, url, **kwargs):
        return self.request("PATCH", url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url, **kwargs)


def set_impl(asyn=False):
    if asyn:
        bits = {
            "class": "jsfs.PyodideFileSystem",
        }
    else:
        bits = {"class": ""}

    known_implementations["http"] = bits
