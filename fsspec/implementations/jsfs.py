import asyncio

from pyodide.http import FetchResponse, pyfetch

from fsspec.implementations.http import HTTPFileSystem
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


class PyodideFileSystem(HTTPFileSystem):
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
            headers["Range"] = f"bytes={str(start or '')-str(end or '')}"
        r = await self.session.get(path, **headers)
        r.raise_for_status()
        async with r as r:
            return await r.read()

    def __str__(self):
        return "JSFS"


known_implementations["http"] = {
    "class": "jsfs.PyodideFileSystem",
}
