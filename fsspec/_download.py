"""Shared byte-range and destination handling for file downloads."""

import os
import re
from contextlib import contextmanager
from operator import index

from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.utils import isfilelike


class _Download:
    def __init__(
        self,
        lpath,
        *,
        outfile=None,
        start=None,
        end=None,
        resume=False,
        callback=DEFAULT_CALLBACK,
    ):
        self.outfile = (
            outfile if outfile is not None else (lpath if isfilelike(lpath) else None)
        )
        self.lpath = lpath
        self.resume = resume
        self.ranged = start is not None or end is not None or resume
        self.callback = callback
        if resume and start is not None:
            raise ValueError("resume and start cannot be used together")
        if resume and self.outfile is not None:
            raise ValueError("resume requires a local filename, not a file-like object")
        if self.outfile is None and lpath is None:
            raise ValueError("A local filename or outfile is required")

        self._start = 0 if start is None else index(start)
        self._end = None if end is None else index(end)
        if resume:
            try:
                self._start = os.path.getsize(lpath)
            except FileNotFoundError:
                self._start = 0
        self.offset = self._start if resume else 0
        self.start = self._start
        self.end = self._end
        self.length = None
        self.count = 0
        if not self.needs_size:
            self.set_size(None)

    @property
    def needs_size(self):
        return self._start < 0 or (self._end is not None and self._end < 0)

    def set_size(self, size):
        """Resolve Python-style, half-open bounds without opening the destination."""
        start, end = self._start, self._end
        if size is not None:
            if self.resume and self.offset > size:
                raise ValueError("Local file is larger than the remote file")
            start, end, _ = slice(start, end).indices(size)
        elif self.needs_size:
            raise ValueError("The remote size is required for negative byte offsets")
        if self.resume and end is not None and self.offset > end:
            raise ValueError("Local file is larger than the requested end offset")
        self.start = start
        self.end = None if end is None else max(start, end)
        self.length = None if self.end is None else self.end - self.start

    def request_headers(self, kwargs):
        """Add a single HTTP range without modifying caller-owned headers."""
        headers = dict(kwargs.get("headers") or {})
        if any(key.lower() == "range" for key in headers):
            raise ValueError("Do not combine a Range header with start, end, or resume")
        headers = {
            key: value
            for key, value in headers.items()
            if key.lower() != "accept-encoding"
        }
        last = "" if self.end is None else self.end - 1
        headers["Range"] = f"bytes={self.start}-{last}"
        headers["Accept-Encoding"] = "identity"
        kwargs["headers"] = headers

    def response(self, status, headers):
        """Validate an HTTP range before any existing local bytes can be changed.

        Return False for a valid, empty response (including an already complete
        resumed download). The caller handles other HTTP errors normally.
        """
        headers = {key.lower(): value for key, value in headers.items()}
        try:
            size = int(headers["content-length"])
            if size < 0:
                size = None
        except (KeyError, TypeError, ValueError):
            size = None
        if not self.ranged:
            self.set_size(size)
            return True
        if headers.get("content-encoding", "identity").lower() != "identity":
            raise ValueError("A byte-range download requires identity content encoding")

        requested_start = self.start
        if status == 416:
            match = re.fullmatch(r"bytes \*/(\d+)", headers.get("content-range", ""))
            if match is None:
                raise ValueError("Missing or invalid Content-Range for HTTP 416")
            total = int(match[1])
            if requested_start < total:
                raise ValueError("Server rejected a satisfiable byte range")
            self.set_size(total)
            return False
        if status == 200:
            if self.start != 0 or self.end is not None:
                raise ValueError("Server does not support the requested byte range")
            self.set_size(size)
            return True
        if status != 206:
            raise ValueError(f"Unexpected HTTP status for a byte range: {status}")

        match = re.fullmatch(
            r"bytes (\d+)-(\d+)/(\d+|\*)", headers.get("content-range", "")
        )
        if match is None:
            raise ValueError("Missing or invalid Content-Range for HTTP 206")
        first, last = int(match[1]), int(match[2])
        total = None if match[3] == "*" else int(match[3])
        if first > last or (total is not None and last >= total):
            raise ValueError("Invalid Content-Range bounds")
        if total is not None:
            self.set_size(total)
        if first != requested_start or first != self.start:
            raise ValueError("Content-Range does not match the requested start offset")
        if self.end is not None and last != self.end - 1:
            raise ValueError("Content-Range does not match the requested end offset")
        self.length = last - first + 1
        if size is not None and size != self.length:
            raise ValueError("Content-Length does not match Content-Range")
        return True

    @contextmanager
    def open(self):
        """Own filename destinations, but never close caller-provided streams."""
        outfile = self.outfile
        owned = outfile is None
        if owned:
            outfile = open(self.lpath, "ab" if self.resume else "wb")
        try:
            if self.resume and os.fstat(outfile.fileno()).st_size != self.offset:
                raise ValueError("Local file changed while preparing the download")
            self.callback.set_size(
                None if self.length is None else self.offset + self.length
            )
            if self.resume:
                self.callback.absolute_update(self.offset)
            yield outfile
            if self.ranged and self.length is not None and self.count != self.length:
                raise OSError(
                    f"Incomplete download: expected {self.length} bytes, "
                    f"received {self.count}; the partial file has been retained"
                )
        finally:
            if owned:
                outfile.close()

    def write(self, outfile, data):
        if self.ranged and self.length is not None:
            if self.count + len(data) > self.length:
                raise OSError("Response contains more bytes than the requested range")
        remaining = memoryview(data)
        while remaining:
            written = outfile.write(remaining)
            if written is None:
                written = len(remaining)
            if written <= 0 or written > len(remaining):
                raise OSError("Destination did not write the requested bytes")
            self.count += written
            self.callback.relative_update(written)
            remaining = remaining[written:]
