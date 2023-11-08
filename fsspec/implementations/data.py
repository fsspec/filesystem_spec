import base64
import io
from urllib.parse import unquote

from fsspec import AbstractFileSystem


class DataFileSystem(AbstractFileSystem):
    """A handy decoder for data-URLs

    Example
    -------
    >>> with fsspec.open("data:,Hello%2C%20World%21") as f:
    ...     print(f.read())
    b"Hello, World!"

    """

    protocol = "data"

    def cat_file(self, path, start=None, end=None, **kwargs):
        pref, data = path.split(",", 1)
        if pref.endswith("base64"):
            return base64.b64decode(data)[start:end]
        return unquote(data).encode()[start:end]

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        if "r" not in mode:
            raise ValueError("Read only filesystem")
        return io.BytesIO(self.cat_file(path))
