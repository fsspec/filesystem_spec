from typing import ClassVar

from fsspec import AbstractFileSystem

__all__ = ("ChainedFileSystem",)


class ChainedFileSystem(AbstractFileSystem):
    protocol: ClassVar[str] = "chained"
