from typing import ClassVar

from fsspec import AbstractFileSystem

__all__ = ("ChainedFileSystem",)


class ChainedFileSystem(AbstractFileSystem):
    chained: ClassVar[str] = "chained"
