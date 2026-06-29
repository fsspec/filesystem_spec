# pyright: reportExplicitAny=none, reportAny=none

import io
import os
from collections.abc import Callable, Generator
from pathlib import Path
from typing import IO, Any, ClassVar, Literal, Self, TypeAlias, TypedDict, overload

from _typeshed import Incomplete, OpenBinaryMode, OpenTextMode

from fsspec.mapping import FSMap

from .callbacks import DEFAULT_CALLBACK as DEFAULT_CALLBACK
from .callbacks import Callback
from .config import conf as conf
from .dircache import DirCache as DirCache
from .transaction import Transaction as Transaction
from .utils import isfilelike as isfilelike
from .utils import other_paths as other_paths
from .utils import read_block as read_block
from .utils import stringify_path as stringify_path
from .utils import tokenize as tokenize

logger: Incomplete

TPath: TypeAlias = str | os.PathLike[str] | Path

class FileInfo(TypedDict):
    name: str
    size: int | None
    type: Literal["file", "directory"] | str  # noqa: PYI051

def make_instance(cls, args, kwargs: dict[str, Any]): ...

class _Cached(type):
    def __init__(cls, *args, **kwargs: Any) -> None: ...
    def __call__(cls, *args, **kwargs: Any): ...

class AbstractFileSystem(metaclass=_Cached):
    cachable: bool
    blocksize: Incomplete
    sep: str
    protocol: ClassVar[str | tuple[str, ...]]
    async_impl: bool
    mirror_sync_methods: bool
    root_marker: str
    transaction_type: type[Transaction]
    storage_args: tuple[Any, ...]
    storage_options: dict[str, Any]
    dircache: Incomplete
    def __init__(self, *args, **storage_options) -> None: ...
    @property
    def fsid(self) -> None: ...
    def __dask_tokenize__(self): ...
    def __hash__(self): ...
    def __eq__(self, other): ...
    def __reduce__(self): ...
    def unstrip_protocol(self, name: str) -> str: ...
    @classmethod
    def current(cls) -> Self: ...
    @property
    def transaction(self) -> Transaction: ...
    def start_transaction(self) -> Transaction: ...
    def end_transaction(self) -> None: ...
    def invalidate_cache(self, path: str | None = None) -> None: ...
    def mkdir(
        self, path: TPath, create_parents: bool = True, **kwargs: Any
    ) -> None: ...
    def makedirs(self, path: TPath, exist_ok: bool = False) -> None: ...
    def rmdir(self, path: TPath) -> None: ...
    @overload
    def ls(
        self, path: TPath, detail: Literal[True], **kwargs: Any
    ) -> list[FileInfo]: ...
    @overload
    def ls(self, path: TPath, detail: Literal[False], **kwargs: Any) -> list[str]: ...
    def ls(
        self, path: TPath, detail: bool = True, **kwargs: Any
    ) -> list[str] | list[FileInfo]: ...
    def walk(
        self,
        path: TPath,
        maxdepth: int | None = None,
        topdown: bool = True,
        on_error: Literal["omit", "raise"] | Callable[[OSError], Any] = "omit",
        **kwargs: Any,
    ) -> Generator[Incomplete, Incomplete]: ...
    @overload
    def find(
        self,
        path: TPath,
        maxdepth: int | None,
        withdirs: bool,
        detail: Literal[True],
        **kwargs: Any,
    ) -> dict[str, FileInfo]: ...
    @overload
    def find(
        self,
        path: TPath,
        maxdepth: int | None,
        withdirs: bool,
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]: ...
    def find(
        self,
        path: TPath,
        maxdepth: int | None = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs: Any,
    ) -> list[str] | dict[str, FileInfo]: ...
    @overload
    def du(
        self,
        path: TPath,
        total: Literal[True],
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> int: ...
    @overload
    def du(
        self,
        path: TPath,
        total: Literal[False],
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> dict[str, int]: ...
    def du(
        self,
        path: TPath,
        total: bool = True,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> int | dict[str, int]: ...
    def glob(
        self, path: TPath, maxdepth: int | None = None, **kwargs: Any
    ) -> list[str] | dict[str, FileInfo]: ...
    def exists(self, path: TPath, **kwargs: Any) -> bool: ...
    def lexists(self, path: TPath, **kwargs: Any) -> bool: ...
    def info(self, path: TPath, **kwargs: Any) -> FileInfo: ...
    def checksum(self, path: TPath) -> int: ...
    def size(self, path: TPath) -> int | None: ...
    def sizes(self, paths: list[TPath]) -> list[int | None]: ...
    def isdir(self, path: TPath) -> bool: ...
    def isfile(self, path: TPath) -> bool: ...
    def read_text(
        self,
        path: TPath,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        **kwargs: Any,
    ) -> str | bytes: ...
    def write_text(
        self,
        path: TPath,
        value: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        **kwargs: Any,
    ) -> int: ...
    def cat_file(
        self,
        path: TPath,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> str | bytes: ...
    def pipe_file(
        self,
        path: TPath,
        value: bytes,
        mode: Literal["create", "overwrite"] = "overwrite",
        **kwargs: Any,
    ) -> None: ...
    def pipe(
        self,
        path: TPath | dict[TPath, bytes],
        value: bytes | None = None,
        **kwargs: Any,
    ) -> None: ...
    @overload
    def cat_ranges(
        self,
        paths: list[TPath],
        starts: int | list[int],
        ends: int | list[int],
        max_gap: None,
        on_error: Literal["raise"],
        **kwargs: Any,
    ) -> list[str | bytes]: ...
    @overload
    def cat_ranges(
        self,
        paths: list[TPath],
        starts: int | list[int],
        ends: int | list[int],
        max_gap: None,
        on_error: Literal["return"],
        **kwargs: Any,
    ) -> list[str | bytes | Exception]: ...
    def cat_ranges(
        self,
        paths: list[TPath],
        starts: int | list[int],
        ends: int | list[int],
        max_gap: None = None,
        on_error: Literal["raise", "return"] = "return",
        **kwargs: Any,
    ) -> list[str | bytes] | list[str | bytes | Exception]: ...
    def cat(
        self,
        path: TPath,
        recursive: bool = False,
        on_error: Literal["raise", "omit", "return"] = "raise",
        **kwargs: Any,
    ) -> str | bytes | dict[TPath, str | bytes]: ...
    def get_file(
        self,
        rpath: TPath,
        lpath: TPath,
        callback: Callback = ...,
        outfile: IO[bytes] | None = None,
        **kwargs: Any,
    ) -> None: ...
    def get(
        self,
        path1: TPath | list[TPath],
        path2: TPath | list[TPath],
        recursive: bool = False,
        callback: Callback = ...,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    def put_file(
        self,
        lpath: TPath,
        rpath: TPath,
        callback: Callback = ...,
        mode: Literal["create", "overwrite"] = "overwrite",
        **kwargs: Any,
    ) -> None: ...
    def put(
        self,
        path1: TPath | list[TPath],
        path2: TPath | list[TPath],
        recursive: bool = False,
        callback: Callback = ...,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    def head(self, path: TPath, size: int = 1024) -> str | bytes: ...
    def tail(self, path: TPath, size: int = 1024) -> str | bytes: ...
    def cp_file(self, path1: TPath, path2: TPath, **kwargs: Any) -> None: ...
    def copy(
        self,
        path1: TPath | list[TPath],
        path2: TPath | list[TPath],
        recursive: bool = False,
        maxdepth: int | None = None,
        on_error: Literal["raise", "ignore"] | None = None,
        **kwargs: Any,
    ) -> None: ...
    def expand_path(
        self,
        path: TPath,
        recursive: bool = False,
        maxdepth: int | None = None,
        assume_literal: bool = False,
        **kwargs: Any,
    ) -> list[str]: ...
    def mv(
        self,
        path1: TPath | list[TPath],
        path2: TPath | list[TPath],
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    def rm_file(self, path: TPath) -> None: ...
    def rm(
        self, path: TPath, recursive: bool = False, maxdepth: int | None = None
    ) -> None: ...
    @overload
    def open(
        self,
        path: TPath,
        mode: OpenTextMode,
        block_size: int | None = None,
        cache_options: dict[str, Any] | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> io.TextIOWrapper: ...
    @overload
    def open(
        self,
        path: TPath,
        mode: OpenBinaryMode,
        block_size: int | None = None,
        cache_options: dict[str, Any] | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> AbstractBufferedFile: ...
    def open(
        self,
        path: TPath,
        mode: OpenTextMode | OpenBinaryMode = "rb",
        block_size: int | None = None,
        cache_options: dict[str, Any] | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> io.TextIOWrapper | AbstractBufferedFile: ...
    def touch(self, path: TPath, truncate: bool = True, **kwargs: Any) -> None: ...
    def ukey(self, path: TPath) -> str: ...
    def read_block(
        self, fn: str, offset: int, length: int | None, delimiter: bytes | None = None
    ) -> bytes: ...
    def to_json(self, *, include_password: bool = True) -> str: ...
    @staticmethod
    def from_json(blob: str) -> AbstractFileSystem: ...
    def to_dict(self, *, include_password: bool = True) -> dict[str, Any]: ...
    @staticmethod
    def from_dict(dct: dict[str, Any]) -> AbstractFileSystem: ...
    def get_mapper(
        self,
        root: str = "",
        check: bool = False,
        create: bool = False,
        missing_exceptions: tuple[Exception] | None = None,
    ) -> FSMap: ...
    @classmethod
    def clear_instance_cache(cls) -> None: ...
    def created(self, path: TPath) -> None: ...
    def modified(self, path: TPath) -> None: ...
    def tree(
        self,
        path: str = "/",
        recursion_limit: int = 2,
        max_display: int = 25,
        display_size: bool = False,
        prefix: str = "",
        is_last: bool = True,
        first: bool = True,
        indent_size: int = 4,
    ) -> str: ...
    def read_bytes(
        self,
        path: TPath,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> str | bytes: ...
    def write_bytes(self, path: TPath, value: bytes, **kwargs: Any) -> None: ...
    def makedir(
        self, path: TPath, create_parents: bool = True, **kwargs: Any
    ) -> None: ...
    def mkdirs(self, path: TPath, exist_ok: bool = False) -> None: ...
    @overload
    def listdir(
        self, path: TPath, detail: Literal[True], **kwargs: Any
    ) -> list[FileInfo]: ...
    @overload
    def listdir(
        self, path: TPath, detail: Literal[False], **kwargs: Any
    ) -> list[str]: ...
    def listdir(
        self, path: TPath, detail: bool = True, **kwargs: Any
    ) -> list[str] | list[FileInfo]: ...
    def cp(self, path1: TPath, path2: TPath, **kwargs: Any) -> None: ...
    def move(self, path1: TPath, path2: TPath, **kwargs: Any) -> None: ...
    def stat(self, path: TPath, **kwargs: Any) -> FileInfo: ...
    @overload
    def disk_usage(
        self,
        path: TPath,
        total: Literal[True],
        maxdepth: int | None,
        withdirs: bool,
        **kwargs: Any,
    ) -> int: ...
    @overload
    def disk_usage(
        self,
        path: TPath,
        total: Literal[False],
        maxdepth: int | None,
        withdirs: bool,
        **kwargs: Any,
    ) -> dict[str, int]: ...
    def disk_usage(
        self,
        path: TPath,
        total: bool = True,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> int | dict[str, int]: ...
    def rename(self, path1: TPath, path2: TPath, **kwargs: Any) -> None: ...
    def delete(
        self, path: TPath, recursive: bool = False, maxdepth: int | None = None
    ) -> None: ...
    def upload(
        self, lpath: TPath, rpath: TPath, recursive: bool = False, **kwargs: Any
    ) -> None: ...
    def download(
        self, rpath: TPath, lpath: TPath, recursive: bool = False, **kwargs: Any
    ) -> None: ...
    def sign(self, path: TPath, expiration: int = 100, **kwargs: Any) -> None: ...

class AbstractBufferedFile(io.IOBase):
    DEFAULT_BLOCK_SIZE: Incomplete
    path: Incomplete
    fs: Incomplete
    mode: Incomplete
    blocksize: Incomplete
    loc: int
    autocommit: Incomplete
    end: Incomplete
    start: Incomplete
    kwargs: Incomplete
    size: Incomplete
    cache: Incomplete
    buffer: Incomplete
    offset: Incomplete
    forced: bool
    location: Incomplete
    def __init__(
        self,
        fs,
        path: TPath,
        mode: str = "rb",
        block_size: str = "default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ) -> None: ...
    @property
    def details(self): ...
    @details.setter
    def details(self, value) -> None: ...
    @property
    def full_name(self): ...
    @property
    def closed(self): ...
    @closed.setter
    def closed(self, c) -> None: ...
    def __hash__(self): ...
    def __eq__(self, other): ...
    def commit(self) -> None: ...
    def discard(self) -> None: ...
    def info(self): ...
    def tell(self): ...
    def seek(self, loc, whence: int = 0): ...
    def write(self, data): ...
    def flush(self, force: bool = False) -> None: ...
    def read(self, length: int = -1): ...
    def readinto(self, b): ...
    def readuntil(self, char: bytes = b"\n", blocks=None): ...
    def readline(self): ...
    def __next__(self): ...
    def __iter__(self): ...
    def readlines(self): ...
    def readinto1(self, b): ...
    def close(self) -> None: ...
    def readable(self): ...
    def seekable(self): ...
    def writable(self): ...
    def __reduce__(self): ...
    def __del__(self) -> None: ...
    def __enter__(self): ...
    def __exit__(self, *args) -> None: ...

def reopen(
    fs, path: TPath, mode, blocksize, loc, size, autocommit, cache_type, kwargs
): ...
