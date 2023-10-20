from __future__ import annotations

import array
import posixpath
import warnings
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Literal,
    MutableMapping,
    TypeVar,
    overload,
)

from .core import url_to_fs

if TYPE_CHECKING:
    from .spec import AbstractFileSystem
    from .implementations.dirfs import DirFileSystem

    T = TypeVar("T")


class FSMap(MutableMapping[str, bytes]):
    """Wrap a FileSystem instance as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    root: string
        prefix for all the files
    fs: FileSystem instance
    check: bool (=True)
        performs a touch at the location, to check for write access.

    Examples
    --------
    >>> fs = FileSystem(**parameters)  # doctest: +SKIP
    >>> d = FSMap('my-data/path/', fs)  # doctest: +SKIP
    or, more likely
    >>> d = fs.get_mapper('my-data/path/')

    >>> d['loc1'] = b'Hello World'  # doctest: +SKIP
    >>> list(d.keys())  # doctest: +SKIP
    ['loc1']
    >>> d['loc1']  # doctest: +SKIP
    b'Hello World'
    """

    def __init__(
        self,
        root: str,
        fs: AbstractFileSystem,
        check: bool = False,
        create: bool = False,
        missing_exceptions: tuple[type[Exception], ...] | None = None,
    ):
        self.fs = fs
        self.root = fs._strip_protocol(root).rstrip("/")
        self._root_key_to_str = fs._strip_protocol(posixpath.join(root, "x"))[:-1]
        if missing_exceptions is None:
            missing_exceptions = (
                FileNotFoundError,
                IsADirectoryError,
                NotADirectoryError,
            )
        self.missing_exceptions = missing_exceptions
        self.check = check
        self.create = create
        if create:
            if not self.fs.exists(root):
                self.fs.mkdir(root)
        if check:
            if not self.fs.exists(root):
                raise ValueError(
                    f"Path {root} does not exist. Create "
                    f" with the ``create=True`` keyword"
                )
            self.fs.touch(root + "/a")
            self.fs.rm(root + "/a")

    @cached_property
    def dirfs(self) -> DirFileSystem:
        """dirfs instance that can be used with the same keys as the mapper"""
        from .implementations.dirfs import DirFileSystem

        return DirFileSystem(path=self._root_key_to_str, fs=self.fs)

    def clear(self) -> None:
        """Remove all keys below root - empties out mapping"""
        try:
            self.fs.rm(self.root, True)
            self.fs.mkdir(self.root)
        except:  # noqa: E722
            pass

    @overload
    def getitems(
        self, keys: Iterable[str], on_error: Literal["raise", "omit"] = ...
    ) -> dict[str, bytes]:
        pass

    @overload
    def getitems(
        self, keys: Iterable[str], on_error: Literal["return"]
    ) -> dict[str, bytes | Exception]:
        pass

    def getitems(
        self,
        keys: Iterable[str],
        on_error: Literal["raise", "omit", "return"] = "raise",
    ) -> dict[str, bytes | Exception] | dict[str, bytes]:
        """Fetch multiple items from the store

        If the backend is async-able, this might proceed concurrently

        Parameters
        ----------
        keys: iterable(str)
            They keys to be fetched
        on_error : "raise", "omit", "return"
            If raise, an underlying exception will be raised (converted to KeyError
            if the type is in self.missing_exceptions); if omit, keys with exception
            will simply not be included in the output; if "return", all keys are
            included in the output, but the value will be bytes or an exception
            instance.

        Returns
        -------
        dict(key, bytes|exception)
        """
        keys2 = [self._key_to_str(k) for k in keys]
        oe = on_error if on_error == "raise" else "return"
        try:
            out = self.fs.cat(keys2, on_error=oe)
            if isinstance(out, bytes):
                out = {keys2[0]: out}
        except self.missing_exceptions as e:
            raise KeyError from e
        out = {
            k: (KeyError() if isinstance(v, self.missing_exceptions) else v)
            for k, v in out.items()
        }
        return {
            key: out[k2]
            for key, k2 in zip(keys, keys2)
            if on_error == "return" or not isinstance(out[k2], BaseException)
        }

    def setitems(self, values_dict: dict[str, bytes]) -> None:
        """Set the values of multiple items in the store

        Parameters
        ----------
        values_dict: dict(str, bytes)
        """
        values = {self._key_to_str(k): maybe_convert(v) for k, v in values_dict.items()}
        self.fs.pipe(values)

    def delitems(self, keys: Iterable[str]) -> None:
        """Remove multiple keys from the store"""
        self.fs.rm([self._key_to_str(k) for k in keys])

    def _key_to_str(self, key: str) -> str:
        """Generate full path for the key"""
        if not isinstance(key, str):
            # raise TypeError("key must be of type `str`, got `{type(key).__name__}`"
            warnings.warn(
                "from fsspec 2023.5 onward FSMap non-str keys will raise TypeError",
                DeprecationWarning,
            )
            if isinstance(key, list):
                key = tuple(key)
            key = str(key)
        return f"{self._root_key_to_str}{key}"

    def _str_to_key(self, s: str) -> str:
        """Strip path of to leave key name"""
        return s[len(self.root) :].lstrip("/")

    def __getitem__(self, key: str, default: bytes | None = None) -> bytes:
        """Retrieve data"""
        k = self._key_to_str(key)
        try:
            result = self.fs.cat(k)
        except self.missing_exceptions:
            if default is not None:
                return default
            raise KeyError(key)
        return result

    def pop(self, key: str, default: bytes | None = None) -> bytes:  # type: ignore[override]
        """Pop data"""
        result = self.__getitem__(key, default)
        try:
            del self[key]
        except KeyError:
            pass
        return result

    def __setitem__(self, key: str, value: bytes) -> None:
        """Store value in key"""
        key = self._key_to_str(key)
        self.fs.mkdirs(self.fs._parent(key), exist_ok=True)
        self.fs.pipe_file(key, maybe_convert(value))

    def __iter__(self) -> Iterator[str]:
        return (self._str_to_key(x) for x in self.fs.find(self.root))

    def __len__(self) -> int:
        return len(self.fs.find(self.root))

    def __delitem__(self, key: str) -> None:
        """Remove key"""
        try:
            self.fs.rm(self._key_to_str(key))
        except:  # noqa: E722
            raise KeyError

    def __contains__(self, key: str) -> bool:  # type: ignore[override]
        """Does key exist in mapping?"""
        path = self._key_to_str(key)
        return self.fs.exists(path) and self.fs.isfile(path)

    def __reduce__(self):
        return FSMap, (self.root, self.fs, False, False, self.missing_exceptions)


def maybe_convert(value):
    if isinstance(value, array.array) or hasattr(value, "__array__"):
        # bytes-like things
        if hasattr(value, "dtype") and value.dtype.kind in "Mm":
            # The buffer interface doesn't support datetime64/timdelta64 numpy
            # arrays
            value = value.view("int64")
        value = bytes(memoryview(value))
    return value


def get_mapper(
    url: str = "",
    check: bool = False,
    create: bool = False,
    missing_exceptions: tuple[type[Exception], ...] | None = None,
    alternate_root: str | None = None,
    **kwargs,
) -> FSMap:
    """Create key-value interface for given URL and options

    The URL will be of the form "protocol://location" and point to the root
    of the mapper required. All keys will be file-names below this location,
    and their values the contents of each key.

    Also accepts compound URLs like zip::s3://bucket/file.zip , see ``fsspec.open``.

    Parameters
    ----------
    url: str
        Root URL of mapping
    check: bool
        Whether to attempt to read from the location before instantiation, to
        check that the mapping does exist
    create: bool
        Whether to make the directory corresponding to the root before
        instantiating
    missing_exceptions: None or tuple
        If given, these exception types will be regarded as missing keys and
        return KeyError when trying to read data. By default, you get
        (FileNotFoundError, IsADirectoryError, NotADirectoryError)
    alternate_root: None or str
        In cases of complex URLs, the parser may fail to pick the correct part
        for the mapper root, so this arg can override

    Returns
    -------
    ``FSMap`` instance, the dict-like key-value store.
    """
    # Removing protocol here - could defer to each open() on the backend
    fs, urlpath = url_to_fs(url, **kwargs)
    root = alternate_root if alternate_root is not None else urlpath
    return FSMap(root, fs, check, create, missing_exceptions=missing_exceptions)
