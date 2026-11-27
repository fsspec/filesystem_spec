import os
import zipfile

import fsspec
from fsspec.archive import AbstractArchiveFileSystem


class ZipFileSystem(AbstractArchiveFileSystem):
    """Read/Write contents of ZIP archive as a file-system

    Keeps file object open while instance lives.

    This class is pickleable, but not necessarily thread-safe
    """

    root_marker = ""
    protocol = "zip"
    cachable = False

    def __init__(
        self,
        fo="",
        mode="r",
        target_protocol=None,
        target_options=None,
        compression=zipfile.ZIP_STORED,
        allowZip64=True,
        compresslevel=None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        fo: str or file-like
            Contains ZIP. In append mode, a missing archive is created.
            If a str, will fetch file using
            :meth:`~fsspec.open_files`, which must return one file exactly.
        mode: str
            Accept: "r", "w", "a"
        target_protocol: str (optional)
            If ``fo`` is a string, this value can be used to override the
            FS protocol inferred from a URL
        target_options: dict (optional)
            Kwargs passed when instantiating the target FS, if ``fo`` is
            a string.
        compression, allowZip64, compresslevel: passed to ZipFile
            Only relevant when creating a ZIP
        """
        super().__init__(self, **kwargs)
        if mode not in set("rwa"):
            raise ValueError(f"mode '{mode}' no understood")
        self.mode = mode
        if isinstance(fo, (str, os.PathLike)):
            if mode == "a":
                m = "r+b"
            else:
                m = mode + "b"
            fo = fsspec.open(
                fo, mode=m, protocol=target_protocol, **(target_options or {})
            )
        self.force_zip_64 = allowZip64
        self.of = fo
        try:
            self.fo = fo.__enter__()  # the whole instance is a context
        except FileNotFoundError:
            if mode != "a" or not isinstance(fo, fsspec.core.OpenFile):
                raise
            fo.mode = "w+b"
            self.fo = fo.__enter__()
        self.zip = zipfile.ZipFile(
            self.fo,
            mode=mode,
            compression=compression,
            allowZip64=allowZip64,
            compresslevel=compresslevel,
        )
        self.dir_cache = None
        self._members = None

    @classmethod
    def _strip_protocol(cls, path):
        # zip file paths are always relative to the archive root
        return cls._normalize_path(super()._strip_protocol(path))

    @staticmethod
    def _normalize_path(path):
        # Do not resolve '..': it can be part of a literal archive member name.
        return "/".join(part for part in path.split("/") if part not in ("", "."))

    def __del__(self):
        if hasattr(self, "zip"):
            self.close()
            del self.zip
        if hasattr(self, "of") and hasattr(self.of, "__exit__"):
            self.of.__exit__(None, None, None)

    def close(self):
        """Commits any write changes to the file. Done on ``del`` too."""
        self.zip.close()
        if isinstance(self.of, fsspec.core.OpenFile):
            self.of.close()

    def _get_members(self):
        if self._members is None or self.mode != "r":
            # Members such as "./dir/file" or "/file" are listed and opened by
            # their normalized name; an explicit "./" entry is the root itself.
            members = {}
            for z in self.zip.infolist():
                name = self._normalize_path(z.filename)
                if name:
                    members[name] = z
            self._members = members
        return self._members

    def _get_dirs(self):
        if self.dir_cache is None or self.mode in set("wa"):
            members = self._get_members()
            self.dir_cache = {
                dirname: {"name": dirname, "size": 0, "type": "directory"}
                for dirname in self._all_dirnames(members)
            }
            for name, z in members.items():
                f = {s: getattr(z, s, None) for s in zipfile.ZipInfo.__slots__}
                f.update(
                    {
                        "name": name,
                        "size": z.file_size,
                        "type": ("directory" if z.is_dir() else "file"),
                    }
                )
                self.dir_cache[name] = f

    def pipe_file(self, path, value, mode="overwrite", **kwargs):
        # override upstream, because we know the exact file size in this case
        path = self._strip_protocol(path)
        if mode not in {"create", "overwrite"}:
            raise ValueError(f"Unsupported ZIP write mode: {mode!r}")
        if mode == "create":
            path = self._strip_protocol(zipfile.ZipInfo(path).filename)
            if self.exists(path):
                raise FileExistsError(path)
        self.zip.writestr(path, value, **kwargs)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        path = self._strip_protocol(path)
        if "r" in mode and self.mode in set("wa"):
            if self.exists(path):
                raise OSError("ZipFS can only be open for reading or writing, not both")
            raise FileNotFoundError(path)
        if "r" in self.mode and "w" in mode:
            raise OSError("ZipFS can only be open for reading or writing, not both")
        member = path
        direct_read = False
        if "r" in mode:
            # A known read-only member needs no directory catalog. Keep
            # the normal info lookup for subclasses and protocol aliases.
            direct_read = (
                type(self) is ZipFileSystem
                and self.mode == "r"
                and self.dir_cache is None
                and not path.startswith(self.protocol + "::")
            )
            if direct_read:
                members = self._get_members()
            else:
                self._get_dirs()
                members = self._members
            member = members.get(path, path)
            if isinstance(member, zipfile.ZipInfo) and member.is_dir():
                raise IsADirectoryError(path)
        try:
            out = self.zip.open(member, mode.strip("b"), force_zip64=self.force_zip_64)
        except KeyError as exc:
            # zipfile reports a missing member as KeyError; fsspec callers,
            # including FSMap's missing_exceptions, expect FileNotFoundError.
            raise self._not_a_file(path) from exc
        if "r" in mode:
            if direct_read and isinstance(member, zipfile.ZipInfo):
                out.size = member.file_size
                out.name = path
            else:
                info = self.info(path)
                out.size = info["size"]
                out.name = info["name"]
        return out

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        def find_path(_path: str):
            # Preserve the separator aliases used by find without changing
            # the original cache keys returned to callers.
            return "/".join(filter(None, _path.replace("\\", "/").split("/")))

        if not isinstance(path, str):
            path = str(path)

        path = self._strip_protocol(path)
        normalized = find_path(path)
        prefix = normalized + "/"

        self._get_dirs()

        result = {}
        # To match posix find, if an exact file name is given, we should
        # return only that file
        if path in self.dir_cache and self.dir_cache[path]["type"] == "file":
            result[path] = self.dir_cache[path]
            return result if detail else [path]

        for file_path, file_info in self.dir_cache.items():
            if normalized:
                # Ordinary cache keys are already normalized by _get_dirs().
                candidate = find_path(file_path) if "\\" in file_path else file_path
                if candidate != normalized and not candidate.startswith(prefix):
                    continue

            if file_info["type"] == "directory":
                if withdirs and file_path not in result:
                    result[file_path.strip("/")] = file_info
                continue

            if file_path not in result:
                result[file_path] = file_info if detail else None

        if maxdepth:
            path_depth = normalized.count("/") + bool(normalized)
            result = {
                k: v for k, v in result.items() if k.count("/") < maxdepth + path_depth
            }
        return result if detail else sorted(result)
