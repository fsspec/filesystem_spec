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
            Contains ZIP, and must exist. If a str, will fetch file using
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
        if isinstance(fo, str):
            if mode == "a":
                m = "r+b"
            else:
                m = mode + "b"
            fo = fsspec.open(
                fo, mode=m, protocol=target_protocol, **(target_options or {})
            )
        self.force_zip_64 = allowZip64
        self.of = fo
        self.fo = fo.__enter__()  # the whole instance is a context
        self.zip = zipfile.ZipFile(
            self.fo,
            mode=mode,
            compression=compression,
            allowZip64=allowZip64,
            compresslevel=compresslevel,
        )
        self.dir_cache = None

    @classmethod
    def _strip_protocol(cls, path):
        # zip file paths are always relative to the archive root
        return super()._strip_protocol(path).lstrip("/")

    def __del__(self):
        if hasattr(self, "zip"):
            self.close()
            del self.zip

    def close(self):
        """Commits any write changes to the file. Done on ``del`` too."""
        self.zip.close()

    def _get_dirs(self):
        if self.dir_cache is None or self.mode in set("wa"):
            # when writing, dir_cache is always in the ZipFile's attributes,
            # not read from the file.
            files = self.zip.infolist()
            self.dir_cache = {
                dirname.rstrip("/"): {
                    "name": dirname.rstrip("/"),
                    "size": 0,
                    "type": "directory",
                }
                for dirname in self._all_dirnames(self.zip.namelist())
            }
            for z in files:
                f = {s: getattr(z, s, None) for s in zipfile.ZipInfo.__slots__}
                f.update(
                    {
                        "name": z.filename.rstrip("/"),
                        "size": z.file_size,
                        "type": ("directory" if z.is_dir() else "file"),
                    }
                )
                self.dir_cache[f["name"]] = f

    def pipe_file(self, path, value, **kwargs):
        # override upstream, because we know the exact file size in this case
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
        out = self.zip.open(path, mode.strip("b"), force_zip64=self.force_zip_64)
        if "r" in mode:
            info = self.info(path)
            out.size = info["size"]
            out.name = info["name"]
        return out

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")

        result = {}

        def _below_max_recursion_depth(path):
            if not maxdepth:
                return True

            depth = len(path.split("/"))
            return depth <= maxdepth

        for zip_info in self.zip.infolist():
            file_name = zip_info.filename
            if not file_name.startswith(path.lstrip("/")):
                continue

            # zip files can contain explicit or implicit directories
            # hence the need to either add them directly or infer them
            # from the file paths
            if zip_info.is_dir():
                if withdirs:
                    if not file_name in result and _below_max_recursion_depth(
                        file_name
                    ):
                        result[file_name.strip("/")] = (
                            self.info(file_name) if detail else None
                        )
                continue

            if file_name not in result:
                if _below_max_recursion_depth(file_name):
                    result[file_name] = self.info(file_name) if detail else None

                # Here we handle the case of implicitly adding the
                # directories if they have been requested
                if withdirs:
                    directories = file_name.split("/")
                    for i in range(1, len(directories)):
                        dir_path = "/".join(directories[:i]).strip(
                            "/"
                        )  # remove the trailing slash, as this is not expected
                        if not result.get(dir_path) and _below_max_recursion_depth(
                            dir_path
                        ):
                            result[dir_path] = {
                                "name": dir_path,
                                "size": 0,
                                "type": "directory",
                            }

        return result if detail else sorted(result)
