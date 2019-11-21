import contextlib
import time

import dropbox
from ..spec import AbstractFileSystem, AbstractBufferedFile


class DropboxDriveFileSystem(AbstractFileSystem):
    def __init__(self, **storage_options):
        super().__init__(**storage_options)
        self.token = storage_options["token"]
        self.kwargs = storage_options
        self.connect()

    def connect(self):
        self.dbx = dropbox.Dropbox(self.token)

    def _open(self, path, mode="rb", **kwargs):
        return DropboxDriveFile(self, path, self.dbx, mode="rb", **kwargs)

    def info(self, url, **kwargs):
        """Get info of URL
        Tries to access location via HEAD, and then GET methods, but does
        not fetch the data.
        It is possible that the server does not supply any size information, in
        which case size will be given as None (and certain operations on the
        corresponding file will not work).
        """
        size = None
        return {"name": url, "size": size or None, "type": "file"}


DEFAULT_BLOCK_SIZE = 5 * 2 ** 20


class DropboxDriveFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        pass

    def __init__(
        self,
        fs,
        path,
        dbx,
        block_size=None,
        mode="rb",
        cache_type="bytes",
        cache_options=None,
        size=None,
        **kwargs
    ):
        """
        Open a file.
        Parameters
        ----------
        fs: instance of GoogleDriveFileSystem
        mode: str
            Normal file modes. Currently only 'rb'.
        block_size: int
            Buffer size for reading or writing (default 5MB)
        """
        if mode != "rb":
            raise NotImplementedError("File mode not supported")
        if size is not None:
            self.details = {"name": path, "size": size, "type": "file"}

        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs
        )
        self.path = path
        self.dbx = dbx

    def _fetch_all(self):
        if not isinstance(self.cache, AllBytes):
            r = self._download(self.path)
            self.cache = AllBytes(r)
            self.size = len(r)

    def _download(self, path):
        """Download a file.
        Return the bytes of the file, or None if it doesn't exist.
        """
        while "//" in path:
            path = path.replace("//", "/")
        with stopwatch("download"):
            try:
                md, res = self.dbx.files_download("/" + path)
            except dropbox.exceptions.HttpError as err:
                print("*** HTTP error :", err)
                return None
        data = res.content
        return data

    def read(self, length=-1):
        """Read bytes from file
        Parameters
        ----------
        length: int
            Read up to this many bytes. If negative, read all content to end of
            file. If the server has not supplied the filesize, attempting to
            read only part of the data will raise a ValueError.
        """
        self._fetch_all()
        return super().read(length)


@contextlib.contextmanager
def stopwatch(message):
    """Context manager to print how long a block of code took."""
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        print("Total elapsed time for %s: %.3f" % (message, t1 - t0))


class AllBytes(object):
    """Cache entire contents of the dropbox file"""

    def __init__(self, data):
        self.data = data

    def _fetch(self, start, end):
        return self.data[start:end]
