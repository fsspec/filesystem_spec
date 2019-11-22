import requests
import dropbox
from ..spec import AbstractFileSystem, AbstractBufferedFile


class DropboxDriveFileSystem(AbstractFileSystem):
    """ Interface dropbox to connect, list and manage files
    Parameters:
    ----------
    token : str
          Generated key by adding a dropbox app in the user dropbox account. 
          Needs to be done by the user

    """

    def __init__(self, **storage_options):
        super().__init__(**storage_options)
        self.token = storage_options["token"]
        self.kwargs = storage_options
        self.connect()

    def connect(self):
        """ connect to the dropbox account with the given token
        """
        self.dbx = dropbox.Dropbox(self.token)
        self.session = requests.Session()
        self.session.auth = ("Authorization", self.token)

    def ls(self, path, detail=True, **kwargs):
        """ List objects at path
        """
        while "//" in path:
            path = path.replace("//", "/")
        list_file = []
        list_item = self.dbx.files_list_folder(
            path, recursive=True, include_media_info=True
        )
        items = list_item.entries
        while list_item.has_more:
            list_item = self.dbx.files_list_folder_continue(list_item.cursor)
            items = list_item.entries + items

        if detail:
            for item in list_item.entries:
                if isinstance(item, dropbox.files.FileMetadata):
                    list_file.append(
                        {"name": item.path_display, "size": item.size, "type": "file"}
                    )
                elif isinstance(item, dropbox.files.FolderMetadata):
                    list_file.append(
                        {"name": item.path_display, "size": None, "type": "folder"}
                    )
                else:
                    list_file.append(
                        {"name": item.path_display, "size": item.size, "type": "unknow"}
                    )
        else:
            for item in list_item.entries:
                list_file.append(item.path_display)
        return list_file

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=None,
        cache_options=None,
        **kwargs
    ):
        return DropboxDriveFile(
            self,
            self.dbx,
            path,
            session=self.session,
            block_size=4 * 1024 * 1024,
            mode=mode,
            **kwargs
        )

    def info(self, url, **kwargs):
        """Get info of URL
        """
        metadata = self.dbx.files_get_metadata(url)
        if isinstance(metadata, dropbox.files.FileMetadata):
            return {
                "name": metadata.path_display,
                "size": metadata.size,
                "type": "file",
            }
        elif isinstance(metadata, dropbox.files.FolderMetadata):
            return {"name": metadata.path_display, "size": None, "type": "folder"}
        else:
            return {"name": url, "size": None, "type": "unknow"}


class DropboxDriveFile(AbstractBufferedFile):
    """ fetch_all, fetch_range, and read method are based from the http implementation
    """

    def __init__(
        self, fs, dbx, path, session=None, block_size=None, mode="rb", **kwargs
    ):
        """
        Open a file.
        Parameters
        ----------
        fs: instance of DropboxDriveFileSystem
        dbx : instance of dropbox
        session: requests.Session or None
                All calls will be made within this session, to avoid restarting connections
                where the server allows this
        path : str
            file path to inspect in dropbox
        mode: str
            Normal file modes.'rb' or 'wb'
        block_size: int or None
            The amount of read-ahead to do, in bytes. Default is 5MB, or the value
            configured for the FileSystem creating this file
        """
        for key, value in kwargs.items():
            print("{0} = {1}".format(key, value))
        self.session = session if session is not None else requests.Session()
        super().__init__(fs=fs, path=path, mode=mode, block_size=block_size, **kwargs)

        self.path = path
        self.dbx = dbx
        while "//" in path:
            path = path.replace("//", "/")
        self.url = self.dbx.files_get_temporary_link(path).link

    def read(self, length=-1):
        """Read bytes from file

        Parameters
        ----------
        length: int
            Read up to this many bytes. If negative, read all content to end of
            file. If the server has not supplied the filesize, attempting to
            read only part of the data will raise a ValueError.
        """
        if (
            (length < 0 and self.loc == 0)
            or (length > (self.size or length))  # explicit read all
            or (  # read more than there is
                self.size and self.size < self.blocksize
            )  # all fits in one block anyway
        ):
            self._fetch_all()
        if self.size is None:
            if length < 0:
                self._fetch_all()
        else:
            length = min(self.size - self.loc, length)
        return super().read(length)

    def _fetch_all(self):
        """Read whole file in one shot, without caching

        This is only called when position is still at zero,
        and read() is called without a byte-count.
        """
        if not isinstance(self.cache, AllBytes):
            r = self.session.get(self.url, **self.kwargs)
            r.raise_for_status()
            out = r.content
            self.cache = AllBytes(out)
            self.size = len(out)

    def _fetch_range(self, start, end):
        """Download a block of data

        The expectation is that the server returns only the requested bytes,
        with HTTP code 206. If this is not the case, we first check the headers,
        and then stream the output - if the data size is bigger than we
        requested, an exception is raised.
        """
        kwargs = self.kwargs.copy()
        headers = kwargs.pop("headers", {})
        headers["Range"] = "bytes=%i-%i" % (start, end - 1)
        r = self.session.get(self.url, headers=headers, stream=True, **kwargs)
        if r.status_code == 416:
            # range request outside file
            return b""
        r.raise_for_status()
        if r.status_code == 206:
            # partial content, as expected
            out = r.content
        elif "Content-Length" in r.headers:
            cl = int(r.headers["Content-Length"])
            if cl <= end - start:
                # data size OK
                out = r.content
            else:
                raise ValueError(
                    "Got more bytes (%i) than requested (%i)" % (cl, end - start)
                )
        else:
            cl = 0
            out = []
            for chunk in r.iter_content(chunk_size=2 ** 20):
                # data size unknown, let's see if it goes too big
                if chunk:
                    out.append(chunk)
                    cl += len(chunk)
                    if cl > end - start:
                        raise ValueError(
                            "Got more bytes so far (>%i) than requested (%i)"
                            % (cl, end - start)
                        )
                else:
                    break
            out = b"".join(out)
        return out

    def _upload_chunk(self, final=False):
        self.cursor.offset += self.buffer.seek(0, 2)
        if final:
            self.dbx.files_upload_session_finish(
                self.buffer.getvalue(), self.cursor, self.commit
            )
        else:
            self.dbx.files_upload_session_append(
                self.buffer.getvalue(), self.cursor.session_id, self.cursor.offset
            )

    def _initiate_upload(self):
        """ Initiate the upload session
        """
        session = self.dbx.files_upload_session_start(self.buffer.getvalue())
        self.commit = dropbox.files.CommitInfo(path=self.path)
        self.cursor = dropbox.files.UploadSessionCursor(
            session_id=session.session_id, offset=self.offset
        )


class AllBytes(object):
    """Cache entire contents of the dropbox file"""

    def __init__(self, data):
        self.data = data

    def _fetch(self, start, end):
        return self.data[start:end]
