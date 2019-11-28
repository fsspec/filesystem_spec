import requests
import dropbox
from ..spec import AbstractFileSystem, AbstractBufferedFile
from .http import HTTPFile


class DropboxDriveFileSystem(AbstractFileSystem):
    """ Interface dropbox to connect, list and manage files
    Parameters:
    ----------
    token : str
          Generated key by adding a dropbox app in the user dropbox account.
          Needs to be done by the user

    """

    def __init__(self, token, *args, **storage_options):
        super().__init__(token=token, *args, **storage_options)
        self.token = token
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
                        {"name": item.path_display, "size": None, "type": "directory"}
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
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        return DropboxDriveFile(
            self,
            path,
            mode=mode,
            block_size=4 * 1024 * 1024,
            autocommit=autocommit,
            cache_options=cache_options,
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
            return {"name": metadata.path_display, "size": None, "type": "directory"}
        else:
            return {"name": url, "size": None, "type": "unknow"}


class DropboxDriveFile(AbstractBufferedFile):
    """ fetch_all, fetch_range, and read method are based from the http implementation
    """

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs
    ):
        """
        Open a file.
        Parameters
        ----------
        fs: instance of DropboxDriveFileSystem
        path : str
            file path to inspect in dropbox
        mode: str
            Normal file modes.'rb' or 'wb'
        block_size: int or None
            The amount of read-ahead to do, in bytes. Default is 5MB, or the value
            configured for the FileSystem creating this file
        """
        super().__init__(fs=fs, path=path, mode=mode, block_size=block_size, **kwargs)

        self.path = path
        self.dbx = self.fs.dbx
        path = path.replace("//", "/")
        if mode == "rb":
            self.url = self.dbx.files_get_temporary_link(path).link
            self.session = fs.session if fs.session is not None else requests.Session()
            self.httpfile = HTTPFile(
                fs,
                self.url,
                self.session,
                mode=mode,
                cache_options=cache_options,
                size=fs.info(path)["size"],
            )

    def read(self, length=-1):
        """Read bytes from file via the http
        """
        return self.httpfile.read(length=length)

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
