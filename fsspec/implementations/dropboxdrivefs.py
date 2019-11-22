import contextlib
import time

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

    def ls(self, path, detail=True, **kwargs):
        """ List objects at path
        """
        while '//' in path:
            path = path.replace('//', '/')
        list_file = []
        list_item = self.dbx.files_list_folder(path, recursive = True, include_media_info = True)
        items = list_item.entries
        while list_item.has_more: 
            list_item = self.dbx.files_list_folder_continue(list_item.cursor)
            items = list_item.entries + items

        if detail: 
            for item in list_item.entries: 
                if isinstance(item, dropbox.files.FileMetadata):
                    list_file.append({'name':item.path_display, 'size':item.size, 'type':'file'})
                elif isinstance(item, dropbox.files.FolderMetadata):
                    list_file.append({'name':item.path_display, 'size':None, 'type':'folder'})
                else:
                    list_file.append({'name':item.path_display, 'size':item.size, 'type':'unknow'})
        else: 
            for item in list_item.entries: 
                list_file.append(item.path_display)
        return list_file
        
    def _open(
            self,
            path,
            mode="rb",
            
            **kwargs
    ):
        return DropboxDriveFile(self, path, self.dbx, mode=mode, blocksize= 4 * 1024 * 1024, **kwargs)

    def info(self, url, **kwargs):
        """Get info of URL
        """
        metadata = self.dbx.files_get_metadata(url)
        if isinstance(metadata, dropbox.files.FileMetadata):
            return {'name':metadata.path_display, 'size':metadata.size, 'type':'file'}
        elif isinstance(metadata, dropbox.files.FolderMetadata):
            return{'name':metadata.path_display, 'size':None, 'type':'folder'}
        else:
            return {"name": url, "size": None, "type": "unknow"}

DEFAULT_BLOCK_SIZE = 5 * 2 ** 20


class DropboxDriveFile(AbstractBufferedFile):

    def _fetch_range(self, start, end):
        pass

    def __init__(
        self, fs, path, dbx,
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
        print(self.mode)
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
        while '//' in path:
            path = path.replace('//', '/')
        with stopwatch('download'):
            try:
                md, res = self.dbx.files_download('/'+path)
            except dropbox.exceptions.HttpError as err:
                print('*** HTTP error :', err)
                return None
        data = res.content
        return data
    
    def _upload_chunk(self, final = False):
        self.cursor.offset += self.buffer.seek(0, 2)
        print(self.cursor.offset)
        if final:
            self.dbx.files_upload_session_finish(self.buffer.getvalue(),self.cursor,self.commit)
        else:
            self.dbx.files_upload_session_append(self.buffer.getvalue(), self.cursor.session_id, self.cursor.offset)


    def _initiate_upload(self):
        """ Example given by dropbox API
        """
        session = self.dbx.files_upload_session_start(self.buffer.getvalue())
        self.commit = dropbox.files.CommitInfo(path=self.path)
        self.cursor = dropbox.files.UploadSessionCursor(session_id=session.session_id,offset=self.offset)

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
        print('Total elapsed time for %s: %.3f' % (message, t1 - t0))


class AllBytes(object):
    """Cache entire contents of the dropbox file"""

    def __init__(self, data):
        self.data = data

    def _fetch(self, start, end):
        return self.data[start:end]
