import base64
import io
import fsspec
import re
import requests


class JupyterFileSystem(fsspec.AbstractFileSystem):

    protocol = ("jupyter", "jlab")

    def __init__(self, url, tok=None, **kwargs):
        if "?" in url:
            if tok is None:
                tok = re.findall("token=([a-f0-9]+)")[0]
            url = url.split("?", 1)[0]
        self.url = url.rstrip("/") + '/api/contents'
        self.session = requests.Session()
        if tok:
            self.session.headers['Authorization'] = f'token {tok}'

        super().__init__(**kwargs)

    def ls(self, path, detail=True, **kwargs):
        r = self.session.get(self.url + '/' + path)
        if r.status_code == 404:
            return FileNotFoundError(path)
        r.raise_for_status()
        out = r.json()

        if out['type'] == 'directory':
            out = out['content']
        else:
            out = [out]
        for o in out:
            o['name'] = o.pop('path')
            o.pop('content')
            if o['type'] == 'notebook':
                o['type'] = 'file'
        if detail:
            return out
        return [o['name'] for o in out]

    def cat_file(self, path):
        r = self.session.get(self.url + '/' + path)
        if r.status_code == 404:
            return FileNotFoundError(path)
        r.raise_for_status()
        out = r.json()
        if out['format'] == 'text':
            # data should be binary
            return out['content'].encode()
        else:
            return base64.b64decode(out['content'])

    def pipe_file(self, path, value, **_):
        json = {'name': path.rsplit('/', 1)[-1],
                'path': path,
                'size': len(value),
                'content': base64.b64encode(value),
                'format': 'base64',
                'type': 'file'}
        self.session.put(self.url + '/' + path, json=json)

    def mkdir(self, path, create_parents=True, **kwargs):
        if create_parents and '/' in path:
            self.mkdir(path.rsplit('/', 1)[0], True)
        json = {'name': path.rsplit('/', 1)[-1],
                'path': path,
                'size': None,
                'content': None,
                'type': 'directory'}
        self.session.put(self.url + '/' + path, json=json)

    def _rm(self, path):
        self.session.delete(self.url + '/' + path)

    def _open(
        self,
        path,
        mode="rb",
        **kwargs
    ):
        if mode == 'rb':
            data = self.cat_file(path)
            return io.BytesIO(data)
        else:
            return SimpleFileWriter(self, path, mode='wb')


class SimpleFileWriter(fsspec.spec.AbstractBufferedFile):

    def _upload_chunk(self, final=False):
        """Never uploads a chunk until file is done

        Not suitable for large files
        """
        if final is False:
            return False
        self.buffer.seek(0)
        data = self.buffer.read()
        self.fs.pipe_file(self.path, data)
