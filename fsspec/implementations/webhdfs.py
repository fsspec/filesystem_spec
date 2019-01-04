

# https://hadoop.apache.org/docs/r1.0.4/webhdfs.html

import requests
from urllib.parse import quote
import uuid
from ..spec import AbstractFileSystem, AbstractBufferedFile


class WebHDFS(AbstractFileSystem):
    tempdir = '/tmp'

    def __init__(self, host, port=50070, kerberos=False, token=None, user=None,
                 proxy_to=None, kerb_kwargs=None, data_proxy=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.url = f"http://{host}:{port}/webhdfs/v1"
        self.kerb = kerberos
        self.kerb_kwargs = kerb_kwargs or {}
        self.pars = {}
        self.proxy = data_proxy or {}
        if token is not None:
            if user is not None or proxy_to is not None:
                raise ValueError('If passing a delegation token, must not set '
                                 'user or proxy_to, as these are encoded in the'
                                 ' token')
            self.pars['delegation'] = token
        if user is not None:
            self.pars['user.name'] = user
        if proxy_to is not None:
            self.pars['doas'] = proxy_to
        if kerberos and user is not None:
            raise ValueError('If using Kerberos auth, do not specify the '
                             'user, this is handled by kinit.')
        self._connect()

    def _connect(self):
        if self.kerb:
            from requests_kerberos import HTTPKerberosAuth
            self.session = requests.Session(
                auth=HTTPKerberosAuth(**self.kerb_kwargs))
        else:
            self.session = requests.Session()

    def _call(self, op, method='get', path=None, data=None,
              redirect=True, **kwargs):
        url = self.url + quote(path or "")
        args = kwargs.copy()
        args['op'] = op.upper()
        out = self.session.request(method=method.upper(), url=url, params=args,
                                   data=data, allow_redirects=redirect)
        if out.status_code == 404:
            raise FileNotFoundError(path)
        out.raise_for_status()
        return out

    def _open(self, path, mode='rb', block_size=None, autocommit=True,
              **kwargs):
        block_size = block_size or self.blocksize
        return WebHDFile(self, path, mode=mode, block_size=block_size,
                         tempdir=self.tempdir, autocommit=autocommit,
                         proxy=self.proxy)

    @staticmethod
    def _process_info(info):
        info['type'] = info['type'].lower()
        info['size'] = info['length']
        return info

    def info(self, path):
        out = self._call('GETFILESTATUS', path=path)
        info = out.json()['FileStatus']
        info['name'] = path
        return self._process_info(info)

    def ls(self, path, detail=False):
        out = self._call('LISTSTATUS', path=path)
        infos = out.json()['FileStatuses']['FileStatus']
        for info in infos:
            self._process_info(info)
            info['name'] = path.rstrip('/') + '/' + info['pathSuffix']
        if detail:
            return sorted(infos, key=lambda i: i['name'])
        else:
            return sorted(info['name'] for info in infos)

    def mkdir(self, path, **kwargs):
        self._call('MKDIRS', method='put', path=path)

    def makedirs(self, path, exist_ok=False):
        self.mkdir(path)

    def mv(self, path1, path2, **kwargs):
        self._call('RENAME', method='put', path=path1, destination=path2)

    def rm(self, path, recursive=False, **kwargs):
        self._call('DELETE', method='delete', path=path,
                   recursive='true' if recursive else 'false')


class WebHDFile(AbstractBufferedFile):
    """A file living in HDFS over webHDFS"""

    def __init__(self, fs, path, **kwargs):
        """
        kwargs:
            proxy: dict
                A mapping from provided hostname to actual hostname
        """
        kwargs = kwargs.copy()
        self.proxy = kwargs.pop('proxy', {})
        tempdir = kwargs.pop('tempdir')
        if kwargs.pop('autocommit', False) is False:
            self.target = self.path
            self.path = '/'.join([tempdir, str(uuid.uuid4())])
        super().__init__(fs, path, **kwargs)

    def _upload_chunk(self, final=False):
        """ Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        out = self.fs.session.post(self.location, data=self.buffer.getvalue())
        out.raise_for_status()
        return True

    def _apply_proxy(self, location):
        if self.proxy:
            for k, v in self.proxy.items():
                location = location.replace(k, v)
        return location

    def _initiate_upload(self):
        """ Create remote file/upload """
        if 'a' in self.mode:
            op, method = 'APPEND', 'POST'
        else:
            op, method = 'CREATE', 'PUT'
            if self.fs.exists(self.path):
                # no "truncate" or "create empty"
                self.fs.rm(self.path)
        out = self.fs._call(op, method, self.path, redirect=False,
                            **self.kwargs)
        out.raise_for_status()
        location = self._apply_proxy(out.headers['Location'])
        if 'w' in self.mode:
            # create empty file to append to
            out2 = self.fs.session.put(location)
            out2.raise_for_status()
        self.location = location.replace('CREATE', 'APPEND')

    def _fetch_range(self, start, end):
        out = self.fs._call('OPEN', path=self.path, offset=start,
                            length=end-start, redirect=False)
        out.raise_for_status()
        location = out.headers['Location']
        out2 = self.fs.session.get(self._apply_proxy(location))
        return out2.content

    def commit(self):
        self.fs.mv(self.path, self.target)

    def discard(self):
        self.fs.rm(self.path)
