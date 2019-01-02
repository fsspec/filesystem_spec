

# https://hadoop.apache.org/docs/r1.0.4/webhdfs.html

import requests
from ..spec import AbstractFileSystem


class WebHDFS(AbstractFileSystem):

    def __init__(self, host, port, kerberos=False, token=None, user=None,
                 proxy_to=None, kerb_kwargs=None):
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}/webhdfs/v1/"
        self.kerb = kerberos
        self.pars = {}
        if token is not None:
            if user is not None or proxy_to is not None:
                raise ValueError('If passing a delegation token, must not set'
                                 'user or proxy_to, as these are encoded in the'
                                 'token')
            self.pars['delegation'] = token
        if user is not None:
            self.pars['user.name'] = user
        if proxy_to is not None:
            self.pars['doas'] = proxy_to
        if kerberos:
            from requests_kerberos import HTTPKerberosAuth
            self.session = requests.Session(
                auth=HTTPKerberosAuth(**(kerb_kwargs or {})))
        else:
            self.session = requests.Session()
