import pytest
import pickle
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import fsspec

requests = pytest.importorskip('requests')
port = 9898
data = b'\n'.join([b'some test data'] * 1000)
realfile = "http://localhost:%i/index/realfile" % port
index = b'<a href="%s">Link</a>' % realfile.encode()


class HTTPTestHandler(BaseHTTPRequestHandler):
    def _respond(self, code=200, headers=None, data=b''):
        headers = headers or {}
        headers.update({'User-Agent': 'test'})
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, str(v))
        self.end_headers()
        if data:
            self.wfile.write(data)

    def do_GET(self):
        if self.path not in ['/index/realfile', '/index']:
            self._respond(404)
            return

        d = data if self.path == '/index/realfile' else index
        if 'Range' in self.headers:
            ran = self.headers['Range']
            b, ran = ran.split("=")
            start, end = ran.split('-')
            print(start)
            print(end)
            d = d[int(start):int(end)+1]
        if 'give_length' in self.headers:
            self._respond(200, {'Content-Length': len(d)}, d)
        elif 'give_range' in self.headers:
            self._respond(
                200, {'Content-Range': "0-%i/%i" % (len(d) - 1, len(d))},
                d
            )
        else:
            self._respond(200, data=d)

    def do_HEAD(self):
        if 'head_ok' not in self.headers:
            self._respond(405)
            return
        d = data if self.path == '/index/realfile' else index
        if self.path not in ['/index/realfile', '/index']:
            self._respond(404)
        elif 'give_length' in self.headers:
            self._respond(200, {'Content-Length': len(d)})
        elif 'give_range' in self.headers:
            self._respond(200, {'Content-Range': "0-%i/%i" %
                                                 (len(d) - 1, len(d))})
        else:
            self._respond(200)  # OK response, but no useful info


@pytest.fixture(scope='module')
def server():
    server_address = ('', port)
    httpd = HTTPServer(server_address, HTTPTestHandler)
    th = threading.Thread(target=httpd.serve_forever)
    th.daemon = True
    th.start()
    try:
        yield 'http://localhost:%i' % port
    finally:
        httpd.socket.close()
        httpd.shutdown()
        th.join()


def test_list(server):
    h = fsspec.filesystem('http')
    out = h.glob(server + '/index/*')
    assert out == [server + '/index/realfile']


def test_policy_arg(server):
    h = fsspec.filesystem('http', size_policy='get')
    out = h.glob(server + '/index/*')
    assert out == [server + '/index/realfile']


def test_exists(server):
    h = fsspec.filesystem('http')
    assert not h.exists(server + '/notafile')


def test_read(server):
    h = fsspec.filesystem('http')
    out = server + '/index/realfile'
    with h.open(out, 'rb') as f:
        assert f.read() == data
    with h.open(out, 'rb', block_size=0) as f:
        assert f.read() == data
    with h.open(out, 'rb') as f:
        assert f.read(100) + f.read() == data


def test_methods(server):
    h = fsspec.filesystem('http')
    url = server + '/index/realfile'
    assert h.exists(url)
    assert h.cat(url) == data


@pytest.mark.parametrize('headers', [{},
                                     {'give_length': 'true'},
                                     {'give_length': 'true', 'head_ok': 'true'},
                                     {'give_range': 'true'}
                                     ])
def test_random_access(server, headers):
    h = fsspec.filesystem('http', headers=headers)
    url = server + '/index/realfile'
    with h.open(url, 'rb') as f:
        if headers:
            assert f.size == len(data)
        assert f.read(5) == data[:5]
        # python server does not respect bytes range request
        # we actually get all the data
        f.seek(5, 1)
        assert f.read(5) == data[10:15]


def test_local_session():
    h = fsspec.filesystem('http')
    import threading
    out = []

    def target():
        out.append(h.session)

    t = threading.Thread(target=target)
    t.start()
    t.join()

    assert out[0] != id(h.session)

    h2 = pickle.loads(pickle.dumps(h))
    assert h is h2
    assert h.session is h2.session
