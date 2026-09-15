"""ia://<identifier>/<filename>: the Internet Archive filesystem.

Offline: the URI mapping, credential loading from ``ia.ini`` and the environment, reads
through the local HTTP test server, and the ``Authorization`` header's scoping, including a
stand-in archive.org whose download URL redirects to a data node on another origin. One
opt-in network test (``IA_NETWORK_TESTS=1``; not ``FSSPEC_IA_*``, which fsspec.config would
turn into a constructor argument) reads the first bytes of a public item.
"""

import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import fsspec
from fsspec.implementations.ia import (
    InternetArchiveFileSystem,
    ia_config_path,
    in_domain,
    load_ia_credentials,
)
from fsspec.tests.conftest import data, server  # noqa: F401

pytest.importorskip("aiohttp")

# What ``ia configure`` writes: the [cookies] section is ignored (and must not trip the
# parser with its '%').
INI = """[s3]
access = AKIA-TEST
secret = s3cr3t
[cookies]
logged-in-user = someone%40example.org; expires=Sat, 28-Aug-2027 19:39:52 GMT; path=/; domain=.archive.org
logged-in-sig = 1756000000-abcdef0123456789; expires=Sat, 28-Aug-2027 19:39:52 GMT; path=/; domain=.archive.org
[general]
screenname = Some One
"""

PUBLIC_ITEM = (
    "ia://EOT24PRE-20240926175758-crawl808/EOT24PRE-20240926175758-00032.warc.gz"
)


@pytest.fixture(autouse=True)
def isolated_credentials(tmp_path, monkeypatch):
    """Never read the developer's real ia.ini; never reuse a cached instance, which
    would carry the previous test's credentials."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    for name in (
        "IA_CONFIG_FILE",
        "XDG_CONFIG_HOME",
        "IA_ACCESS_KEY_ID",
        "IA_SECRET_ACCESS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    InternetArchiveFileSystem.clear_instance_cache()
    yield
    InternetArchiveFileSystem.clear_instance_cache()


@pytest.fixture
def ini(tmp_path):
    path = tmp_path / "ia.ini"
    path.write_text(INI, encoding="utf-8")
    return str(path)


class _CrossOriginHandler(BaseHTTPRequestHandler):
    """``/download/<path>`` redirects to ``/items/<path>`` on the data node, a second
    server on another port and so another origin, which is where aiohttp drops the
    ``Authorization`` header. The item route serves ``files`` by Range and, when
    ``required`` is set, only to a request carrying that ``Authorization`` value."""

    files = {}
    required = None
    hits = []
    data_node = None  # "http://host:port", set by the fixture

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.hits.append((self.path, dict(self.headers)))
        if self.path.startswith("/download/"):
            self.send_response(302)
            self.send_header("Location", f"{self.data_node}/items/{self.path[10:]}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = self.files.get(self.path)
        if body is None:
            self.send_error(404)
            return
        if self.required and self.headers.get("Authorization") != self.required:
            self.send_error(403)
            return
        status, start, end = 200, 0, len(body) - 1
        if "Range" in self.headers:
            first, _, last = self.headers["Range"][len("bytes=") :].partition("-")
            start, end = int(first), (min(int(last), end) if last else end)
            status = 206
        chunk = body[start : end + 1]
        self.send_response(status)
        self.send_header("Content-Length", str(len(chunk)))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(chunk)

    do_HEAD = do_GET


@pytest.fixture
def ia_server(monkeypatch):
    """A stand-in archive.org on ``localhost``: the download server redirects to a data
    node on a second port. Both are in the (patched) auth domain ``localhost``; set
    ``handler.data_node`` to a ``127.0.0.1`` URL to move the node out of it."""
    handler = type(
        "Handler", (_CrossOriginHandler,), {"files": {}, "required": None, "hits": []}
    )
    servers = [HTTPServer(("127.0.0.1", 0), handler) for _ in range(2)]
    for httpd in servers:
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
    front, node = servers
    handler.data_node = f"http://localhost:{node.server_port}"
    handler.node_port = node.server_port
    monkeypatch.setattr(
        InternetArchiveFileSystem,
        "download_url",
        f"http://localhost:{front.server_port}/download/",
    )
    monkeypatch.setattr(InternetArchiveFileSystem, "auth_domain", "localhost")
    try:
        yield handler
    finally:
        for httpd in servers:
            httpd.shutdown()
            httpd.server_close()


def test_uri_maps_to_download_url_and_back():
    url = "https://archive.org/download/some-item/some-file.warc.gz"
    assert (
        InternetArchiveFileSystem._strip_protocol("ia://some-item/some-file.warc.gz")
        == url
    )
    assert (
        InternetArchiveFileSystem._strip_protocol("some-item/some-file.warc.gz") == url
    )
    assert InternetArchiveFileSystem._strip_protocol(url) == url  # idempotent
    fs = InternetArchiveFileSystem()
    assert fs.unstrip_protocol(url) == "ia://some-item/some-file.warc.gz"
    assert fs.unstrip_protocol("some-item/x") == "ia://some-item/x"


def test_registered():
    fs, path = fsspec.core.url_to_fs("ia://some-item/some-file")
    assert isinstance(fs, InternetArchiveFileSystem)
    assert path == "https://archive.org/download/some-item/some-file"
    assert fs.fsid == "ia"


def test_no_config_means_anonymous():
    assert ia_config_path() is None
    credentials = load_ia_credentials()
    assert credentials.anonymous and credentials.config_file is None
    assert credentials.authorization is None
    fs = InternetArchiveFileSystem()
    assert fs.access_key is None and fs.authorization is None
    assert "request_class" not in fs.client_kwargs


def test_ini_is_parsed(ini, monkeypatch):
    monkeypatch.setenv("IA_CONFIG_FILE", ini)
    assert ia_config_path() == ini
    credentials = load_ia_credentials()
    assert (credentials.access_key, credentials.secret_key) == ("AKIA-TEST", "s3cr3t")
    assert credentials.config_file == ini
    assert not hasattr(credentials, "cookies")
    fs = InternetArchiveFileSystem()
    assert fs.authorization == "LOW AKIA-TEST:s3cr3t"
    assert "Authorization" not in fs.kwargs.get("headers", {})  # scoped, not global


def test_ini_lookup_order(tmp_path, monkeypatch):
    home = tmp_path / "home"
    xdg_default = home / ".config" / "internetarchive" / "ia.ini"
    dot_ia = home / ".ia"
    for path in (xdg_default, dot_ia):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(INI)
    assert ia_config_path() == str(xdg_default)
    xdg_default.unlink()
    assert ia_config_path() == str(dot_ia)
    xdg = tmp_path / "xdg" / "internetarchive"
    xdg.mkdir(parents=True)
    (xdg / "ia.ini").write_text(INI)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    assert ia_config_path() == str(xdg / "ia.ini")
    monkeypatch.setenv("IA_CONFIG_FILE", str(dot_ia))
    assert ia_config_path() == str(dot_ia)


def test_environment_keys_override_the_file_and_come_in_pairs(ini, monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY_ID", "ENV-KEY")
    monkeypatch.setenv("IA_SECRET_ACCESS_KEY", "ENV-SECRET")
    credentials = load_ia_credentials(ini)
    assert (credentials.access_key, credentials.secret_key) == ("ENV-KEY", "ENV-SECRET")
    monkeypatch.delenv("IA_SECRET_ACCESS_KEY")
    with pytest.raises(ValueError, match="must be set together"):
        load_ia_credentials(ini)


def test_explicit_arguments_win_over_the_file(ini, monkeypatch):
    monkeypatch.setenv("IA_CONFIG_FILE", ini)
    fs = InternetArchiveFileSystem(access_key="explicit", secret_key="s")
    assert fs.authorization == "LOW explicit:s"
    anonymous = InternetArchiveFileSystem(access_key="", secret_key="")
    assert anonymous.authorization is None
    with pytest.raises(ValueError, match="must be given together"):
        InternetArchiveFileSystem(access_key="only-one")


def test_authorization_is_scoped_to_archive_org():
    """The header goes to archive.org and its data nodes, and nowhere else."""
    for host in (
        "archive.org",
        "dn721904.ca.archive.org",
        "ARCHIVE.ORG",
        "s3.us.archive.org.",
    ):
        assert in_domain(host, ".archive.org")
    for host in ("archive.example.com", "notarchive.org", "example.org", "", None):
        assert not in_domain(host, ".archive.org")


def test_reads_go_through_the_download_url(server, monkeypatch):
    monkeypatch.setattr(InternetArchiveFileSystem, "download_url", server.address + "/")
    fs = InternetArchiveFileSystem(headers={"give_length": "true", "use_206": "true"})
    assert fs.cat("ia://index/realfile") == data
    with fs.open("ia://index/realfile", "rb") as f:
        assert f.read() == data
    assert fs.cat_file("ia://index/realfile", start=1, end=10) == data[1:10]
    assert fs.info("ia://index/realfile")["size"] == len(data)


def test_refusal_is_a_permission_error(server, monkeypatch):
    monkeypatch.setattr(InternetArchiveFileSystem, "download_url", server.address + "/")
    fs = InternetArchiveFileSystem()
    with pytest.raises(PermissionError):
        fs.cat("ia://unauthorized")
    with pytest.raises(PermissionError):
        fs.open("ia://unauthorized", "rb")
    with pytest.raises(FileNotFoundError):
        fs.cat("ia://index/missing")


def test_authorization_survives_the_cross_origin_redirect(ia_server):
    """The keys must reach the data node, which sits behind a cross-origin redirect where
    aiohttp has already dropped the Authorization header; the request class re-adds it."""
    ia_server.files["/items/restricted/file"] = data
    ia_server.required = "LOW k:s"

    anonymous = InternetArchiveFileSystem()
    with pytest.raises(PermissionError):
        anonymous.cat("ia://restricted/file")
    with pytest.raises(PermissionError):
        anonymous.open("ia://restricted/file", "rb")

    fs = InternetArchiveFileSystem(access_key="k", secret_key="s")
    ia_server.hits.clear()
    assert fs.cat("ia://restricted/file") == data
    assert fs.cat_file("ia://restricted/file", start=2, end=5) == data[2:5]
    with fs.open("ia://restricted/file", "rb", block_size=4) as f:
        assert f.read() == data
    hops = {p.split("/")[1] for p, _ in ia_server.hits}
    assert hops == {"download", "items"}
    assert all(h.get("Authorization") == "LOW k:s" for _, h in ia_server.hits)
    assert not any("Cookie" in h for _, h in ia_server.hits)


def test_authorization_does_not_leave_the_domain(ia_server):
    """A redirect to a host outside ``auth_domain`` gets no Authorization header."""
    ia_server.files["/items/public/file"] = data
    ia_server.data_node = f"http://127.0.0.1:{ia_server.node_port}"  # not `localhost`

    fs = InternetArchiveFileSystem(access_key="k", secret_key="s")
    assert fs.cat("ia://public/file") == data
    first_hop = next(h for p, h in ia_server.hits if p.startswith("/download/"))
    assert first_hop.get("Authorization") == "LOW k:s"
    data_hop = next(h for p, h in ia_server.hits if p.startswith("/items/"))
    assert "Authorization" not in data_hop


@pytest.mark.skipif(not os.environ.get("IA_NETWORK_TESTS"), reason="needs archive.org")
def test_public_item_over_the_network():
    fs = InternetArchiveFileSystem(access_key="", secret_key="")
    assert fs.cat_file(PUBLIC_ITEM, start=0, end=2) == b"\x1f\x8b"  # gzip magic
    assert fs.size(PUBLIC_ITEM) > 1_000_000_000
