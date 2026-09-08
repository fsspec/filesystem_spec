"""ia://<identifier>/<filename>: the Internet Archive filesystem.

Offline: the URI mapping, credential loading from ``ia.ini`` and the environment, reads
through the local HTTP test server, and the cookie-jar scoping. One opt-in network test
(``IA_NETWORK_TESTS=1``; not ``FSSPEC_IA_*``, which fsspec.config would turn into a constructor
argument) reads the first bytes of a public item.
"""

import os

import pytest

import fsspec
from fsspec.asyn import sync
from fsspec.implementations.ia import (
    InternetArchiveFileSystem,
    ia_config_path,
    load_ia_credentials,
)
from fsspec.tests.conftest import data, server  # noqa: F401

pytest.importorskip("aiohttp")

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
    fs = InternetArchiveFileSystem(cookies={})
    assert fs.unstrip_protocol(url) == "ia://some-item/some-file.warc.gz"
    assert fs.unstrip_protocol("some-item/x") == "ia://some-item/x"


def test_registered():
    fs, path = fsspec.core.url_to_fs("ia://some-item/some-file", cookies={})
    assert isinstance(fs, InternetArchiveFileSystem)
    assert path == "https://archive.org/download/some-item/some-file"
    assert fs.fsid == "ia"


def test_no_config_means_anonymous():
    assert ia_config_path() is None
    credentials = load_ia_credentials()
    assert credentials.anonymous and credentials.config_file is None
    fs = InternetArchiveFileSystem()
    assert fs.cookies == {} and fs.access_key is None
    assert "Authorization" not in fs.kwargs.get("headers", {})


def test_ini_is_parsed(ini, monkeypatch):
    monkeypatch.setenv("IA_CONFIG_FILE", ini)
    assert ia_config_path() == ini
    credentials = load_ia_credentials()
    assert (credentials.access_key, credentials.secret_key) == ("AKIA-TEST", "s3cr3t")
    assert credentials.cookies == {
        "logged-in-user": "someone%40example.org",
        "logged-in-sig": "1756000000-abcdef0123456789",
    }
    fs = InternetArchiveFileSystem()
    assert fs.kwargs["headers"]["Authorization"] == "LOW AKIA-TEST:s3cr3t"
    assert fs.cookies == credentials.cookies


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
    assert credentials.cookies
    monkeypatch.delenv("IA_SECRET_ACCESS_KEY")
    with pytest.raises(ValueError, match="must be set together"):
        load_ia_credentials(ini)


def test_explicit_arguments_win_over_the_file(ini, monkeypatch):
    monkeypatch.setenv("IA_CONFIG_FILE", ini)
    fs = InternetArchiveFileSystem(cookies={"logged-in-sig": "explicit"})
    assert fs.cookies == {"logged-in-sig": "explicit"}
    assert "Authorization" not in fs.kwargs.get("headers", {})


def test_cookies_are_scoped_to_archive_org():
    """The jar, not a header, carries the login: it must follow the redirect to a data
    node on another archive.org origin, and go nowhere else."""
    fs = InternetArchiveFileSystem(
        cookies={"logged-in-sig": "sig", "logged-in-user": "u"}
    )
    session = sync(fs.loop, fs.set_session)
    jar = session.cookie_jar
    from yarl import URL

    sent = jar.filter_cookies(URL("https://dn721904.ca.archive.org/0/items/x/y"))
    assert {m.key for m in sent.values()} == {"logged-in-sig", "logged-in-user"}
    assert not jar.filter_cookies(URL("https://archive.example.com/"))


def test_reads_go_through_the_download_url(server, monkeypatch):
    monkeypatch.setattr(InternetArchiveFileSystem, "download_url", server.address + "/")
    fs = InternetArchiveFileSystem(
        cookies={}, headers={"give_length": "true", "use_206": "true"}
    )
    assert fs.cat("ia://index/realfile") == data
    with fs.open("ia://index/realfile", "rb") as f:
        assert f.read() == data
    assert fs.cat_file("ia://index/realfile", start=1, end=10) == data[1:10]
    assert fs.info("ia://index/realfile")["size"] == len(data)


def test_refusal_is_a_permission_error(server, monkeypatch):
    monkeypatch.setattr(InternetArchiveFileSystem, "download_url", server.address + "/")
    fs = InternetArchiveFileSystem(cookies={})
    with pytest.raises(PermissionError):
        fs.cat("ia://unauthorized")
    with pytest.raises(PermissionError):
        fs.open("ia://unauthorized", "rb")
    with pytest.raises(FileNotFoundError):
        fs.cat("ia://index/missing")


@pytest.mark.skipif(not os.environ.get("IA_NETWORK_TESTS"), reason="needs archive.org")
def test_public_item_over_the_network():
    fs = InternetArchiveFileSystem(cookies={})
    assert fs.cat_file(PUBLIC_ITEM, start=0, end=2) == b"\x1f\x8b"  # gzip magic
    assert fs.size(PUBLIC_ITEM) > 1_000_000_000
