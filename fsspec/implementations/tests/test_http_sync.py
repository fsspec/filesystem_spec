import io
import json
import os
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

import fsspec.utils
from fsspec.tests.conftest import (  # noqa: F401
    HTTPTestHandler,
    data,
    requests,
    reset_faults,
    reset_files,
    server,
    win,
)


@pytest.fixture()
def sync():
    from fsspec.implementations.http_sync import register, unregister

    register()
    yield
    unregister()


@pytest.mark.parametrize("method", ["get", "head", "post", "put", "patch", "delete"])
def test_session_shim_request_method(monkeypatch, method):
    from fsspec.implementations.http_sync import RequestsSessionShim

    session = RequestsSessionShim()
    response = object()
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return response

    monkeypatch.setattr(session, "request", request)
    url = "https://example.com/data"
    options = {"headers": {"X-Test": "value"}, "timeout": 10}
    assert getattr(session, method)(url, **options) is response
    assert calls == [(method.upper(), url, options)]


@pytest.fixture
def js_session_shim(monkeypatch):
    """Check the conversion protocol without requiring Pyodide in native CI."""
    request = Mock()
    blob = Mock()
    object_type = SimpleNamespace(fromEntries=object())
    to_js = Mock(side_effect=lambda value, **kwargs: value)
    ffi = SimpleNamespace(to_js=to_js)
    monkeypatch.setitem(
        sys.modules,
        "js",
        SimpleNamespace(
            Blob=blob, Object=object_type, XMLHttpRequest=Mock(new=lambda: request)
        ),
    )
    monkeypatch.setitem(sys.modules, "pyodide", SimpleNamespace(ffi=ffi))
    monkeypatch.setitem(sys.modules, "pyodide.ffi", ffi)
    return request, blob, object_type, to_js


@pytest.mark.parametrize("value", [{"a": 1}, {}, [], False, 0, ""])
def test_session_shim_json_body(js_session_shim, value):
    from fsspec.implementations.http_sync import RequestsSessionShim

    request, blob, object_type, to_js = js_session_shim
    RequestsSessionShim().post("https://example.com/data", json=value)

    parts = [json.dumps(value)]
    options = {"type": "application/json"}
    assert to_js.call_args_list == [
        call(parts),
        call(options, dict_converter=object_type.fromEntries),
    ]
    blob.new.assert_called_once_with(parts, options)
    request.send.assert_called_once_with(blob.new.return_value)


@pytest.mark.parametrize("value", [b"hello", io.BytesIO(b"hello"), "hello"])
def test_session_shim_data_body(js_session_shim, value):
    from fsspec.implementations.http_sync import RequestsSessionShim

    request, blob, object_type, to_js = js_session_shim
    RequestsSessionShim().post("https://example.com/data", data=value)

    parts = [b"hello" if isinstance(value, io.IOBase) else value]
    options = {"type": "application/octet-stream"}
    assert to_js.call_args_list == [
        call(parts),
        call(options, dict_converter=object_type.fromEntries),
    ]
    blob.new.assert_called_once_with(parts, options)
    request.send.assert_called_once_with(blob.new.return_value)


@pytest.mark.parametrize("value", [{"a": 1}, {}, False, 0])
def test_session_shim_rejects_data_and_json(js_session_shim, value):
    from fsspec.implementations.http_sync import RequestsSessionShim

    request, blob, _, to_js = js_session_shim
    with pytest.raises(ValueError, match="Use json= or data=, not both"):
        RequestsSessionShim().post(
            "https://example.com/data", data=b"hello", json=value
        )
    request.send.assert_not_called()
    blob.new.assert_not_called()
    to_js.assert_not_called()


@pytest.mark.parametrize("chunks", [(), (b"he", b"llo")])
def test_session_shim_iterator_body(js_session_shim, chunks):
    from fsspec.implementations.http_sync import RequestsSessionShim

    request, blob, object_type, to_js = js_session_shim
    RequestsSessionShim().post("https://example.com/data", data=iter(chunks))

    parts = list(chunks)
    options = {"type": "application/octet-stream"}
    assert to_js.call_args_list == [
        call(parts),
        call(options, dict_converter=object_type.fromEntries),
    ]
    blob.new.assert_called_once_with(parts, options)
    request.send.assert_called_once_with(blob.new.return_value)


def test_session_shim_without_body(js_session_shim):
    from fsspec.implementations.http_sync import RequestsSessionShim

    request, blob, _, to_js = js_session_shim
    RequestsSessionShim().post("https://example.com/data")
    request.send.assert_called_once_with(None)
    blob.new.assert_not_called()
    to_js.assert_not_called()


def test_list(server, sync):
    h = fsspec.filesystem("http")
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]


def test_list_pathological_listing(server, sync, reset_files):
    # a long whitespace run inside an anchor used to make the link regex
    # backtrack cubically, so listing this page took about a minute
    from fsspec.tests.conftest import HTTPTestHandler

    body = b"<a" + b" " * 1500 + b'x><a href="/slow/file">file</a>'
    HTTPTestHandler.dynamic_files["/slow"] = body
    h = fsspec.filesystem("http", skip_instance_cache=True)
    start = time.monotonic()
    out = h.ls(server.address + "/slow", detail=False)
    assert time.monotonic() - start < 10
    assert out == [server.address + "/slow/file"]


def test_list_invalid_args(server, sync):
    with pytest.raises(TypeError):
        h = fsspec.filesystem("http", use_foobar=True)
        h.glob(server.address + "/index/*")


@pytest.mark.parametrize("retry_without_slash", [False, True])
def test_list_request_options(server, sync, monkeypatch, retry_without_slash):
    from fsspec.tests.conftest import HTTPTestHandler

    received = []
    original_handler = HTTPTestHandler.do_GET

    def respond(handler):
        if handler.path not in ("/options", "/options/"):
            return original_handler(handler)
        received.append((handler.path, handler.headers.get("X-Listing")))
        body = (
            b"" if handler.path.endswith("/") else b'<a href="/options/file">file</a>'
        )
        handler._respond(data=body)

    monkeypatch.setattr(HTTPTestHandler, "do_GET", respond)
    h = fsspec.filesystem(
        "http", headers={"X-Listing": "default"}, timeout=3, skip_instance_cache=True
    )
    timeouts = []
    original_get = h.session.get

    def get(*args, **kwargs):
        timeouts.append(kwargs.get("timeout"))
        return original_get(*args, **kwargs)

    monkeypatch.setattr(h.session, "get", get)
    suffix = "/" if retry_without_slash else ""
    url = server.address + "/options" + suffix
    paths = ["/options/", "/options"] if retry_without_slash else ["/options"]
    try:
        assert h.ls(url, detail=False, headers={"X-Listing": "call"}, timeout=1) == [
            server.address + "/options/file"
        ]
        assert received == [(path, "call") for path in paths]
        assert timeouts == [1] * len(paths)
        received.clear()
        timeouts.clear()
        assert h.ls(url, detail=False) == [server.address + "/options/file"]
        assert received == [(path, "default") for path in paths]
        assert timeouts == [3] * len(paths)
        assert h.kwargs == {"headers": {"X-Listing": "default"}, "timeout": 3}
    finally:
        h.session.close()


def test_list_cache(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True)
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]


@pytest.mark.parametrize("first_detail", [True, False])
def test_list_cache_detail_shape_is_independent_of_the_first_caller(
    server, sync, first_detail
):
    """The cached listing must not depend on which shape the first caller asked for.

    Same contract as the async implementation in http.py: ``info``/``find``/``glob`` all
    go through ``ls(detail=True)``, so a cached list of strings breaks them.
    """
    h = fsspec.filesystem("http", use_listings_cache=True)
    url = server.address + "/index/"

    first = h.ls(url, detail=first_detail)
    second = h.ls(url, detail=not first_detail)
    third = h.ls(url, detail=first_detail)

    # Whichever order the two shapes arrive in, each call gets what it asked for.
    shape = {True: dict, False: str}
    assert all(type(x) is shape[first_detail] for x in first)
    assert all(type(x) is shape[not first_detail] for x in second)
    assert all(type(x) is shape[first_detail] for x in third)

    # The projected names must be the ones the detailed listing carries.
    detailed = h.ls(url, detail=True)
    assert h.ls(url, detail=False) == sorted(d["name"] for d in detailed)


def test_list_cache_detail_false_does_not_poison_the_detailed_consumers(server, sync):
    """info/find/glob read the cached entry as dicts and must keep working."""
    h = fsspec.filesystem("http", use_listings_cache=True)
    url = server.address + "/index/"

    h.ls(url, detail=False)

    assert h.ls(url) == h.ls(url, detail=True)
    assert h.glob(url + "*") == [server.realfile]
    assert h.info(server.realfile)["type"] == "file"


def test_list_cache_with_expiry_time_cached(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=30)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]

    # Verify cache content.
    assert len(h.dircache) == 1

    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]


def test_list_cache_with_expiry_time_purged(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=0.3)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]
    assert len(h.dircache) == 1

    # Verify cache content.
    assert server.address + "/index/" in h.dircache
    assert len(h.dircache.get(server.address + "/index/")) == 1

    # Wait beyond the TTL / cache expiry time.
    time.sleep(0.31)

    # Verify that the cache item should have been purged.
    cached_items = h.dircache.get(server.address + "/index/")
    assert cached_items is None

    # Verify that after clearing the item from the cache,
    # it can get populated again.
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]
    cached_items = h.dircache.get(server.address + "/index/")
    assert len(cached_items) == 1


def test_list_cache_reuse(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)

    # First, the directory cache is not initialized.
    assert not h.dircache

    # By querying the filesystem with "use_listings_cache=True",
    # the cache will automatically get populated.
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]

    # Verify cache content.
    assert len(h.dircache) == 1

    # Verify another instance without caching enabled does not have cache content.
    h = fsspec.filesystem("http", use_listings_cache=False)
    assert not h.dircache

    # Verify that yet another new instance, with caching enabled,
    # will see the same cache content again.
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=5)
    assert len(h.dircache) == 1

    # However, yet another instance with a different expiry time will also not have
    # any valid cache content.
    h = fsspec.filesystem("http", use_listings_cache=True, listings_expiry_time=666)
    assert len(h.dircache) == 0


def test_ls_raises_filenotfound(server, sync):
    h = fsspec.filesystem("http")

    with pytest.raises(FileNotFoundError):
        h.ls(server.address + "/not-a-key")


def test_list_cache_with_max_paths(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, max_paths=5)
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]


def test_list_cache_with_skip_instance_cache(server, sync):
    h = fsspec.filesystem("http", use_listings_cache=True, skip_instance_cache=True)
    out = h.glob(server.address + "/index/*")
    assert out == [server.address + "/index/realfile"]


def test_isdir(server, sync):
    h = fsspec.filesystem("http")
    assert h.isdir(server.address + "/index/")
    assert not h.isdir(server.address + "/index/realfile")
    assert not h.isdir(server.address + "doesnotevenexist")


def test_exists(server, sync):
    h = fsspec.filesystem("http")
    assert not h.exists(server.address + "/notafile")
    with pytest.raises(FileNotFoundError):
        h.cat(server.address + "/notafile")


def test_exists_strict(server, sync):
    h = fsspec.filesystem("http")
    assert not h.exists(server.address + "/notafile", strict=True)
    with pytest.raises(requests.exceptions.HTTPError) as e:
        h.exists(server.address + "/unauthorized", strict=True)
    assert e.value.response.status_code == 401


def test_read(server, sync):
    h = fsspec.filesystem("http")
    out = server.address + "/index/realfile"
    # with h.open(out, "rb") as f:
    #    assert f.read() == data
    # with h.open(out, "rb", block_size=0) as f:
    #    assert f.read() == data
    with h.open(out, "rb") as f:
        o1 = f.read(100)
        o2 = f.read()
        assert o1 + o2 == data


def test_methods(server, sync):
    h = fsspec.filesystem("http")
    url = server.address + "/index/realfile"
    assert h.exists(url)
    assert h.cat(url) == data


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"give_length": "true"},
        {"give_length": "true", "head_ok": "true"},
        {"give_range": "true"},
        {"give_length": "true", "head_not_auth": "true"},
        {"give_range": "true", "head_not_auth": "true"},
        {"use_206": "true", "head_ok": "true", "head_give_length": "true"},
        {"use_206": "true", "give_length": "true"},
        {"use_206": "true", "give_range": "true"},
    ],
)
def test_random_access(server, headers, sync):
    h = fsspec.filesystem("http", headers=headers)
    url = server.address + "/index/realfile"
    with h.open(url, "rb") as f:
        if headers:
            assert f.size == len(data)
        assert f.read(5) == data[:5]

        if headers:
            f.seek(5, 1)
            assert f.read(5) == data[10:15]
        else:
            with pytest.raises(ValueError):
                f.seek(5, 1)
    assert f.closed


@pytest.mark.parametrize(
    "headers",
    [
        {"ignore_range": "true", "head_ok": "true", "give_length": "true"},
        {"ignore_range": "true", "give_length": "true"},
        {"ignore_range": "true", "give_range": "true"},
    ],
)
def test_no_range_support(server, headers, sync):
    h = fsspec.filesystem("http", headers=headers)
    url = server.address + "/index/realfile"
    with h.open(url, "rb") as f:
        # Random access is not possible if the server doesn't respect Range
        f.seek(5)
        with pytest.raises(ValueError):
            f.read(10)

        # Reading from the beginning should still work
        f.seek(0)
        assert f.read(10) == data[:10]


def test_mapper_url(server, sync):
    h = fsspec.filesystem("http")
    mapper = h.get_mapper(server.address + "/index/")
    assert mapper.root.startswith("http:")
    assert list(mapper)

    mapper2 = fsspec.get_mapper(server.address + "/index/")
    assert mapper2.root.startswith("http:")
    assert list(mapper) == list(mapper2)


def test_content_length_zero(server, sync):
    h = fsspec.filesystem(
        "http", headers={"give_length": "true", "zero_length": "true"}
    )
    url = server.address + "/index/realfile"

    with h.open(url, "rb") as f:
        assert f.read() == data


@pytest.mark.parametrize("head_ok", [False, True])
@pytest.mark.parametrize("chunked", [False, True])
def test_content_encoded_response_is_streamed(server, sync, head_ok, chunked):
    headers = {"give_length": "true", "gzip_encoding": "true"}
    if head_ok:
        headers["head_ok"] = "true"
    h = fsspec.filesystem("http", headers=headers, block_size=512)
    try:
        with h.open(server.realfile, "rb") as f:
            if chunked:
                chunks = []
                while chunk := f.read(17):
                    chunks.append(chunk)
                actual = b"".join(chunks)
            else:
                actual = f.read()
            assert actual == data
    finally:
        h.session.close()


@pytest.mark.parametrize("lowercase", [False, True])
@pytest.mark.parametrize("encoding", ["", "identity", "gzip"])
def test_file_info_content_encoding_preserves_checksums(lowercase, encoding):
    from fsspec.implementations.http_sync import _file_info

    headers = {"Content-Length": "5", "Content-Encoding": encoding}
    if lowercase:
        headers = {k.lower(): v for k, v in headers.items()}
    headers["ETag"] = "checksum"

    class Response:
        def raise_for_status(self):
            pass

    class Session:
        def head(self, *args, **kwargs):
            response = Response()
            response.headers = headers
            return response

    info = _file_info("https://example.com/data", Session())
    assert info["ETag"] == "checksum"
    if encoding == "gzip":
        assert "size" not in info
    else:
        assert info["size"] == 5


def test_download(server, tmpdir, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    url = server.address + "/index/realfile"
    fn = os.path.join(tmpdir, "afile")
    h.get(url, fn)
    assert open(fn, "rb").read() == data


def test_multi_download(server, tmpdir, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.address + "/index/realfile"
    urlb = server.address + "/index/otherfile"
    fna = os.path.join(tmpdir, "afile")
    fnb = os.path.join(tmpdir, "bfile")
    h.get([urla, urlb], [fna, fnb])
    assert open(fna, "rb").read() == data
    assert open(fnb, "rb").read() == data


def test_ls(server, sync):
    h = fsspec.filesystem("http")
    l = h.ls(server.address + "/data/20020401/", detail=False)
    nc = server.address + "/data/20020401/GRACEDADM_CLSM0125US_7D.A20020401.030.nc4"
    assert nc in l
    assert len(l) == 11
    assert all(u["type"] == "file" for u in h.ls(server.address + "/data/20020401/"))
    assert h.glob(server.address + "/data/20020401/*.nc4") == [nc]


def test_mcat(server, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.address + "/index/realfile"
    urlb = server.address + "/index/otherfile"
    out = h.cat([urla, urlb])
    assert out == {urla: data, urlb: data}


def test_cat_file_range(server, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    urla = server.address + "/index/realfile"
    assert h.cat(urla, start=1, end=10) == data[1:10]
    assert h.cat(urla, start=1) == data[1:]

    assert h.cat(urla, start=-10) == data[-10:]
    assert h.cat(urla, start=-10, end=-2) == data[-10:-2]

    assert h.cat(urla, end=-10) == data[:-10]


@pytest.mark.parametrize(
    "headers",
    [
        {"ignore_range": "true", "give_length": "true"},
        {"ignore_range": "true", "give_range": "true"},
    ],
)
def test_cat_file_range_when_server_ignores_range(server, headers, sync):
    """The body of a server that ignores ``Range`` is the whole file, not the
    window that was asked for, so the window is read out of it instead."""
    h = fsspec.filesystem("http", headers=headers)
    urla = server.address + "/index/realfile"
    assert h.cat_file(urla, start=0, end=10) == data[:10]
    assert h.cat_file(urla, end=10) == data[:10]
    assert h.cat_file(urla, start=-10) == data[-10:]

    # a window that starts inside the file cannot be told apart from a response
    # that served more of the range than was asked for
    with pytest.raises(ValueError):
        h.cat_file(urla, start=1, end=10)


@pytest.mark.parametrize("start,end", [(None, 0), (0, 0), (1, 1), (-1, -1)])
def test_cat_file_empty_range_does_not_make_request(
    server, sync, reset_faults, start, end
):
    h = fsspec.filesystem("http", skip_instance_cache=True)

    assert h.cat_file(server.realfile, start=start, end=end) == b""
    assert HTTPTestHandler.get_counts.get("/index/realfile", 0) == 0


def test_mcat_cache(server, sync):
    urla = server.address + "/index/realfile"
    urlb = server.address + "/index/otherfile"
    fs = fsspec.filesystem("simplecache", target_protocol="http")
    assert fs.cat([urla, urlb]) == {urla: data, urlb: data}


def test_mcat_expand(server, sync):
    h = fsspec.filesystem("http", headers={"give_length": "true", "head_ok": "true"})
    out = h.cat(server.address + "/index/*")
    assert out == {server.address + "/index/realfile": data}


def test_info(server, sync):
    fs = fsspec.filesystem("http", headers={"give_etag": "true", "head_ok": "true"})
    info = fs.info(server.address + "/index/realfile")
    assert info["ETag"] == "xxx"


@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_put_file(server, tmp_path, method, reset_files, sync):
    src_file = tmp_path / "file_1"
    src_file.write_bytes(data)

    dwl_file = tmp_path / "down_1"

    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    with pytest.raises(FileNotFoundError):
        fs.info(server.address + "/hey")

    fs.put_file(src_file, server.address + "/hey", method=method)
    assert fs.info(server.address + "/hey")["size"] == len(data)

    fs.get_file(server.address + "/hey", dwl_file)
    assert dwl_file.read_bytes() == data

    src_file.write_bytes(b"xxx")
    with open(src_file, "rb") as stream:
        fs.put_file(stream, server.address + "/hey_2", method=method)
    assert fs.cat(server.address + "/hey_2") == b"xxx"

    fs.put_file(io.BytesIO(b"yyy"), server.address + "/hey_3", method=method)
    assert fs.cat(server.address + "/hey_3") == b"yyy"


@pytest.mark.parametrize("caller_owned", [False, True])
@pytest.mark.parametrize("callback_fails", [False, True])
def test_get_file_closes_only_owned_output(
    server, sync, tmp_path, monkeypatch, caller_owned, callback_fails
):
    from fsspec.callbacks import Callback
    from fsspec.implementations import http_sync

    destination = tmp_path / "download"
    opened = []
    builtin_open = open

    def track_open(path, mode):
        stream = builtin_open(path, mode)
        opened.append(stream)
        return stream

    def progress(size, value):
        if callback_fails and value:
            raise RuntimeError("progress callback failed")

    monkeypatch.setattr(http_sync, "open", track_open, raising=False)
    output = io.BytesIO() if caller_owned else destination
    callback = Callback(hooks={"progress": progress})
    fs = fsspec.filesystem("http")
    try:
        if callback_fails:
            # Keep the traceback alive: GC is not error-path resource cleanup.
            with pytest.raises(RuntimeError, match="progress callback failed") as exc:
                fs.get_file(server.realfile, output, chunk_size=5, callback=callback)
            assert exc.value.__traceback__ is not None
        else:
            fs.get_file(server.realfile, output, chunk_size=5, callback=callback)

        if caller_owned:
            assert opened == []
            assert not output.closed
            assert output.getvalue() == (data[:5] if callback_fails else data)
        else:
            assert len(opened) == 1
            assert opened[0].closed
            assert destination.read_bytes() == (data[:5] if callback_fails else data)
    finally:
        for stream in opened:
            stream.close()
        if caller_owned:
            output.close()


def test_encoded(server, sync):
    fs = fsspec.filesystem("http", encoded=False)
    out = fs.cat(server.address + "/Hello: Günter", headers={"give_path": "true"})
    assert json.loads(out)["path"] == "/Hello:%20G%C3%BCnter"


def test_with_cache(server, sync):
    fs = fsspec.filesystem("http", headers={"head_ok": "true", "give_length": "true"})
    fn = server.address + "/index/realfile"
    fs1 = fsspec.filesystem("blockcache", fs=fs)
    with fs1.open(fn, "rb") as f:
        out = f.read()
    assert out == fs1.cat(fn)
