import pytest

from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.spec import AbstractFileSystem

PATH = "path/to/dir"
ARGS = ["foo", "bar"]
KWARGS = {"baz": "baz", "qux": "qux"}


@pytest.fixture
def make_fs(mocker):
    def _make_fs(async_impl=False, asynchronous=False):
        attrs = {
            "sep": "/",
            "async_impl": async_impl,
            "_strip_protocol": lambda path: path,
        }

        if async_impl:
            attrs["asynchronous"] = asynchronous
            cls = AsyncFileSystem
        else:
            cls = AbstractFileSystem

        fs = mocker.MagicMock(spec=cls, **attrs)

        return fs

    return _make_fs


@pytest.fixture(
    params=[
        pytest.param(False, id="sync"),
        pytest.param(True, id="async"),
    ]
)
def fs(make_fs, request):
    return make_fs(async_impl=request.param)


@pytest.fixture
def asyncfs(make_fs):
    return make_fs(async_impl=True, asynchronous=True)


@pytest.fixture
def make_dirfs():
    def _make_dirfs(fs, asynchronous=False):
        return DirFileSystem(PATH, fs, asynchronous=asynchronous)

    return _make_dirfs


@pytest.fixture
def dirfs(make_dirfs, fs):
    return make_dirfs(fs)


@pytest.fixture
def adirfs(make_dirfs, asyncfs):
    return make_dirfs(asyncfs, asynchronous=True)


def test_dirfs(fs, asyncfs):
    DirFileSystem("path", fs)
    DirFileSystem("path", asyncfs, asynchronous=True)

    with pytest.raises(ValueError):
        DirFileSystem("path", asyncfs)

    with pytest.raises(ValueError):
        DirFileSystem("path", fs, asynchronous=True)


@pytest.mark.parametrize(
    "root, rel, full",
    [
        ("", "", ""),
        ("", "foo", "foo"),
        ("root", "", "root"),
        ("root", "foo", "root/foo"),
        ("/root", "", "/root"),
        ("/root", "foo", "/root/foo"),
    ],
)
def test_path(fs, root, rel, full):
    dirfs = DirFileSystem(root, fs)
    assert dirfs._join(rel) == full
    assert dirfs._relpath(full) == rel


@pytest.mark.parametrize(
    "root, rel, full",
    [
        ("/root", "foo", "root/foo"),
        ("/root", "", "root"),
    ],
)
def test_path_no_leading_slash(fs, root, rel, full):
    dirfs = DirFileSystem(root, fs)
    assert dirfs._relpath(full) == rel


def test_sep(mocker, dirfs):
    sep = mocker.Mock()
    dirfs.fs.sep = sep
    assert dirfs.sep == sep


@pytest.mark.asyncio
async def test_set_session(mocker, adirfs):
    adirfs.fs.set_session = mocker.AsyncMock()
    assert (
        await adirfs.set_session(*ARGS, **KWARGS) == adirfs.fs.set_session.return_value
    )
    adirfs.fs.set_session.assert_called_once_with(*ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_rm_file(adirfs):
    await adirfs._rm_file("file", **KWARGS)
    adirfs.fs._rm_file.assert_called_once_with(f"{PATH}/file", **KWARGS)


def test_rm_file(dirfs):
    dirfs.rm_file("file", **KWARGS)
    dirfs.fs.rm_file.assert_called_once_with("path/to/dir/file", **KWARGS)


@pytest.mark.asyncio
async def test_async_rm(adirfs):
    await adirfs._rm("file", *ARGS, **KWARGS)
    adirfs.fs._rm.assert_called_once_with("path/to/dir/file", *ARGS, **KWARGS)


def test_rm(dirfs):
    dirfs.rm("file", *ARGS, **KWARGS)
    dirfs.fs.rm.assert_called_once_with("path/to/dir/file", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_cp_file(adirfs):
    await adirfs._cp_file("one", "two", **KWARGS)
    adirfs.fs._cp_file.assert_called_once_with(f"{PATH}/one", f"{PATH}/two", **KWARGS)


def test_cp_file(dirfs):
    dirfs.cp_file("one", "two", **KWARGS)
    dirfs.fs.cp_file.assert_called_once_with(f"{PATH}/one", f"{PATH}/two", **KWARGS)


@pytest.mark.asyncio
async def test_async_copy(adirfs):
    await adirfs._copy("one", "two", *ARGS, **KWARGS)
    adirfs.fs._copy.assert_called_once_with(
        f"{PATH}/one", f"{PATH}/two", *ARGS, **KWARGS
    )


def test_copy(dirfs):
    dirfs.copy("one", "two", *ARGS, **KWARGS)
    dirfs.fs.copy.assert_called_once_with(f"{PATH}/one", f"{PATH}/two", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_pipe(adirfs):
    await adirfs._pipe("file", *ARGS, **KWARGS)
    adirfs.fs._pipe.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_pipe(dirfs):
    dirfs.pipe("file", *ARGS, **KWARGS)
    dirfs.fs.pipe.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_pipe_dict(dirfs):
    dirfs.pipe({"file": b"foo"}, *ARGS, **KWARGS)
    dirfs.fs.pipe.assert_called_once_with({f"{PATH}/file": b"foo"}, *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_pipe_file(adirfs):
    await adirfs._pipe_file("file", *ARGS, **KWARGS)
    adirfs.fs._pipe_file.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_pipe_file(dirfs):
    dirfs.pipe_file("file", *ARGS, **KWARGS)
    dirfs.fs.pipe_file.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_cat_file(adirfs):
    assert (
        await adirfs._cat_file("file", *ARGS, **KWARGS)
        == adirfs.fs._cat_file.return_value
    )
    adirfs.fs._cat_file.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_cat_file(dirfs):
    assert dirfs.cat_file("file", *ARGS, **KWARGS) == dirfs.fs.cat_file.return_value
    dirfs.fs.cat_file.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_cat(adirfs):
    assert await adirfs._cat("file", *ARGS, **KWARGS) == adirfs.fs._cat.return_value
    adirfs.fs._cat.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_cat(dirfs):
    assert dirfs.cat("file", *ARGS, **KWARGS) == dirfs.fs.cat.return_value
    dirfs.fs.cat.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_cat_list(adirfs):
    adirfs.fs._cat.return_value = {f"{PATH}/one": "foo", f"{PATH}/two": "bar"}
    assert await adirfs._cat(["one", "two"], *ARGS, **KWARGS) == {
        "one": "foo",
        "two": "bar",
    }
    adirfs.fs._cat.assert_called_once_with(
        [f"{PATH}/one", f"{PATH}/two"], *ARGS, **KWARGS
    )


def test_cat_list(dirfs):
    dirfs.fs.cat.return_value = {f"{PATH}/one": "foo", f"{PATH}/two": "bar"}
    assert dirfs.cat(["one", "two"], *ARGS, **KWARGS) == {"one": "foo", "two": "bar"}
    dirfs.fs.cat.assert_called_once_with(
        [f"{PATH}/one", f"{PATH}/two"], *ARGS, **KWARGS
    )


@pytest.mark.asyncio
async def test_async_put_file(adirfs):
    await adirfs._put_file("local", "file", **KWARGS)
    adirfs.fs._put_file.assert_called_once_with("local", f"{PATH}/file", **KWARGS)


def test_put_file(dirfs):
    dirfs.put_file("local", "file", **KWARGS)
    dirfs.fs.put_file.assert_called_once_with("local", f"{PATH}/file", **KWARGS)


@pytest.mark.asyncio
async def test_async_put(adirfs):
    await adirfs._put("local", "file", **KWARGS)
    adirfs.fs._put.assert_called_once_with("local", f"{PATH}/file", **KWARGS)


def test_put(dirfs):
    dirfs.put("local", "file", **KWARGS)
    dirfs.fs.put.assert_called_once_with("local", f"{PATH}/file", **KWARGS)


@pytest.mark.asyncio
async def test_async_get_file(adirfs):
    await adirfs._get_file("file", "local", **KWARGS)
    adirfs.fs._get_file.assert_called_once_with(f"{PATH}/file", "local", **KWARGS)


def test_get_file(dirfs):
    dirfs.get_file("file", "local", **KWARGS)
    dirfs.fs.get_file.assert_called_once_with(f"{PATH}/file", "local", **KWARGS)


@pytest.mark.asyncio
async def test_async_get(adirfs):
    await adirfs._get("file", "local", **KWARGS)
    adirfs.fs._get.assert_called_once_with(f"{PATH}/file", "local", **KWARGS)


def test_get(dirfs):
    dirfs.get("file", "local", **KWARGS)
    dirfs.fs.get.assert_called_once_with(f"{PATH}/file", "local", **KWARGS)


@pytest.mark.asyncio
async def test_async_isfile(adirfs):
    assert await adirfs._isfile("file") == adirfs.fs._isfile.return_value
    adirfs.fs._isfile.assert_called_once_with(f"{PATH}/file")


def test_isfile(dirfs):
    assert dirfs.isfile("file") == dirfs.fs.isfile.return_value
    dirfs.fs.isfile.assert_called_once_with(f"{PATH}/file")


@pytest.mark.asyncio
async def test_async_isdir(adirfs):
    assert await adirfs._isdir("file") == adirfs.fs._isdir.return_value
    adirfs.fs._isdir.assert_called_once_with(f"{PATH}/file")


def test_isdir(dirfs):
    assert dirfs.isdir("file") == dirfs.fs.isdir.return_value
    dirfs.fs.isdir.assert_called_once_with(f"{PATH}/file")


@pytest.mark.asyncio
async def test_async_size(adirfs):
    assert await adirfs._size("file") == adirfs.fs._size.return_value
    adirfs.fs._size.assert_called_once_with(f"{PATH}/file")


def test_size(dirfs):
    assert dirfs.size("file") == dirfs.fs.size.return_value
    dirfs.fs.size.assert_called_once_with(f"{PATH}/file")


@pytest.mark.asyncio
async def test_async_exists(adirfs):
    assert await adirfs._exists("file") == adirfs.fs._exists.return_value
    adirfs.fs._exists.assert_called_once_with(f"{PATH}/file")


def test_exists(dirfs):
    assert dirfs.exists("file") == dirfs.fs.exists.return_value
    dirfs.fs.exists.assert_called_once_with(f"{PATH}/file")


@pytest.mark.asyncio
async def test_async_info(adirfs):
    assert await adirfs._info("file", **KWARGS) == adirfs.fs._info.return_value
    adirfs.fs._info.assert_called_once_with(f"{PATH}/file", **KWARGS)


def test_info(dirfs):
    assert dirfs.info("file", **KWARGS) == dirfs.fs.info.return_value
    dirfs.fs.info.assert_called_once_with(f"{PATH}/file", **KWARGS)


@pytest.mark.asyncio
async def test_async_ls(adirfs):
    adirfs.fs._ls.return_value = [f"{PATH}/file"]
    assert await adirfs._ls("file", detail=False, **KWARGS) == ["file"]
    adirfs.fs._ls.assert_called_once_with(f"{PATH}/file", detail=False, **KWARGS)


def test_ls(dirfs):
    dirfs.fs.ls.return_value = [f"{PATH}/file"]
    assert dirfs.ls("file", detail=False, **KWARGS) == ["file"]
    dirfs.fs.ls.assert_called_once_with(f"{PATH}/file", detail=False, **KWARGS)


@pytest.mark.asyncio
async def test_async_ls_detail(adirfs):
    adirfs.fs._ls.return_value = [{"name": f"{PATH}/file", "foo": "bar"}]
    assert await adirfs._ls("file", detail=True, **KWARGS) == [
        {"name": "file", "foo": "bar"}
    ]
    adirfs.fs._ls.assert_called_once_with(f"{PATH}/file", detail=True, **KWARGS)


def test_ls_detail(dirfs):
    dirfs.fs.ls.return_value = [{"name": f"{PATH}/file", "foo": "bar"}]
    assert dirfs.ls("file", detail=True, **KWARGS) == [{"name": "file", "foo": "bar"}]
    dirfs.fs.ls.assert_called_once_with(f"{PATH}/file", detail=True, **KWARGS)


@pytest.mark.asyncio
async def test_async_walk(adirfs, mocker):
    async def _walk(path, *args, **kwargs):
        yield (f"{PATH}/root", ["foo", "bar"], ["baz", "qux"])

    adirfs.fs._walk = mocker.MagicMock()
    adirfs.fs._walk.side_effect = _walk

    actual = [entry async for entry in adirfs._walk("root", *ARGS, **KWARGS)]
    assert actual == [("root", ["foo", "bar"], ["baz", "qux"])]
    adirfs.fs._walk.assert_called_once_with(f"{PATH}/root", *ARGS, **KWARGS)


def test_walk(dirfs):
    dirfs.fs.walk.return_value = iter(
        [(f"{PATH}/root", ["foo", "bar"], ["baz", "qux"])]
    )
    assert list(dirfs.walk("root", *ARGS, **KWARGS)) == [
        ("root", ["foo", "bar"], ["baz", "qux"])
    ]
    dirfs.fs.walk.assert_called_once_with(f"{PATH}/root", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_glob(adirfs):
    adirfs.fs._glob.return_value = [f"{PATH}/one", f"{PATH}/two"]
    assert await adirfs._glob("*", **KWARGS) == ["one", "two"]
    adirfs.fs._glob.assert_called_once_with(f"{PATH}/*", **KWARGS)


def test_glob(dirfs):
    dirfs.fs.glob.return_value = [f"{PATH}/one", f"{PATH}/two"]
    assert dirfs.glob("*", **KWARGS) == ["one", "two"]
    dirfs.fs.glob.assert_called_once_with(f"{PATH}/*", **KWARGS)


def test_glob_with_protocol(dirfs):
    dirfs.fs.glob.return_value = [f"{PATH}/one", f"{PATH}/two"]
    assert dirfs.glob("dir://*", **KWARGS) == ["one", "two"]
    dirfs.fs.glob.assert_called_once_with(f"{PATH}/*", **KWARGS)


@pytest.mark.asyncio
async def test_async_glob_detail(adirfs):
    adirfs.fs._glob.return_value = {
        f"{PATH}/one": {"foo": "bar"},
        f"{PATH}/two": {"baz": "qux"},
    }
    assert await adirfs._glob("*", detail=True, **KWARGS) == {
        "one": {"foo": "bar"},
        "two": {"baz": "qux"},
    }
    adirfs.fs._glob.assert_called_once_with(f"{PATH}/*", detail=True, **KWARGS)


def test_glob_detail(dirfs):
    dirfs.fs.glob.return_value = {
        f"{PATH}/one": {"foo": "bar"},
        f"{PATH}/two": {"baz": "qux"},
    }
    assert dirfs.glob("*", detail=True, **KWARGS) == {
        "one": {"foo": "bar"},
        "two": {"baz": "qux"},
    }
    dirfs.fs.glob.assert_called_once_with(f"{PATH}/*", detail=True, **KWARGS)


@pytest.mark.asyncio
async def test_async_du(adirfs):
    adirfs.fs._du.return_value = 1234
    assert await adirfs._du("file", *ARGS, **KWARGS) == 1234
    adirfs.fs._du.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_du(dirfs):
    dirfs.fs.du.return_value = 1234
    assert dirfs.du("file", *ARGS, **KWARGS) == 1234
    dirfs.fs.du.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_du_granular(adirfs):
    adirfs.fs._du.return_value = {f"{PATH}/dir/one": 1, f"{PATH}/dir/two": 2}
    assert await adirfs._du("dir", *ARGS, total=False, **KWARGS) == {
        "dir/one": 1,
        "dir/two": 2,
    }
    adirfs.fs._du.assert_called_once_with(f"{PATH}/dir", *ARGS, total=False, **KWARGS)


def test_du_granular(dirfs):
    dirfs.fs.du.return_value = {f"{PATH}/dir/one": 1, f"{PATH}/dir/two": 2}
    assert dirfs.du("dir", *ARGS, total=False, **KWARGS) == {"dir/one": 1, "dir/two": 2}
    dirfs.fs.du.assert_called_once_with(f"{PATH}/dir", *ARGS, total=False, **KWARGS)


@pytest.mark.asyncio
async def test_async_find(adirfs):
    adirfs.fs._find.return_value = [f"{PATH}/dir/one", f"{PATH}/dir/two"]
    assert await adirfs._find("dir", *ARGS, **KWARGS) == ["dir/one", "dir/two"]
    adirfs.fs._find.assert_called_once_with(f"{PATH}/dir", *ARGS, **KWARGS)


def test_find(dirfs):
    dirfs.fs.find.return_value = [f"{PATH}/dir/one", f"{PATH}/dir/two"]
    assert dirfs.find("dir", *ARGS, **KWARGS) == ["dir/one", "dir/two"]
    dirfs.fs.find.assert_called_once_with(f"{PATH}/dir", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_find_detail(adirfs):
    adirfs.fs._find.return_value = {
        f"{PATH}/dir/one": {"foo": "bar"},
        f"{PATH}/dir/two": {"baz": "qux"},
    }
    assert await adirfs._find("dir", *ARGS, detail=True, **KWARGS) == {
        "dir/one": {"foo": "bar"},
        "dir/two": {"baz": "qux"},
    }
    adirfs.fs._find.assert_called_once_with(f"{PATH}/dir", *ARGS, detail=True, **KWARGS)


def test_find_detail(dirfs):
    dirfs.fs.find.return_value = {
        f"{PATH}/dir/one": {"foo": "bar"},
        f"{PATH}/dir/two": {"baz": "qux"},
    }
    assert dirfs.find("dir", *ARGS, detail=True, **KWARGS) == {
        "dir/one": {"foo": "bar"},
        "dir/two": {"baz": "qux"},
    }
    dirfs.fs.find.assert_called_once_with(f"{PATH}/dir", *ARGS, detail=True, **KWARGS)


@pytest.mark.asyncio
async def test_async_expand_path(adirfs):
    adirfs.fs._expand_path.return_value = [f"{PATH}/file"]
    assert await adirfs._expand_path("*", *ARGS, **KWARGS) == ["file"]
    adirfs.fs._expand_path.assert_called_once_with(f"{PATH}/*", *ARGS, **KWARGS)


def test_expand_path(dirfs):
    dirfs.fs.expand_path.return_value = [f"{PATH}/file"]
    assert dirfs.expand_path("*", *ARGS, **KWARGS) == ["file"]
    dirfs.fs.expand_path.assert_called_once_with(f"{PATH}/*", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_expand_path_list(adirfs):
    adirfs.fs._expand_path.return_value = [f"{PATH}/1file", f"{PATH}/2file"]
    assert await adirfs._expand_path(["1*", "2*"], *ARGS, **KWARGS) == [
        "1file",
        "2file",
    ]
    adirfs.fs._expand_path.assert_called_once_with(
        [f"{PATH}/1*", f"{PATH}/2*"], *ARGS, **KWARGS
    )


def test_expand_path_list(dirfs):
    dirfs.fs.expand_path.return_value = [f"{PATH}/1file", f"{PATH}/2file"]
    assert dirfs.expand_path(["1*", "2*"], *ARGS, **KWARGS) == ["1file", "2file"]
    dirfs.fs.expand_path.assert_called_once_with(
        [f"{PATH}/1*", f"{PATH}/2*"], *ARGS, **KWARGS
    )


@pytest.mark.asyncio
async def test_async_mkdir(adirfs):
    await adirfs._mkdir("dir", *ARGS, **KWARGS)
    adirfs.fs._mkdir.assert_called_once_with(f"{PATH}/dir", *ARGS, **KWARGS)


def test_mkdir(dirfs):
    dirfs.mkdir("dir", *ARGS, **KWARGS)
    dirfs.fs.mkdir.assert_called_once_with(f"{PATH}/dir", *ARGS, **KWARGS)


@pytest.mark.asyncio
async def test_async_makedirs(adirfs):
    await adirfs._makedirs("dir", *ARGS, **KWARGS)
    adirfs.fs._makedirs.assert_called_once_with(f"{PATH}/dir", *ARGS, **KWARGS)


def test_makedirs(dirfs):
    dirfs.makedirs("dir", *ARGS, **KWARGS)
    dirfs.fs.makedirs.assert_called_once_with(f"{PATH}/dir", *ARGS, **KWARGS)


def test_rmdir(mocker, dirfs):
    dirfs.fs.rmdir = mocker.Mock()
    dirfs.rmdir("dir")
    dirfs.fs.rmdir.assert_called_once_with(f"{PATH}/dir")


def test_mv(mocker, dirfs):
    dirfs.fs.mv = mocker.Mock()
    dirfs.mv("one", "two", **KWARGS)
    dirfs.fs.mv.assert_called_once_with(f"{PATH}/one", f"{PATH}/two", **KWARGS)


def test_touch(mocker, dirfs):
    dirfs.fs.touch = mocker.Mock()
    dirfs.touch("file", **KWARGS)
    dirfs.fs.touch.assert_called_once_with(f"{PATH}/file", **KWARGS)


def test_created(mocker, dirfs):
    dirfs.fs.created = mocker.Mock(return_value="date")
    assert dirfs.created("file") == "date"
    dirfs.fs.created.assert_called_once_with(f"{PATH}/file")


def test_modified(mocker, dirfs):
    dirfs.fs.modified = mocker.Mock(return_value="date")
    assert dirfs.modified("file") == "date"
    dirfs.fs.modified.assert_called_once_with(f"{PATH}/file")


def test_sign(mocker, dirfs):
    dirfs.fs.sign = mocker.Mock(return_value="url")
    assert dirfs.sign("file", *ARGS, **KWARGS) == "url"
    dirfs.fs.sign.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_open(mocker, dirfs):
    dirfs.fs.open = mocker.Mock()
    assert dirfs.open("file", *ARGS, **KWARGS) == dirfs.fs.open.return_value
    dirfs.fs.open.assert_called_once_with(f"{PATH}/file", *ARGS, **KWARGS)


def test_from_url(m):
    from fsspec.core import url_to_fs

    m.pipe("inner/file", b"data")
    fs, _ = url_to_fs("dir::memory://inner")
    assert fs.ls("", False) == ["file"]
    assert fs.ls("", True)[0]["name"] == "file"
    assert fs.cat("file") == b"data"
