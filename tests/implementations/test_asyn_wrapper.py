import asyncio
import os
from itertools import cycle

import pytest

import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper
from fsspec.implementations.local import LocalFileSystem

from .test_local import csv_files, filetexts


class LockedFileSystem(AsyncFileSystem):
    """
    A mock file system that simulates a synchronous locking file systems with delays.
    """

    def __init__(
        self,
        asynchronous: bool = False,
        delays=None,
    ) -> None:
        self.lock = asyncio.Lock()
        self.delays = cycle((0.03, 0.01) if delays is None else delays)

        super().__init__(asynchronous=asynchronous)

    async def _cat_file(self, path, start=None, end=None) -> bytes:
        await self._simulate_io_operation(path)
        return path.encode()

    async def _await_io(self) -> None:
        await asyncio.sleep(next(self.delays))

    async def _simulate_io_operation(self, path) -> None:
        await self._check_active()
        async with self.lock:
            await self._await_io()

    async def _check_active(self) -> None:
        if self.lock.locked():
            raise RuntimeError("Concurrent requests!")


@pytest.mark.asyncio
async def test_is_async_default():
    fs = fsspec.filesystem("file")
    async_fs = AsyncFileSystemWrapper(fs)
    assert async_fs.async_impl
    assert async_fs.asynchronous
    async_fs = AsyncFileSystemWrapper(fs, asynchronous=False)
    assert not async_fs.asynchronous


def test_class_wrapper():
    fs_cls = LocalFileSystem
    async_fs_cls = AsyncFileSystemWrapper.wrap_class(fs_cls)
    assert async_fs_cls.__name__ == "AsyncLocalFileSystemWrapper"
    async_fs = async_fs_cls()
    assert async_fs.async_impl


@pytest.mark.asyncio
async def test_cats():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        result = await async_fs._cat(".test.fakedata.1.csv")
        assert result == b"a,b\n1,2\n"

        out = set(
            (
                await async_fs._cat([".test.fakedata.1.csv", ".test.fakedata.2.csv"])
            ).values()
        )
        assert out == {b"a,b\n1,2\n", b"a,b\n3,4\n"}

        result = await async_fs._cat(".test.fakedata.1.csv", None, None)
        assert result == b"a,b\n1,2\n"

        result = await async_fs._cat(".test.fakedata.1.csv", start=1, end=6)
        assert result == b"a,b\n1,2\n"[1:6]

        result = await async_fs._cat(".test.fakedata.1.csv", start=-1)
        assert result == b"a,b\n1,2\n"[-1:]

        result = await async_fs._cat(".test.fakedata.1.csv", start=1, end=-2)
        assert result == b"a,b\n1,2\n"[1:-2]

        # test synchronous API is available as expected
        async_fs = AsyncFileSystemWrapper(fs, asynchronous=False)
        result = async_fs.cat(".test.fakedata.1.csv", start=1, end=-2)
        assert result == b"a,b\n1,2\n"[1:-2]

        out = set(
            (
                await async_fs._cat(
                    [".test.fakedata.1.csv", ".test.fakedata.2.csv"], start=1, end=-1
                )
            ).values()
        )
        assert out == {b"a,b\n1,2\n"[1:-1], b"a,b\n3,4\n"[1:-1]}


@pytest.mark.asyncio
async def test_basic_crud_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        await async_fs._touch(".test.fakedata.3.csv")
        assert await async_fs._exists(".test.fakedata.3.csv")

        data = await async_fs._cat(".test.fakedata.1.csv")
        assert data == b"a,b\n1,2\n"

        await async_fs._pipe(".test.fakedata.1.csv", b"a,b\n5,6\n")
        data = await async_fs._cat(".test.fakedata.1.csv")
        assert data == b"a,b\n5,6\n"

        await async_fs._rm(".test.fakedata.1.csv")
        assert not await async_fs._exists(".test.fakedata.1.csv")


@pytest.mark.asyncio
async def test_error_handling():
    fs = fsspec.filesystem("file")
    async_fs = AsyncFileSystemWrapper(fs)

    with pytest.raises(FileNotFoundError):
        await async_fs._cat(".test.non_existent.csv")

    with pytest.raises(FileNotFoundError):
        await async_fs._rm(".test.non_existent.csv")


@pytest.mark.asyncio
async def test_concurrent_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        async def read_file(file_path):
            return await async_fs._cat(file_path)

        results = await asyncio.gather(
            read_file(".test.fakedata.1.csv"),
            read_file(".test.fakedata.2.csv"),
            read_file(".test.fakedata.1.csv"),
        )

        assert results == [b"a,b\n1,2\n", b"a,b\n3,4\n", b"a,b\n1,2\n"]


@pytest.mark.asyncio
async def test_directory_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        await async_fs._makedirs("new_directory")
        assert await async_fs._isdir("new_directory")

        files = await async_fs._ls(".")
        filenames = [os.path.basename(file) for file in files]

        assert ".test.fakedata.1.csv" in filenames
        assert ".test.fakedata.2.csv" in filenames
        assert "new_directory" in filenames


@pytest.mark.asyncio
async def test_batch_operations():
    with filetexts(csv_files, mode="b"):
        fs = fsspec.filesystem("file")
        async_fs = AsyncFileSystemWrapper(fs)

        await async_fs._rm([".test.fakedata.1.csv", ".test.fakedata.2.csv"])
        assert not await async_fs._exists(".test.fakedata.1.csv")
        assert not await async_fs._exists(".test.fakedata.2.csv")


def test_open(tmpdir):
    fn = f"{tmpdir}/afile"
    with open(fn, "wb") as f:
        f.write(b"hello")
    of = fsspec.open(
        "dir://afile::async_wrapper::file",
        mode="rb",
        async_wrapper={"asynchronous": False},
        dir={"path": str(tmpdir)},
    )
    with of as f:
        assert f.read() == b"hello"


@pytest.mark.asyncio
async def test_semaphore_synchronous():
    fs = AsyncFileSystemWrapper(
        LockedFileSystem(), asynchronous=False, semaphore=asyncio.Semaphore(1)
    )

    paths = [f"path_{i}" for i in range(1, 3)]
    results = await asyncio.gather(*(fs._cat_file(path) for path in paths))

    assert set(results) == {path.encode() for path in paths}


@pytest.mark.asyncio
async def test_deadlock_when_asynchronous():
    fs = AsyncFileSystemWrapper(
        LockedFileSystem(), asynchronous=False, semaphore=asyncio.Semaphore(3)
    )
    paths = [f"path_{i}" for i in range(1, 3)]

    with pytest.raises(RuntimeError, match="Concurrent requests!"):
        await asyncio.gather(*(fs._cat_file(path) for path in paths))
