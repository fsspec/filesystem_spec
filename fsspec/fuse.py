from __future__ import print_function

import logging
import os
import stat
import threading
import time
from errno import EIO, ENOENT

import click
from fuse import FUSE, FuseOSError, LoggingMixIn, Operations

from fsspec._version import get_versions
from fsspec.core import url_to_fs

logger = logging.getLogger("fsspec.fuse")


class FUSEr(Operations):
    def __init__(self, fs, path, ready_file=False):
        self.fs = fs
        self.cache = {}
        self.root = path.rstrip("/") + "/"
        self.counter = 0
        logger.info("Starting FUSE at %s", path)
        self._ready_file = ready_file

    def getattr(self, path, fh=None):
        logger.debug("getattr %s", path)
        if self._ready_file and path in ["/.fuse_ready", ".fuse_ready"]:
            return {"type": "file", "st_size": 5}

        path = "".join([self.root, path.lstrip("/")]).rstrip("/")
        try:
            info = self.fs.info(path)
        except FileNotFoundError:
            raise FuseOSError(ENOENT)

        data = {"st_uid": info.get("uid", 1000), "st_gid": info.get("gid", 1000)}
        perm = info.get("mode", 0o777)

        if info["type"] != "file":
            data["st_mode"] = stat.S_IFDIR | perm
            data["st_size"] = 0
            data["st_blksize"] = 0
        else:
            data["st_mode"] = stat.S_IFREG | perm
            data["st_size"] = info["size"]
            data["st_blksize"] = 5 * 2 ** 20
            data["st_nlink"] = 1
        data["st_atime"] = time.time()
        data["st_ctime"] = time.time()
        data["st_mtime"] = time.time()
        return data

    def readdir(self, path, fh):
        logger.debug("readdir %s", path)
        path = "".join([self.root, path.lstrip("/")])
        files = self.fs.ls(path, False)
        files = [os.path.basename(f.rstrip("/")) for f in files]
        return [".", ".."] + files

    def mkdir(self, path, mode):
        path = "".join([self.root, path.lstrip("/")])
        self.fs.mkdir(path)
        return 0

    def rmdir(self, path):
        path = "".join([self.root, path.lstrip("/")])
        self.fs.rmdir(path)
        return 0

    def read(self, path, size, offset, fh):
        logger.debug("read %s", (path, size, offset))
        if self._ready_file and path in ["/.fuse_ready", ".fuse_ready"]:
            return b"ready"

        f = self.cache[fh]
        f.seek(offset)
        out = f.read(size)
        return out

    def write(self, path, data, offset, fh):
        logger.debug("read %s", (path, offset))
        f = self.cache[fh]
        f.write(data)
        return len(data)

    def create(self, path, flags, fi=None):
        logger.debug("create %s", (path, flags))
        fn = "".join([self.root, path.lstrip("/")])
        f = self.fs.open(fn, "wb")
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1

    def open(self, path, flags):
        logger.debug("open %s", (path, flags))
        fn = "".join([self.root, path.lstrip("/")])
        if flags % 2 == 0:
            # read
            mode = "rb"
        else:
            # write/create
            mode = "wb"
        self.cache[self.counter] = self.fs.open(fn, mode)
        self.counter += 1
        return self.counter - 1

    def truncate(self, path, length, fh=None):
        fn = "".join([self.root, path.lstrip("/")])
        if length != 0:
            raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.fs.touch(fn)

    def unlink(self, path):
        fn = "".join([self.root, path.lstrip("/")])
        try:
            self.fs.rm(fn, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)

    def release(self, path, fh):
        try:
            if fh in self.cache:
                f = self.cache[fh]
                f.close()
                self.cache.pop(fh)
        except Exception as e:
            print(e)
        return 0

    def chmod(self, path, mode):
        if hasattr(self.fs, "chmod"):
            path = "".join([self.root, path.lstrip("/")])
            return self.fs.chmod(path, mode)
        raise NotImplementedError


def run(
    fs,
    path,
    mount_point,
    foreground=True,
    threads=False,
    log_filename="",
    ready_file=False,
):
    """Mount stuff in a local directory

    This uses fusepy to make it appear as if a given path on an fsspec
    instance is in fact resident within the local file-system.

    This requires that fusepy by installed, and that FUSE be available on
    the system (typically requiring a package to be installed with
    apt, yum, brew, etc.).

    Parameters
    ----------
    fs: file-system instance
        From one of the compatible implementations
    path: str
        Location on that file-system to regard as the root directory to
        mount. Note that you typically should include the terminating "/"
        character.
    mount_point: str
        An empty directory on the local file-system where the contents of
        the remote path will appear.
    foreground: bool
        Whether or not calling this function will block. Operation will
        typically be more stable if True.
    threads: bool
        Whether or not to create threads when responding to file operations
        within the mounter directory. Operation will typically be more
        stable if False.
    log_filename: str
        The FUSE log file name. If not provided, the logging feature is
        disabled.
    ready_file: bool
        Whether the FUSE process is ready. The `.fuse_ready` file will
        exist in the `mount_point` directory if True. Debugging purpose.

    """
    if log_filename:
        logging.basicConfig(
            level=logging.DEBUG,
            filename=log_filename,
            format="%(asctime)s %(message)s",
        )

        class LoggingFUSEr(FUSEr, LoggingMixIn):
            pass

        fuser = LoggingFUSEr
    else:
        fuser = FUSEr

    func = lambda: FUSE(
        fuser(fs, path, ready_file=ready_file),
        mount_point,
        nothreads=not threads,
        foreground=foreground,
    )
    if not foreground:
        th = threading.Thread(target=func)
        th.daemon = True
        th.start()
        return th
    else:  # pragma: no cover
        try:
            func()
        except KeyboardInterrupt:
            pass


@click.command()
@click.version_option(
    version=get_versions()["version"], message="fsspec mount %(version)s"
)
@click.help_option("-h", "--help")
@click.argument("url", type=click.STRING, nargs=1)
@click.argument("source_path", type=click.STRING, nargs=1)
@click.argument(
    "mount_point",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    nargs=1,
)
@click.option(
    "-l",
    "--log-file",
    type=click.Path(),
    default="",
    show_default=True,
    help="Enable FUSE debug logging.",
)
@click.option(
    "-f",
    "--foreground",
    default=True,
    show_default=True,
    help="Running in foreground or not.",
)
@click.option(
    "-t",
    "--threads",
    default=False,
    show_default=True,
    help="Running with threads support.",
)
@click.option(
    "-o",
    "--option",
    multiple=True,
    help="Any options of protocol included in the chained URL.",
)
@click.option(
    "--ready-file/--no-ready-file",
    "-r",
    default=False,
    show_default=True,
    help="The `.fuse_ready` file will exist after FUSE is ready.",
)
def mount(
    url,
    source_path,
    mount_point,
    log_file,
    option,
    foreground=True,
    threads=False,
    ready_file=False,
):
    """Mount filesystem from chained URL to MOUNT_POINT.

    Examples:

    \b
    python3 -m fsspec.fuse memory /usr/share /tmp/mem

    \b
    python3 -m fsspec.fuse local /tmp/source /tmp/local \\
            -l /tmp/fsspecfuse.log

    You can also mount chained-URLs and use special settings:

    \b
    python3 -m fsspec.fuse 'filecache::zip::file://data.zip' \\
            / /tmp/zip \\
            -o 'filecache-cache_storage=/tmp/simplecache'

    You can specify the type of the setting by using `[int]` or `[bool]`:

    \b
    python3 -m fsspec.fuse 'simplecache::ftp://ftp1.at.proftpd.org' \\
            /historic/packages/RPMS /tmp/ftp \\
            -o 'simplecache-cache_storage=/tmp/simplecache' \\
            -o 'simplecache-check_files=false[bool]' \\
            -o 'ftp-listings_expiry_time=60[int]' \\
            -o 'ftp-username=anonymous' \\
            -o 'ftp-password=xieyanbo'

    """
    kwargs = {}
    for item in option:
        key, value = item.split("=", 1)
        if value.lower().endswith("[int]"):
            value = int(value[: -len("[int]")])
        elif value.lower().endswith("[bool]"):
            value = value[: -len("[bool]")].lower() in ["1", "yes", "true"]

        if "-" in key:
            fs_name, setting_name = key.split("-", 1)
            if fs_name in kwargs:
                kwargs[fs_name][setting_name] = value
            else:
                kwargs[fs_name] = {setting_name: value}
        else:
            kwargs[key] = value

    fs, url_path = url_to_fs(url, **kwargs)
    logger.debug("Mounting %s to %s", url_path, mount_point)
    run(fs, source_path, mount_point, log_filename=log_file, ready_file=ready_file)


if __name__ == "__main__":
    mount()
