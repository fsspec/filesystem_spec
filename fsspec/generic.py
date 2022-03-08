import inspect

from .asyn import AsyncFileSystem
from .callbacks import _DEFAULT_CALLBACK
from .core import filesystem, get_filesystem_class, split_protocol

_generic_fs = {}


def set_generic_fs(protocol, **storage_options):
    _generic_fs[protocol] = filesystem(protocol ** storage_options)


default_method = "default"


def _resolve_fs(url, method=None, protocol=None, storage_options=None):
    method = method or default_method
    protocol = protocol or split_protocol(url)[0]
    storage_options = storage_options or {}
    if method == "default":
        return filesystem(protocol)
    if method == "generic":
        return _generic_fs[protocol]
    if method == "current":
        cls = get_filesystem_class(protocol)
        return cls.current()
    if method == "options":
        return filesystem(protocol, **storage_options.get(protocol, {}))
    raise ValueError(f"Unknown FS resolution method: {method}")


class GenericFileSystem(AsyncFileSystem):
    @staticmethod
    async def _info(
        url, method=None, protocol=None, storage_options=None, fs=None, **kwargs
    ):
        fs = fs or _resolve_fs(url, method, protocol, storage_options)
        if fs.async_impl:
            out = await fs._info(url, **kwargs)
        else:
            out = fs.info(url, **kwargs)
        out["name"] = fs._unstrip_protocol(out["name"])
        return out

    @staticmethod
    async def _ls(
        url,
        method=None,
        protocol=None,
        storage_options=None,
        fs=None,
        detail=True,
        **kwargs,
    ):
        fs = fs or _resolve_fs(url, method, protocol, storage_options)
        if fs.async_impl:
            out = await fs._ls(url, detail=True, **kwargs)
        else:
            out = fs.ls(url, detail=True, **kwargs)
        for o in out:
            o["name"] = fs._unstrip_protocol(o["name"])
        if detail:
            return out
        else:
            return [o["name"] for o in out]

    @staticmethod
    async def _cp_file(
        url,
        url2,
        method=None,
        protocol=None,
        storage_options=None,
        fs=None,
        method2=None,
        protocol2=None,
        storage_options2=None,
        fs2=None,
        blocksize=2 ** 20,
        callback=_DEFAULT_CALLBACK,
        **kwargs,
    ):
        fs = fs or _resolve_fs(url, method, protocol, storage_options)
        fs2 = fs2 or _resolve_fs(url2, method2, protocol2, storage_options2)
        if fs is fs2:
            # pure remote
            if fs.async_impl:
                return await fs._cp_file(url, url2, **kwargs)
            else:
                return fs.cp_file(url, url2, **kwargs)
        kw = {"blocksize": 0, "cache_type": "none"}
        try:
            f1 = (
                fs.open_async(url, "rb")
                if hasattr(fs, "open_async")
                else fs.open(url, "rb", **kw)
            )
            callback.set_size(f1.size)
            f2 = (
                fs2.open_async(url2, "wb")
                if hasattr(fs, "open_async")
                else fs2.open(url2, "wb", **kw)
            )
            while f2.tell() < f1.size:
                data = await maybe_await(f1.read(blocksize))
                await maybe_await(f2.write(data))
                callback.absolute_update(f2.tell())
        finally:
            try:
                await maybe_await(f2.close())
            except NameError:
                # fail while opening f1 or f2
                pass


async def maybe_await(cor):
    if inspect.iscoroutine(cor):
        return await cor
    else:
        return cor
