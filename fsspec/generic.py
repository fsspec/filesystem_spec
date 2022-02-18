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

        if not fs.async_impl and not fs2.async_impl:

            with fs.open(url, "rb", **kw) as f1, fs2.open(url2, "rb", **kw) as f2:
                callback.set_size(f1.size)
                while True:
                    data = f1.read(blocksize)
                    if not data:
                        # TODO:
                        return
                    f2.write(data)
                    callback.relative_update(len(data))

        if fs.async_impl and fs2.async_impl:

            with fs.open(url, "rb", **kw) as f1, fs2.open(url2, "rb", **kw) as f2:
                callback.set_size(f1.size)
                while True:
                    data = f1.read(blocksize)
                    if not data:
                        # TODO:
                        return
                    f2.write(data)
                    callback.relative_update(len(data))
