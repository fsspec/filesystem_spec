import inspect

from .asyn import AsyncFileSystem
from .callbacks import _DEFAULT_CALLBACK
from .core import filesystem, get_filesystem_class, split_protocol

_generic_fs = {}


def set_generic_fs(protocol, **storage_options):
    _generic_fs[protocol] = filesystem(protocol, **storage_options)


default_method = "default"


def _resolve_fs(url, method=None, protocol=None, storage_options=None):
    """Pick instance of backend FS"""
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
    """Wrapper over all other FS types

    <experimental!>

    This implementation is a single unified interface to be able to run FS operations
    over generic URLs, and dispatch to the specific implementations using the URL
    protocol prefix.

    Note: instances of this FS are always async, even if you never use it with any async
    backend.
    """

    protocol = "generic"  # there is no real reason to ever use a protocol with this FS

    def __init__(self, default_method=None, **kwargs):
        """

        Parameters
        ----------
        default_method: str (optional)
            Defines how to configure backend FS instances. Options are:
            - "default" (you get this with None): instantiate like FSClass(), with no
              extra arguments; this is the default instance of that FS, and can be
              configured via the config system
            - "generic": takes instances from the `_generic_fs` dict in this module,
              which you must populate before use. Keys are by protocol
            - "current": takes the most recently instantiated version of each FS
            - "options": expect ``storage_options`` to be passed along with every call.
        """
        self.method = default_method
        super(GenericFileSystem, self).__init__(**kwargs)

    async def _info(
        self, url, method=None, protocol=None, storage_options=None, fs=None, **kwargs
    ):
        fs = fs or _resolve_fs(url, method or self.method, protocol, storage_options)
        if fs.async_impl:
            out = await fs._info(url, **kwargs)
        else:
            out = fs.info(url, **kwargs)
        out["name"] = fs.unstrip_protocol(out["name"])
        return out

    async def _ls(
        self,
        url,
        method=None,
        protocol=None,
        storage_options=None,
        fs=None,
        detail=True,
        **kwargs,
    ):
        fs = fs or _resolve_fs(url, method or self.method, protocol, storage_options)
        if fs.async_impl:
            out = await fs._ls(url, detail=True, **kwargs)
        else:
            out = fs.ls(url, detail=True, **kwargs)
        for o in out:
            o["name"] = fs.unstrip_protocol(o["name"])
        if detail:
            return out
        else:
            return [o["name"] for o in out]

    async def _cat_file(
        self,
        url,
        method=None,
        protocol=None,
        storage_options=None,
        fs=None,
        **kwargs,
    ):
        fs = fs or _resolve_fs(url, method or self.method, protocol, storage_options)
        if fs.async_impl:
            return await fs._cat_file(url, **kwargs)
        else:
            return fs.cat_file(url, **kwargs)

    async def _pipe_file(
        self,
        path,
        value,
        method=None,
        protocol=None,
        storage_options=None,
        fs=None,
        **kwargs,
    ):
        fs = fs or _resolve_fs(path, method or self.method, protocol, storage_options)
        if fs.async_impl:
            return await fs._pipe_file(path, value, **kwargs)
        else:
            return fs.pipe_file(path, value, **kwargs)

    async def _rm(
        self, url, method=None, protocol=None, storage_options=None, fs=None, **kwargs
    ):
        fs = fs or _resolve_fs(url, method or self.method, protocol, storage_options)
        if fs.async_impl:
            await fs._rm(url, **kwargs)
        else:
            fs.rm(url, **kwargs)

    async def _cp_file(
        self,
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
        blocksize=2**20,
        callback=_DEFAULT_CALLBACK,
        **kwargs,
    ):
        fs = fs or _resolve_fs(url, method or self.method, protocol, storage_options)
        fs2 = fs2 or _resolve_fs(
            url2, method2 or self.method, protocol2, storage_options2
        )
        if fs is fs2:
            # pure remote
            if fs.async_impl:
                return await fs._cp_file(url, url2, **kwargs)
            else:
                return fs.cp_file(url, url2, **kwargs)
        kw = {"blocksize": 0, "cache_type": "none"}
        try:
            f1 = (
                await fs.open_async(url, "rb")
                if hasattr(fs, "open_async")
                else fs.open(url, "rb", **kw)
            )
            callback.set_size(await maybe_await(f1.size))
            f2 = (
                await fs2.open_async(url2, "wb")
                if hasattr(fs2, "open_async")
                else fs2.open(url2, "wb", **kw)
            )
            while f1.size is None or f2.tell() < f1.size:
                data = await maybe_await(f1.read(blocksize))
                if f1.size is None and not data:
                    break
                await maybe_await(f2.write(data))
                callback.absolute_update(f2.tell())
        finally:
            try:
                await maybe_await(f2.close())
                await maybe_await(f1.close())
            except NameError:
                # fail while opening f1 or f2
                pass


async def maybe_await(cor):
    if inspect.iscoroutine(cor):
        return await cor
    else:
        return cor
