import base64
import io
import itertools
import json
import logging

from ..asyn import AsyncFileSystem, sync
from ..core import filesystem, open
from ..spec import AbstractFileSystem

logger = logging.getLogger("fsspec.reference")


class ReferenceFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system

    Initial version: single file system target, which must support
    async, and must allow start and end args in _cat_file. Later versions
    may allow multiple arbitrary URLs for the targets.

    This FileSystem is read-only. It is designed to be used with async
    targets (for now). This FileSystem only allows whole-file access, no
    ``open``. We do not get original file details from the target FS.

    Configuration is by passing a dict of references at init, or a URL to
    a JSON file containing the same; this dict
    can also contain concrete data for some set of paths.

    Reference dict format:
    {path0: bytes_data, path1: (target_url, offset, size)}

    https://github.com/intake/fsspec-reference-maker/blob/main/README.md
    """

    protocol = "reference"

    def __init__(
        self,
        fo,
        target=None,
        ref_storage_args=None,
        target_protocol=None,
        target_options=None,
        remote_protocol=None,
        remote_options=None,
        fs=None,
        template_overrides=None,
        loop=None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        fo : dict or str
            The set of references to use for this instance, with a structure as above.
            If str, will use fsspec.open, in conjunction with ref_storage_args to
            open and parse JSON at this location.
        target : str
            For any references having target_url as None, this is the default file
            target to use
        ref_storage_args : dict
            If references is a str, use these kwargs for loading the JSON file
        target_protocol : str
            Used for loading the reference file, if it is a path. If None, protocol
            will be derived from the given path
        target_options : dict
            Extra FS options for loading the reference file, if given as a path
        remote_protocol : str
            The protocol of the filesystem on which the references will be evaluated
            (unless fs is provided)
        remote_options : dict
            kwargs to go with remote_protocol
        fs : file system instance
            Directly provide a file system, if you want to configure it beforehand. This
            takes precedence over target_protocol/target_options
        template_overrides : dict
            Swap out any templates in the references file with these - useful for
            testing.
        kwargs : passed to parent class
        """
        super().__init__(loop=loop, **kwargs)
        if isinstance(fo, str):
            if target_protocol:
                extra = {"protocol": target_protocol}
            else:
                extra = {}
            with open(
                fo, "rb", **(ref_storage_args or target_options or {}), **extra
            ) as f:
                logger.info("Read reference from URL %s", fo)
                text = f.read()
        else:
            text = fo
        if fs is None and remote_protocol is None:
            remote_protocol = target_protocol
        if remote_protocol:
            fs = filesystem(remote_protocol, loop=loop, **(remote_options or {}))
        self.target = target
        self._process_references(text, template_overrides)
        self.fs = fs

    @property
    def loop(self):
        return self.fs.loop if self.fs.async_impl else self._loop

    def _cat_common(self, path):
        path = self._strip_protocol(path)
        part = self.references[path]
        if isinstance(part, str):
            part = part.encode()
        if isinstance(part, bytes):
            logger.debug(f"Reference: {path}, type bytes")
            if part.startswith(b"base64:"):
                part = base64.b64decode(part[7:])
            return part, None, None

        if len(part) == 1:
            logger.debug(f"Reference: {path}, whole file")
            url = part[0]
            start = None
            end = None
        else:
            url, start, size = part
            logger.debug(f"Reference: {path}, offset {start}, size {size}")
            end = start + size
        if url is None:
            url = self.target
        return url, start, end

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        return (await self.fs._cat_file(part_or_url, start=start0, end=end0))[start:end]

    def cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        return self.fs.cat_file(part_or_url, start=start0, end=end0)[start:end]

    async def _get_file(self, rpath, lpath, **kwargs):
        data = await self._cat_file(rpath)
        with open(lpath, "wb") as f:
            f.write(data)

    def get_file(self, rpath, lpath, **kwargs):
        data = self.cat_file(rpath, **kwargs)
        with open(lpath, "wb") as f:
            f.write(data)

    def get(self, rpath, lpath, recursive=False, **kwargs):
        if self.fs.async_impl:
            return sync(self.loop, self._get, rpath, lpath, recursive, **kwargs)
        return AbstractFileSystem.get(rpath, lpath, recursive=recursive, **kwargs)

    def cat(self, path, recursive=False, **kwargs):
        if self.fs.async_impl:
            return sync(self.loop, self._cat, path, recursive, **kwargs)
        elif isinstance(path, list):
            if recursive or any("*" in p for p in path):
                raise NotImplementedError
            return {p: AbstractFileSystem.cat_file(self, p, **kwargs) for p in path}
        else:
            return AbstractFileSystem.cat_file(self, path)

    def _process_references(self, references, template_overrides=None):
        if isinstance(references, bytes):
            references = json.loads(references.decode())
        vers = references.get("version", None)
        if vers is None:
            self._process_references0(references)
        elif vers == 1:
            self._process_references1(references, template_overrides=template_overrides)
        else:
            raise ValueError(f"Unknown reference spec version: {vers}")
        # TODO: we make dircache by iteraring over all entries, but for Spec >= 1,
        # can replace with programmatic. Is it even needed for mapper interface?
        self._dircache_from_items()

    def _process_references0(self, references):
        """Make reference dict for Spec Version 0"""
        if "zarr_consolidated_format" in references:
            # special case for Ike prototype
            references = _unmodel_hdf5(references)
        self.references = references

    def _process_references1(self, references, template_overrides=None):
        try:
            import jinja2
        except ImportError as e:
            raise ValueError("Reference Spec Version 1 requires jinja2") from e
        self.references = {}
        templates = {}
        tmp = references.get("templates", {})
        if template_overrides is not None:
            tmp.update(template_overrides)
        for k, v in tmp.items():
            if "{{" in v:
                templates[k] = lambda temp=v, **kwargs: jinja2.Template(temp).render(
                    **kwargs
                )
            else:
                templates[k] = v

        for k, v in references.get("refs", {}).items():
            if isinstance(v, str):
                if v.startswith("base64:"):
                    self.references[k] = base64.b64decode(v[7:])
                self.references[k] = v
            else:
                u = v[0]
                if "{{" in u:
                    u = jinja2.Template(u).render(**templates)
                self.references[k] = [u] if len(v) == 1 else [u, v[1], v[2]]
        for gen in references.get("gen", []):
            dimension = {
                k: v
                if isinstance(v, list)
                else range(v.get("start", 0), v["stop"], v.get("step", 1))
                for k, v in gen["dimensions"].items()
            }
            products = (
                dict(zip(dimension.keys(), values))
                for values in itertools.product(*dimension.values())
            )
            for pr in products:
                key = jinja2.Template(gen["key"]).render(**pr, **templates)
                url = jinja2.Template(gen["url"]).render(**pr, **templates)
                if ("offset" in gen) and ("length" in gen):
                    offset = int(
                        jinja2.Template(gen["offset"]).render(**pr, **templates)
                    )
                    length = int(
                        jinja2.Template(gen["length"]).render(**pr, **templates)
                    )
                    self.references[key] = [url, offset, length]
                elif ("offset" in gen) ^ ("length" in gen):
                    raise ValueError(
                        "Both 'offset' and 'length' are required for a "
                        "reference generator entry if either is provided."
                    )
                else:
                    self.references[key] = [url]

    def _dircache_from_items(self):
        self.dircache = {"": []}
        for path, part in self.references.items():
            if isinstance(part, (bytes, str)):
                size = len(part)
            elif len(part) == 1:
                size = None
            else:
                _, start, size = part
            par = self._parent(path)
            par0 = par
            while par0:
                # build parent directories
                if par0 not in self.dircache:
                    self.dircache[par0] = []
                    self.dircache.setdefault(self._parent(par0), []).append(
                        {"name": par0, "type": "directory", "size": 0}
                    )
                par0 = self._parent(par0)

            self.dircache[par].append({"name": path, "type": "file", "size": size})

    def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        if mode != "rb":
            raise NotImplementedError
        data = self.cat_file(path)  # load whole chunk into memory
        return io.BytesIO(data)

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        out = self._ls_from_cache(path)
        if out is None:
            raise FileNotFoundError
        if detail:
            return out
        return [o["name"] for o in out]

    def exists(self, path, **kwargs):  # overwrite auto-sync version
        try:
            return self._ls_from_cache(path) is not None
        except FileNotFoundError:
            return False

    def isdir(self, path):  # overwrite auto-sync version
        return self.exists(path) and self.info(path)["type"] == "directory"

    def isfile(self, path):  # overwrite auto-sync version
        return self.exists(path) and self.info(path)["type"] == "file"

    async def _ls(self, path, detail=True, **kwargs):  # calls fast sync code
        return self.ls(path, detail, **kwargs)

    def info(self, path, **kwargs):
        out = self.ls(path, True)
        out0 = [o for o in out if o["name"] == path]
        if not out0:
            return {"name": path, "type": "directory", "size": 0}
        return out0[0]

    async def _info(self, path, **kwargs):  # calls fast sync code
        return self.info(path)


def _unmodel_hdf5(references):
    """Special JSON format from HDF5 prototype"""
    # see https://gist.github.com/ajelenak/80354a95b449cedea5cca508004f97a9
    import re

    ref = {}
    for key, value in references["metadata"].items():
        if key.endswith(".zchunkstore"):
            source = value.pop("source")["uri"]
            match = re.findall(r"https://([^.]+)\.s3\.amazonaws\.com", source)
            if match:
                source = source.replace(
                    f"https://{match[0]}.s3.amazonaws.com", match[0]
                )
            for k, v in value.items():
                ref[k] = (source, v["offset"], v["offset"] + v["size"])
        else:
            ref[key] = json.dumps(value).encode()
    return ref
