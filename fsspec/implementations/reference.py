import base64
import io
import itertools
import logging
import os
from functools import lru_cache

import fsspec.core

try:
    import ujson as json
except ImportError:
    import json

from ..asyn import AsyncFileSystem, sync
from ..callbacks import _DEFAULT_CALLBACK
from ..core import filesystem, open, split_protocol
from ..spec import AbstractFileSystem
from ..utils import isfilelike

logger = logging.getLogger("fsspec.reference")


def _first(d):
    return list(d.values())[0]


def _prot_in_references(path, references):
    ref = references.get(path)
    if isinstance(ref, (list, tuple)):
        return split_protocol(ref[0])[0] if ref[0] else ref[0]


def _protocol_groups(paths, references):
    if isinstance(paths, str):
        return {_prot_in_references(paths, references): [paths]}
    out = {}
    for path in paths:
        protocol = _prot_in_references(path, references)
        out.setdefault(protocol, []).append(path)
    return out


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

    https://github.com/fsspec/kerchunk/blob/main/README.md
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
        simple_templates=True,
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
            (unless fs is provided). If not given, will be derived from the first
            URL that has a protocol in the templates or in the references, in that
            order.
        remote_options : dict
            kwargs to go with remote_protocol
        fs : AbstractFileSystem | dict(str, (AbstractFileSystem | dict))
            Directly provide a file system(s):
                - a single filesystem instance
                - a dict of protocol:filesystem, where each value is either a filesystem
                  instance, or a dict of kwargs that can be used to create in
                  instance for the given protocol

            If this is given, remote_options and remote_protocol are ignored.
        template_overrides : dict
            Swap out any templates in the references file with these - useful for
            testing.
        simple_templates: bool
            Whether templates can be processed with simple replace (True) or if
            jinja  is needed (False, much slower). All reference sets produced by
            ``kerchunk`` are simple in this sense, but the spec allows for complex.
        kwargs : passed to parent class
        """
        super().__init__(loop=loop, **kwargs)
        self.target = target
        self.dataframe = False
        self.template_overrides = template_overrides
        self.simple_templates = simple_templates
        self.templates = {}
        self.fss = {}
        if hasattr(fo, "read"):
            text = fo.read()
        elif isinstance(fo, str):
            if target_protocol:
                extra = {"protocol": target_protocol}
            else:
                extra = {}
            dic = dict(**(ref_storage_args or target_options or {}), **extra)
            # text JSON
            with open(fo, "rb", **dic) as f:
                logger.info("Read reference from URL %s", fo)
                text = f.read()
        else:
            # dictionaries
            text = fo
        if self.dataframe:
            self._process_dataframe()
        else:
            self._process_references(text, template_overrides)
        if isinstance(fs, dict):
            self.fss = {
                k: (
                    fsspec.filesystem(k.split(":", 1)[0], **opts)
                    if isinstance(opts, dict)
                    else opts
                )
                for k, opts in fs.items()
            }
            return
        if fs is not None:
            # single remote FS
            remote_protocol = (
                fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
            )

        if remote_protocol is None:
            # get single protocol from any templates
            for ref in self.templates.values():
                if callable(ref):
                    ref = ref()
                protocol, _ = fsspec.core.split_protocol(ref)
                if protocol:
                    remote_protocol = protocol
                    break
        if remote_protocol is None:
            # get single protocol from references
            for ref in self.references.values():
                if callable(ref):
                    ref = ref()
                if isinstance(ref, list) and ref[0]:
                    protocol, _ = fsspec.core.split_protocol(ref[0])
                    if protocol:
                        remote_protocol = protocol
                        break
        if remote_protocol is None:
            remote_protocol = target_protocol

        fs = fs or filesystem(remote_protocol, loop=loop, **(remote_options or {}))
        self.fss[remote_protocol] = fs
        self.fss[None] = fs  # default one

    @property
    def loop(self):
        inloop = [fs.loop for fs in self.fss.values() if fs.async_impl]
        return inloop[0] if inloop else self._loop

    def _cat_common(self, path):
        path = self._strip_protocol(path)
        logger.debug(f"cat: {path}")
        # TODO: can extract and cache templating here
        if self.dataframe:
            part = self.df.loc[path]
            if part["data"]:
                part = part["data"]
            else:
                part = part[["url", "offset", "size"]]
        else:
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
        protocol, _ = split_protocol(part_or_url)
        # TODO: start and end should be passed to cat_file, not sliced
        return (
            await self.fss[protocol]._cat_file(part_or_url, start=start0, end=end0)
        )[start:end]

    def cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        # TODO: start and end should be passed to cat_file, not sliced
        return self.fss[protocol].cat_file(part_or_url, start=start0, end=end0)[
            start:end
        ]

    def pipe_file(self, path, value, **_):
        """Temporarily add binary data or reference as a file"""
        self.references[path] = value

    async def _get_file(self, rpath, lpath, **kwargs):
        if self.isdir(rpath):
            return os.makedirs(lpath, exist_ok=True)
        data = await self._cat_file(rpath)
        with open(lpath, "wb") as f:
            f.write(data)

    def get_file(self, rpath, lpath, callback=_DEFAULT_CALLBACK, **kwargs):
        if self.isdir(rpath):
            return os.makedirs(lpath, exist_ok=True)
        data = self.cat_file(rpath, **kwargs)
        callback.set_size(len(data))
        if isfilelike(lpath):
            lpath.write(data)
        else:
            with open(lpath, "wb") as f:
                f.write(data)
        callback.absolute_update(len(data))

    def get(self, rpath, lpath, recursive=False, **kwargs):
        if isinstance(lpath, list):
            # because we have to figure out here which lpath goes with which path
            # after grouping
            raise NotImplementedError
        proto_dict = _protocol_groups(rpath, self.references)
        for proto, paths in proto_dict.items():
            if self.fss[proto].async_impl:
                sync(self.loop, self._get, paths, lpath, recursive, **kwargs)
            else:
                AbstractFileSystem.get(
                    self, paths, lpath, recursive=recursive, **kwargs
                )

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        proto_dict = _protocol_groups(path, self.references)
        out = {}
        for proto, paths in proto_dict.items():
            if proto is None:
                # binary/string
                for p in paths:
                    try:
                        out[p] = AbstractFileSystem.cat_file(self, p, **kwargs)
                    except Exception as e:
                        if on_error == "raise":
                            raise
                        if on_error == "return":
                            out[p] = e

            elif self.fss[proto].async_impl:
                # TODO: asyncio.gather on multiple async FSs
                out.update(
                    sync(
                        self.loop,
                        self._cat,
                        paths,
                        recursive,
                        on_error=on_error,
                        **kwargs,
                    )
                )
            elif isinstance(paths, list):
                if recursive or any("*" in p for p in paths):
                    raise NotImplementedError
                for p in paths:
                    try:
                        out[p] = AbstractFileSystem.cat_file(self, p, **kwargs)
                    except Exception as e:
                        if on_error == "raise":
                            raise
                        if on_error == "return":
                            out[p] = e
            else:
                out.update(AbstractFileSystem.cat_file(self, paths))
        if len(out) == 1 and isinstance(path, str) and "*" not in path:
            return _first(out)
        return out

    def _process_dataframe(self):
        self._process_templates(self.templates)

        @lru_cache(1000)
        def _render_jinja(url):
            import jinja2

            if "{{" in url:
                if self.simple_templates:
                    return (
                        url.replace("{{", "{")
                        .replace("}}", "}")
                        .format(**self.templates)
                    )

                return jinja2.Template(url).render(**self.templates)

            return url

        if self.templates:
            self.df["url"] = self.df["url"].map(_render_jinja)

    def _process_references(self, references, template_overrides=None):
        if isinstance(references, (str, bytes)):
            references = json.loads(references)
        vers = references.get("version", None)
        if vers is None:
            self._process_references0(references)
        elif vers == 1:
            self._process_references1(references, template_overrides=template_overrides)
        else:
            raise ValueError(f"Unknown reference spec version: {vers}")
        # TODO: we make dircache by iterating over all entries, but for Spec >= 1,
        #  can replace with programmatic. Is it even needed for mapper interface?

    def _process_references0(self, references):
        """Make reference dict for Spec Version 0"""
        if "zarr_consolidated_format" in references:
            # special case for Ike prototype
            references = _unmodel_hdf5(references)
        self.references = references

    def _process_references1(self, references, template_overrides=None):
        if not self.simple_templates or self.templates:
            try:
                import jinja2
            except ImportError as e:
                raise ValueError("Reference Spec Version 1 requires jinja2") from e
        self.references = {}
        self._process_templates(references.get("templates", {}))

        @lru_cache(1000)
        def _render_jinja(u):
            return jinja2.Template(u).render(**self.templates)

        for k, v in references.get("refs", {}).items():
            if isinstance(v, str):
                if v.startswith("base64:"):
                    self.references[k] = base64.b64decode(v[7:])
                self.references[k] = v
            elif self.templates:
                u = v[0]
                if "{{" in u:
                    if self.simple_templates:
                        u = (
                            u.replace("{{", "{")
                            .replace("}}", "}")
                            .format(**self.templates)
                        )
                    else:
                        u = _render_jinja(u)
                self.references[k] = [u] if len(v) == 1 else [u, v[1], v[2]]
            else:
                self.references[k] = v
        self.references.update(self._process_gen(references.get("gen", [])))

    def _process_templates(self, tmp):
        import jinja2

        self.templates = {}
        if self.template_overrides is not None:
            tmp.update(self.template_overrides)
        for k, v in tmp.items():
            if "{{" in v:
                self.templates[k] = lambda temp=v, **kwargs: jinja2.Template(
                    temp
                ).render(**kwargs)
            else:
                self.templates[k] = v

    def _process_gen(self, gens):
        import jinja2

        out = {}
        for gen in gens:
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
                key = jinja2.Template(gen["key"]).render(**pr, **self.templates)
                url = jinja2.Template(gen["url"]).render(**pr, **self.templates)
                if ("offset" in gen) and ("length" in gen):
                    offset = int(
                        jinja2.Template(gen["offset"]).render(**pr, **self.templates)
                    )
                    length = int(
                        jinja2.Template(gen["length"]).render(**pr, **self.templates)
                    )
                    out[key] = [url, offset, length]
                elif ("offset" in gen) ^ ("length" in gen):
                    raise ValueError(
                        "Both 'offset' and 'length' are required for a "
                        "reference generator entry if either is provided."
                    )
                else:
                    out[key] = [url]
        return out

    def _dircache_from_items(self):
        self.dircache = {"": []}
        if self.dataframe:
            it = self.df.iterrows()
        else:
            it = self.references.items()
        for path, part in it:
            if self.dataframe:
                if part["data"]:
                    size = len(part["data"])
                else:
                    size = part["size"]
            else:
                if isinstance(part, (bytes, str)):
                    size = len(part)
                elif len(part) == 1:
                    size = None
                else:
                    _, start, size = part
            par = path.rsplit("/", 1)[0] if "/" in path else ""
            par0 = par
            while par0 and par0 not in self.dircache:
                # build parent directories
                self.dircache[par0] = []
                self.dircache.setdefault(
                    par0.rsplit("/", 1)[0] if "/" in par0 else "", []
                ).append({"name": par0, "type": "directory", "size": 0})
                par0 = self._parent(par0)

            self.dircache[par].append({"name": path, "type": "file", "size": size})

    def open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        if mode != "rb":
            raise NotImplementedError
        data = self.cat_file(path)  # load whole chunk into memory
        return io.BytesIO(data)

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if not self.dircache:
            self._dircache_from_items()
        out = self._ls_from_cache(path)
        if out is None:
            raise FileNotFoundError
        if detail:
            return out
        return [o["name"] for o in out]

    def exists(self, path, **kwargs):  # overwrite auto-sync version
        return self.isdir(path) or self.isfile(path)

    def isdir(self, path):  # overwrite auto-sync version
        if self.dircache:
            return path in self.dircache
        else:
            # this may be faster than building dircache for single calls, but
            # by looping will be slow for many calls; could cache it?
            return any(_.startswith(f"{path}/") for _ in self.references)

    def isfile(self, path):  # overwrite auto-sync version
        return path in self.references

    async def _ls(self, path, detail=True, **kwargs):  # calls fast sync code
        return self.ls(path, detail, **kwargs)

    def find(self, path, maxdepth=None, withdirs=False, **kwargs):
        if withdirs:
            return super().find(path, maxdepth=maxdepth, withdirs=withdirs, **kwargs)
        if path:
            path = self._strip_protocol(path)
            return sorted(k for k in self.references if k.startswith(path))
        return sorted(self.references)

    def info(self, path, **kwargs):
        out = self.ls(path, True)
        out0 = [o for o in out if o["name"] == path]
        if not out0:
            return {"name": path, "type": "directory", "size": 0}
        if out0[0]["size"] is None:
            # if this is a whole remote file, update size using remote FS
            prot, _ = split_protocol(self.references[path][0])
            out0[0]["size"] = self.fss[prot].size(self.references[path][0])
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
