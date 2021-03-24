import base64
import itertools
import json

from ..asyn import AsyncFileSystem
from ..core import filesystem, open


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
        references,
        target=None,
        ref_storage_args=None,
        target_protocol=None,
        target_options=None,
        fs=None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        references : dict or str
            The set of references to use for this instance, with a structure as above.
            If str, will use fsspec.open, in conjunction with ref_storage_args to
            open and parse JSON at this location.
        target : str
            For any references having target_url as None, this is the default file
            target to use
        ref_storage_args : dict
            If references is a str, use these kwargs for loading the JSON file
        target_protocol : str
            If fs is None, instantiate a file system using this protocol
        target_options : dict
            If fs is None, instantiate a filesystem using these kwargs
        fs : file system instance
            Directly provide a file system, if you want to configure it beforehand. This
            takes precedence over target_protocol/target_options
        kwargs : passed to parent class
        """
        if fs is not None:
            if not fs.async_impl:
                raise NotImplementedError("Only works with async targets")
            kwargs["loop"] = fs.loop
        super().__init__(**kwargs)
        if fs is None:
            fs = filesystem(target_protocol, loop=self.loop, **(target_options or {}))
        if not fs.async_impl:
            raise NotImplementedError("Only works with async targets")
        if isinstance(references, str):
            with open(references, "rb", **(ref_storage_args or {})) as f:
                text = f.read()
        else:
            text = references
        self.target = target
        self._process_references(text)
        self.fs = fs

    async def _cat_file(self, path):
        path = self._strip_protocol(path)
        part = self.references[path]
        if isinstance(part, bytes):
            return part
        elif isinstance(part, str):
            return part.encode()

        if len(part) == 1:
            url, start, end = part[0], None, None
        else:
            url, start, size = part
            end = start + size
        if url is None:
            url = self.target
        return await self.fs._cat_file(url, start=start, end=end)

    def _process_references(self, references):
        if isinstance(references, bytes):
            references = json.loads(references.decode())
        vers = references.get("version", None)
        if vers is None:
            self._process_references0(references)
        elif vers == 1:
            self._process_references1(references)
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

    def _process_references1(self, references):
        try:
            import jinja2
        except ImportError as e:
            raise ValueError("Reference Spec Version 1 requires jinja2") from e
        self.references = {}
        templates = {}
        for k, v in references.get("templates", {}).items():
            if "{{" in v:
                templates[k] = lambda temp=v, **kwargs: jinja2.Template(temp).render(
                    **kwargs
                )
            else:
                templates[k] = v

        for k, v in references["refs"].items():
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
                offset = int(jinja2.Template(gen["offset"]).render(**pr, **templates))
                length = int(jinja2.Template(gen["length"]).render(**pr, **templates))

                self.references[key] = [url, offset, length]

    def _dircache_from_items(self):
        self.dircache = {"": []}
        for path, part in self.references.items():
            if isinstance(part, (bytes, str)):
                size = len(part)
            elif len(part) == 1:
                size = None
            else:
                _, start, end = part
                size = end - start
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

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        out = self._ls_from_cache(path)
        if detail:
            return out
        return [o["name"] for o in out]


def _unmodel_hdf5(references):
    """Special JSON format from HDF5"""
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
