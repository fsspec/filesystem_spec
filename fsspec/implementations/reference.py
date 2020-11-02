import json
from ..asyn import AsyncFileSystem
from ..core import open, filesystem


class ReferenceFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system

    Initial version: single URL for the reference target. Later versions
    may allow multiple files from a file system or arbitrary URLs for the
    targets.

    This FileSystem is read-only. It is designed to be used with async
    targets (for now). This FileSystem only allows whole-file access, no
    ``open``. We do not get original file details from the target FS.

    Configuration is by passing a dict of references at init, or a URL to
    a JSON file containing the same; this dict
    can also contain concrete data for some set of paths.
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
        **kwargs
    ):
        super().__init__(**kwargs)
        if isinstance(references, str):
            with open(references, "rb", **(ref_storage_args or {})) as f:
                references = json.load(f)
        self.references = references
        self.target = target
        self._process_references()
        if fs is None:
            fs = filesystem(target_protocol, loop=self.loop, **(target_options or {}))
        self.fs = fs

    async def _cat_file(self, path):
        path = self._strip_protocol(path)
        part = self.references[path]
        if isinstance(part, bytes):
            return part
        url, start, end = part
        if url is None:
            url = self.target
        return await self.fs._cat_file(url, start=start, end=end)

    def _process_references(self):
        if "zarr_consolidated_format" in self.references:
            self.references = unmodel_hds(self.references)
        self.dircache = {"": []}
        for path, part in self.references.items():
            if isinstance(part, bytes):
                size = len(part)
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


def unmodel_hds(references):
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
