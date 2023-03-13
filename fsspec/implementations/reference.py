import ast
import base64
import collections
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

from ..asyn import AsyncFileSystem
from ..callbacks import _DEFAULT_CALLBACK
from ..core import filesystem, open, split_protocol
from ..spec import AbstractFileSystem
from ..utils import isfilelike, merge_offset_ranges, other_paths

logger = logging.getLogger("fsspec.reference")


class ReferenceNotReachable(RuntimeError):
    def __init__(self, reference, target, *args):
        super().__init__(*args)
        self.reference = reference
        self.target = target

    def __str__(self):
        return f'Reference "{self.reference}" failed to fetch target {self.target}'


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


class RefsValuesView(collections.abc.ValuesView):
    def __iter__(self):
        # Caveat: Note that this generates all expected keys, but does not
        # account for reference keys that are missing.
        metkeys = [".zgroup", ".zattrs"]
        for metkey in metkeys:
            if metkey in self._mapping.zmetadata["metadata"]:
                yield self._mapping[metkey]
        for field in self._mapping.listdir():
            if field.startswith("."):
                yield self._mapping[field]
            else:
                chunk_sizes = self._mapping._get_chunk_sizes(field)
                yield self._mapping["/".join([field, ".zarray"])]
                yield self._mapping["/".join([field, ".zattrs"])]
                if chunk_sizes.size == 0:
                    yield self._mapping[field + "/0"]
                    continue
                yield from self._mapping._generate_all_records(field)


class RefsItemsView(collections.abc.ItemsView):
    def __iter__(self):
        return zip(self._mapping.keys(), self._mapping.values())


class LazyReferenceMapper(collections.abc.MutableMapping):
    """Interface to read parquet store as if it were a standard kerchunk
    references dict."""

    # import is class level to prevent numpy dep requirement for fsspec
    import numpy as np
    import pandas as pd

    def __init__(
        self,
        root,
        fs=None,
        cache_size=128,
    ):
        """
        Parameters
        ----------
        root : str
            Root of parquet store
        fs : fsspec.AbstractFileSystem
            fsspec filesystem object, default is local filesystem.
        cache_size : int
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
        """
        self.root = root
        self.chunk_sizes = {}
        self._items = {}
        self.fs = fsspec.filesystem("file") if fs is None else fs

        # Define function to open and decompress refs
        @lru_cache(maxsize=cache_size)
        def open_refs(path):
            with self.fs.open(path) as f:
                df = self.pd.read_parquet(f, engine="fastparquet")
            refs = {c: df[c].values for c in df.columns}
            return refs

        self.open_refs = open_refs

    def listdir(self, basename=True):
        listing = self.fs.ls(self.root)
        if basename:
            listing = [os.path.basename(path) for path in listing]
        return listing

    def join(self, *args):
        return self.fs.sep.join(args)

    @property
    def zmetadata(self):
        if ".zmetadata" not in self._items:
            self._get_and_cache_metadata()
        return self._items[".zmetadata"]

    @property
    def record_size(self):
        return self.zmetadata["record_size"]

    def _load_one_key(self, key):
        if "/" not in key:
            if key in self.zmetadata["metadata"]:
                return self._get_and_cache_metadata(key)
            if key not in self.listdir():
                raise KeyError
            return self._get_and_cache_metadata(key)
        else:
            field, sub_key = key.split("/")
            if sub_key.startswith("."):
                # zarr metadata keys are always cached
                return self._get_and_cache_metadata(key)
            # Chunk keys can be loaded from row group and cached in LRU cache
            record, ri, chunk_size = self._key_to_record(key)
            if chunk_size == 0:
                return b""
            pf_path = self.join(self.root, field, f"refs.{record}.parq")
            refs = self.open_refs(pf_path)
            columns = ["path", "offset", "size", "raw"]
            selection = [refs[c][ri] if c in refs else None for c in columns]
            raw = selection[-1]
            if raw is not None:
                return raw
            data = selection[:-1]
            if not isinstance(data[0], str):
                data[0] = ""
            return data

    def _get_and_cache_metadata(self, key=None):
        if key is None or ".zmetadata" not in self._items:
            with self.fs.open(self.join(self.root, ".zmetadata"), "rb") as f:
                self._items[".zmetadata"] = json.load(f)
        if key is None:
            return
        if key == ".zmetadata":
            # consolidated metadata JSON needs to be encoded as string for zarr
            return json.dumps(self.zmetadata)
        return self.zmetadata["metadata"][key]

    def _key_to_record(self, key):
        field, chunk = key.split("/")
        chunk_sizes = self._get_chunk_sizes(field)
        if chunk_sizes.size == 0:
            return 0, 0, 0
        chunk_idx = self.np.array([int(c) for c in chunk.split(".")])
        chunk_number = self.np.ravel_multi_index(chunk_idx, chunk_sizes)
        record = chunk_number // self.record_size
        ri = chunk_number % self.record_size
        return record, ri, chunk_sizes.size

    def _get_chunk_sizes(self, field):
        if field not in self.chunk_sizes:
            zarray = self._get_and_cache_metadata(f"{field}/.zarray")
            size_ratio = self.np.array(zarray["shape"]) / self.np.array(
                zarray["chunks"]
            )
            self.chunk_sizes[field] = self.np.ceil(size_ratio).astype(int)
        return self.chunk_sizes[field]

    def _generate_record(self, field, irec):
        refs = self.open_refs(self.join(self.root, field, f"refs.{irec}.parq"))
        refs_type = ""
        if "path" in refs:
            refs_type += "url"
            paths = refs["path"]
            offsets = refs["offset"]
            sizes = refs["size"]
            if hasattr(paths, "codes"):
                # Since this is meant to be temporarily iterated over we don't
                # need to mind fully expanded categoricals
                paths = self.np.asarray(paths)
        if "raw" in refs:
            refs_type += "raw"
            raws = refs["raw"]
        if refs_type == "url":
            # Only urls
            for i in range(paths.size):
                yield [paths[i], offsets[i], sizes[i]]
        elif refs_type == "raw":
            # Only raws
            for i in range(raws.size):
                yield raws[i]
        else:
            # Mix of raw and urls
            for i in range(raws.size):
                raw = raws[i]
                if raw:
                    yield raw
                else:
                    yield [paths[i], offsets[i], sizes[i]]

    def _generate_all_records(self, field):
        chunk_size = self._get_chunk_sizes(field)
        nrec = int(self.np.ceil(self.np.product(chunk_size) / self.record_size))
        for irec in range(nrec):
            record = self._generate_record(field, irec)
            yield from record

    def values(self):
        return RefsValuesView(self)

    def items(self):
        return RefsItemsView(self)

    def __getitem__(self, key):
        if key in self._items and key != ".zmetadata":
            return self._items[key]
        return self._load_one_key(key)

    def __setitem__(self, key, value):
        self._items[key] = value

    def __delitem__(self, key):
        del self._items[key]

    def __len__(self):
        # Caveat: This counts expected references, not actual
        count = 0
        for field in self.listdir():
            if field.startswith("."):
                count += 1
            else:
                chunk_sizes = self._get_chunk_sizes(field)
                nchunks = self.np.product(chunk_sizes)
                count += 2 + nchunks
        metkeys = [".zgroup", ".zattrs"]
        for metkey in metkeys:
            if metkey in self.zmetadata["metadata"]:
                count += 1
        return count

    def __iter__(self):
        # Caveat: Note that this generates all expected keys, but does not
        # account for reference keys that are missing.
        metkeys = [".zgroup", ".zattrs"]
        for metkey in metkeys:
            if metkey in self.zmetadata["metadata"]:
                yield metkey
        for field in self.listdir():
            if field.startswith("."):
                yield field
            else:
                chunk_sizes = self._get_chunk_sizes(field)
                yield "/".join([field, ".zarray"])
                yield "/".join([field, ".zattrs"])
                if chunk_sizes.size == 0:
                    yield field + "/0"
                    continue
                inds = self.np.ndindex(*chunk_sizes)
                for ind in inds:
                    yield field + "/" + ".".join([str(c) for c in ind])


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
        max_gap=64_000,
        max_block=256_000_000,
        cache_size=128,
        **kwargs,
    ):
        """

        Parameters
        ----------
        fo : dict or str
            The set of references to use for this instance, with a structure as above.
            If str referencing a JSON file, will use fsspec.open, in conjunction
            with target_options and target_protocol to open and parse JSON at this
            location. If a directory, then assume references are a set of parquet
            files to be loaded lazily.
        target : str
            For any references having target_url as None, this is the default file
            target to use
        ref_storage_args : dict
            If references is a str, use these kwargs for loading the JSON file.
            Deprecated: use target_options instead.
        target_protocol : str
            Used for loading the reference file, if it is a path. If None, protocol
            will be derived from the given path
        target_options : dict
            Extra FS options for loading the reference file ``fo``, if given as a path
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
        max_gap, max_block: int
            For merging multiple concurrent requests to the same remote file.
            Neighboring byte ranges will only be merged when their
            inter-range gap is <= ``max_gap``. Default is 64KB. Set to 0
            to only merge when it requires no extra bytes. Pass a negative
            number to disable merging, appropriate for local target files.
            Neighboring byte ranges will only be merged when the size of
            the aggregated range is <= ``max_block``. Default is 256MB.
        cache_size : int
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
            Only used for lazily loaded references.
        kwargs : passed to parent class
        """
        super().__init__(**kwargs)
        self.target = target
        self.template_overrides = template_overrides
        self.simple_templates = simple_templates
        self.templates = {}
        self.fss = {}
        self._dircache = {}
        self.max_gap = max_gap
        self.max_block = max_block
        if hasattr(fo, "read"):
            text = json.load(fo)
            text = text.decode() if isinstance(text, bytes) else text
        if target_protocol:
            extra = {"protocol": target_protocol}
        else:
            extra = {"protocol": "file"}
        dic = dict(**(ref_storage_args or target_options or {}), **extra)
        ref_fs = filesystem(**dic)
        if isinstance(fo, str):
            if ref_fs.isfile(fo):
                # text JSON
                with ref_fs.open(fo, "rb") as f:
                    logger.info("Read reference from URL %s", fo)
                    text = json.load(f)
            else:
                # Lazy parquet refs
                text = LazyReferenceMapper(
                    fo,
                    fs=ref_fs,
                    cache_size=cache_size,
                )
        else:
            # dictionaries
            text = fo
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
            if None not in self.fss:
                self.fss[None] = filesystem("file")
            return
        if fs is not None:
            # single remote FS
            remote_protocol = (
                fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
            )
            self.fss[remote_protocol] = fs

        if remote_protocol is None:
            # get single protocol from any templates
            for ref in self.templates.values():
                if callable(ref):
                    ref = ref()
                protocol, _ = fsspec.core.split_protocol(ref)
                if protocol and protocol not in self.fss:
                    fs = filesystem(protocol, **(remote_options or {}))
                    self.fss[protocol] = fs
        if remote_protocol is None:
            # get single protocol from references
            for ref in self.references.values():
                if callable(ref):
                    ref = ref()
                if isinstance(ref, list) and ref[0]:
                    protocol, _ = fsspec.core.split_protocol(ref[0])
                    if protocol and protocol not in self.fss:
                        fs = filesystem(protocol, **(remote_options or {}))
                        self.fss[protocol] = fs

        if remote_protocol and remote_protocol not in self.fss:
            fs = filesystem(remote_protocol, **(remote_options or {}))
            self.fss[remote_protocol] = fs

        self.fss[None] = fs or filesystem("file")  # default one

    def _cat_common(self, path, start=None, end=None):
        path = self._strip_protocol(path)
        logger.debug(f"cat: {path}")
        try:
            part = self.references[path]
        except KeyError:
            raise FileNotFoundError(path)
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
            start1, end1 = start, end
        else:
            url, start0, size = part
            logger.debug(f"Reference: {path} => {url}, offset {start0}, size {size}")
            end0 = start0 + size

            if start is not None:
                if start >= 0:
                    start1 = start0 + start
                else:
                    start1 = end0 + start
            else:
                start1 = start0
            if end is not None:
                if end >= 0:
                    end1 = start0 + end
                else:
                    end1 = end0 + end
            else:
                end1 = end0
        if url is None:
            url = self.target
        return url, start1, end1

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path, start=start, end=end)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        try:
            await self.fss[protocol]._cat_file(part_or_url, start=start, end=end)
        except Exception as e:
            raise ReferenceNotReachable(path, part_or_url) from e

    def cat_file(self, path, start=None, end=None, **kwargs):
        part_or_url, start0, end0 = self._cat_common(path, start=start, end=end)
        if isinstance(part_or_url, bytes):
            return part_or_url[start:end]
        protocol, _ = split_protocol(part_or_url)
        try:
            return self.fss[protocol].cat_file(part_or_url, start=start0, end=end0)
        except Exception as e:
            raise ReferenceNotReachable(path, part_or_url) from e

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
        if recursive:
            # trigger directory build
            self.ls("")
        rpath = self.expand_path(rpath, recursive=recursive)
        fs = fsspec.filesystem("file", auto_mkdir=True)
        targets = other_paths(rpath, lpath)
        if recursive:
            data = self.cat([r for r in rpath if not self.isdir(r)])
        else:
            data = self.cat(rpath)
        for remote, local in zip(rpath, targets):
            if remote in data:
                fs.pipe_file(local, data[remote])

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        if isinstance(path, str) and recursive:
            raise NotImplementedError
        if isinstance(path, list) and (recursive or any("*" in p for p in path)):
            raise NotImplementedError
        proto_dict = _protocol_groups(path, self.references)
        out = {}
        for proto, paths in proto_dict.items():
            fs = self.fss[proto]
            urls, starts, ends = [], [], []
            for p in paths:
                # find references or label not-found. Early exit if any not
                # found and on_error is "raise"
                try:
                    u, s, e = self._cat_common(p)
                    urls.append(u)
                    starts.append(s)
                    ends.append(e)
                except FileNotFoundError as e:
                    if on_error == "raise":
                        raise
                    if on_error != "omit":
                        out[p] = e

            # process references into form for merging
            urls2 = []
            starts2 = []
            ends2 = []
            paths2 = []
            whole_files = set()
            for u, s, e, p in zip(urls, starts, ends, paths):
                if isinstance(u, bytes):
                    # data
                    out[p] = u
                elif s is None:
                    # whole file - limits are None, None, but no further
                    # entries take for this file
                    whole_files.add(u)
                    urls2.append(u)
                    starts2.append(s)
                    ends2.append(e)
                    paths2.append(p)
            for u, s, e, p in zip(urls, starts, ends, paths):
                # second run to account for files that are to be loaded whole
                if s is not None and u not in whole_files:
                    urls2.append(u)
                    starts2.append(s)
                    ends2.append(e)
                    paths2.append(p)

            # merge and fetch consolidated ranges
            new_paths, new_starts, new_ends = merge_offset_ranges(
                list(urls2),
                list(starts2),
                list(ends2),
                sort=True,
                max_gap=self.max_gap,
                max_block=self.max_block,
            )
            bytes_out = fs.cat_ranges(new_paths, new_starts, new_ends)

            # unbundle from merged bytes - simple approach
            for u, s, e, p in zip(urls, starts, ends, paths):
                if p in out:
                    continue  # was bytes, already handled
                for np, ns, ne, b in zip(new_paths, new_starts, new_ends, bytes_out):
                    if np == u and (ns is None or ne is None):
                        if isinstance(b, Exception):
                            out[p] = b
                        else:
                            out[p] = b[s:e]
                    elif np == u and s >= ns and e <= ne:
                        if isinstance(b, Exception):
                            out[p] = b
                        else:
                            out[p] = b[s - ns : (e - ne) or None]

        for k, v in out.copy().items():
            # these were valid references, but fetch failed, so transform exc
            if isinstance(v, Exception) and k in self.references:
                ex = out[k]
                new_ex = ReferenceNotReachable(k, self.references[k])
                new_ex.__cause__ = ex
                if on_error == "raise":
                    raise new_ex
                elif on_error != "omit":
                    out[k] = new_ex

        if len(out) == 1 and isinstance(path, str) and "*" not in path:
            return _first(out)
        return out

    def _process_references(self, references, template_overrides=None):
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
            import jinja2
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

        self.templates = {}
        if self.template_overrides is not None:
            tmp.update(self.template_overrides)
        for k, v in tmp.items():
            if "{{" in v:
                import jinja2

                self.templates[k] = lambda temp=v, **kwargs: jinja2.Template(
                    temp
                ).render(**kwargs)
            else:
                self.templates[k] = v

    def _process_gen(self, gens):

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
                import jinja2

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
        it = self.references.items()
        for path, part in it:
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

    def _open(self, path, mode="rb", block_size=None, cache_options=None, **kwargs):
        data = self.cat_file(path)  # load whole chunk into memory
        return io.BytesIO(data)

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if not self.dircache:
            self._dircache_from_items()
        out = self._ls_from_cache(path)
        if out is None:
            raise FileNotFoundError(path)
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

    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        # TODO: details
        if withdirs:
            return super().find(
                path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs
            )
        if path:
            path = self._strip_protocol(path)
            r = sorted(k for k in self.references if k.startswith(path))
        else:
            r = sorted(self.references)
        if detail:
            if not self.dircache:
                self._dircache_from_items()
            return {k: self._ls_from_cache(k)[0] for k in r}
        else:
            return r

    def info(self, path, **kwargs):
        if path in self.references:
            out = self.references[path]
            if isinstance(out, (str, bytes)):
                # decode base64 here
                return {"name": path, "type": "file", "size": len(out)}
            elif len(out) > 1:
                return {"name": path, "type": "file", "size": out[2]}
            else:
                out0 = [{"name": path, "type": "file", "size": None}]
        else:
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

    async def _rm_file(self, path, **kwargs):
        self.references.pop(
            path, None
        )  # ignores FileNotFound, just as well for directories
        self.dircache.clear()  # this is a bit heavy handed

    async def _pipe_file(self, path, data):
        # can be str or bytes
        self.references[path] = data
        self.dircache.clear()  # this is a bit heavy handed

    async def _put_file(self, lpath, rpath):
        # puts binary
        with open(lpath, "rb") as f:
            self.references[rpath] = f.read()
        self.dircache.clear()  # this is a bit heavy handed

    def save_json(self, url, **storage_options):
        """Write modified references into new location"""
        out = {}
        for k, v in self.references.items():
            if isinstance(v, bytes):
                try:
                    out[k] = v.decode("ascii")
                except UnicodeDecodeError:
                    out[k] = (b"base64:" + base64.b64encode(v)).decode()
            else:
                out[k] = v
        with fsspec.open(url, "wb", **storage_options) as f:
            f.write(json.dumps({"version": 1, "refs": out}).encode())


def prefix(x):
    if "/.z" in x or "/" not in x:
        return "metadata", x
    return x.split("/", 1)


def constant_prefix(x):
    return "metadata", x


class DFReferenceFileSystem(AbstractFileSystem):
    """
    (Experimental) Parquet-based Reference Filesystem

    Putative replacement or adjunct to ReferenceFileSystem with
    additional capabilities:
    - loads from parquet for better on-disk and in-memory space
    - optional lazy loading by key prefix (lazy=True)
    - multiple targets for a given key (allow_multi=True), concatenated
      together by default, of multi_func=
    - per-chunk processing with extra parameters stored in the parquet
      (chunk_func=)

    This implementation is not (yet) multable.
    """

    def __init__(
        self,
        fo,
        target_options=None,
        remote_protocol=None,
        remote_options=None,
        fs=None,
        max_gap=64_000,
        max_block=256_000_000,
        parquet_kwargs=None,
        chunk_func=None,
        allow_multi=False,
        multi_func=b"".join,
        prefix_func=prefix,
        lazy=False,
        **kwargs,
    ):
        self.fo = fo
        self.target_options = target_options or {}
        self.max_gap = max_gap
        self.max_block = max_block
        self.dataframes = {}
        self.keysets = {}
        self.url_dict = {}
        self.template_dict = {}
        self.prefs = None
        self.fss = {}
        self.dirs = None
        self.lazy = lazy
        self.chunk_func = chunk_func
        self.allow_multi = allow_multi
        self.multi_func = multi_func
        self.prefix_func = prefix_func if lazy else constant_prefix
        self.pkwargs = parquet_kwargs or {}
        if fs is not None:
            # single remote FS
            remote_protocol = (
                fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
            )
            self.fss[remote_protocol] = fs

        if remote_protocol and remote_protocol not in self.fss:
            fs = filesystem(remote_protocol, **(remote_options or {}))
            self.fss[remote_protocol] = fs

        if fs:
            self.fss[None] = fs
        elif self.fss:
            self.fss[None] = iter(self.fss.values()).__next__()
        else:
            self.fss[None] = fsspec.filesystem(
                remote_protocol, **(remote_options or {})
            )

        super().__init__(**kwargs)
        self._reference_part()

    def _reference_part(self, part="metadata"):
        """Load some references from parquet

        If lazy is False, this is called exactly once per instance

        If lazy is true, selecting a path will determine the name of
        the target parquet file, and the resultant columns will be
        cached so the file need not be read again
        """
        import fastparquet

        if part != "metadata" and part not in self.dirs:
            raise FileNotFoundError(f"prefix {part}")
        if part not in self.dataframes:
            url = f"{self.fo}/{part}.parq" if self.lazy else self.fo
            fs, path = fsspec.core.url_to_fs(url, **self.target_options)
            pf = fastparquet.ParquetFile(path, fs=fs)
            self.template_dict[part] = pf.key_value_metadata
            df = pf.to_pandas()
            thispart = {}
            for k in df:
                if df[k].dtype == "category" and k == "path":
                    self.url_dict[part] = df[k].cat.categories.values
                    thispart[k] = df[k].cat.codes.values
                else:
                    thispart[k] = df[k].values
            self.dataframes[part] = thispart
            if self.allow_multi is False:
                self.keysets[part] = {
                    k: i for (i, k) in enumerate(self.dataframes[part]["key"])
                }
            else:
                self.keysets[part] = {}
                for i, k in enumerate(self.dataframes[part]["key"]):
                    self.keysets[part].setdefault(k, []).append(i)
            if part == "metadata":
                self.dirs = {
                    k.rsplit("/", 1)[0]
                    for k in self.dataframes[part]["key"]
                    if "/" in k
                }
                self.prefs = (
                    ast.literal_eval(pf.key_value_metadata["prefs"])
                    if "prefs" in pf.key_value_metadata
                    else set()
                )

        return self.dataframes[part]

    def isdir(self, path):
        return path in self.dirs

    def cat_file(self, path, start=None, end=None, **kwargs):
        return self.cat_ranges([path], [start], [end])[0]

    def cat(self, path, recursive=False, on_error="return", **kwargs):
        paths = self.expand_path(path, recursive=recursive)
        paths1 = [p for p in paths if not self.isdir(p)]
        result = {
            p: data
            for p, data in zip(
                paths1, self.cat_ranges(paths1, on_error=on_error, **kwargs)
            )
        }
        if len(paths1) == 1 and recursive is False and "*" not in path:
            # same as cat_file
            return list(result.values())[0]
        return result

    def cat_ranges(self, paths, starts=None, ends=None, on_error="return", **kwargs):
        out = []  # eventual output; initially each key contains raw bytes or None
        proto_dict = {}  # mapping of protocol to lists of URL/start/end to fetch
        assign_dict = {}  # how to assign the results of cat_ranges to output
        if starts is None:
            starts = [None] * len(paths)
        if ends is None:
            ends = [None] * len(paths)
        for p, s, e in zip(paths, starts, ends):
            thislist = []
            out.append(thislist)
            if self.lazy:
                pref, p0 = self.prefix_func(p)
                if pref in self.prefs:
                    # reference already inlined in metadata file
                    pref = "metadata"
                else:
                    # new key in the target pref file
                    p = p0
            else:
                # everything is in the same file
                pref = "metadata"
            self._reference_part(pref)
            inds = self.keysets[pref][p]
            if isinstance(inds, int):
                inds = [inds]
            for i in inds:
                if x := self.dataframes[pref]["raw"][i]:
                    thislist.append(x)
                else:
                    # infer path - cache this?
                    path = self.dataframes[pref]["path"][i]
                    if pref in self.url_dict:
                        # dict-encoded columns; actually, numpy can do
                        # many of these at once with int fancy indexing
                        path = self.url_dict[pref][path]
                    # apply template: common prefix
                    path = path.format(**self.template_dict[pref])

                    prot, _ = split_protocol(path)
                    proto_dict.setdefault(prot, [[], [], []])
                    proto_dict[prot][0].append(path)
                    if s is None or s >= 0:
                        proto_dict[prot][1].append(
                            self.dataframes[pref]["offset"][i] or s
                        )
                    else:
                        # range is from end of file, which we do not know
                        # the size of, so this can only work if there is no
                        # merging
                        proto_dict[prot][1].append(s)

                    if e is None or e >= 0:
                        proto_dict[prot][2].append(
                            self.dataframes[pref]["offset"][i]
                            + self.dataframes[pref]["size"][i]
                            or e
                        )
                    else:
                        # range is from end of file, which we do not know
                        # the size of, so this can only work if there is no
                        # merging
                        proto_dict[prot][1].append(e)
                    thislist.append(None)
                    assign_dict.setdefault(prot, []).append(
                        (thislist, len(thislist) - 1)
                    )

        for proto, (urls2, starts2, ends2) in proto_dict.items():
            fs = self.fss[proto]

            new_paths, new_starts, new_ends = merge_offset_ranges(
                list(urls2),
                list(starts2),
                list(ends2),
                sort=True,
                max_gap=self.max_gap,
                max_block=self.max_block,
            )
            bytes_out = fs.cat_ranges(new_paths, new_starts, new_ends)
            if len(urls2) == len(bytes_out):
                # we didn't do any merging
                for (l, i), d in zip(assign_dict[proto], bytes_out):
                    l[i] = d
            else:
                # unbundle from merged bytes - simple approach
                for u, s, e, (l, i) in zip(urls2, starts2, ends2, assign_dict[proto]):
                    if p in out:
                        continue  # was bytes, already handled
                    for np, ns, ne, b in zip(
                        new_paths, new_starts, new_ends, bytes_out
                    ):
                        if np == u and (ns is None or ne is None):
                            l[i] = b[s:e]
                        elif np == u and s >= ns and e <= ne:
                            l[i] = b[s - ns : (e - ne) or None]

        out = [self.multi_func(part) for part in out]
        return out

    def find(self, path, detail=False, withdirs=False, **kwargs):
        path = self._strip_protocol(path)
        if path in self.dirs:
            path = path + "/"
        pref, p = self.prefix_func(path)
        dirs = (
            [
                {"name": d, "size": 0, "type": "directory"}
                for d in self.dirs
                if d.startswith(path)
            ]
            if withdirs
            else []
        )
        if pref in self.prefs:
            pref = "metadata"
        df = self._reference_part(pref)
        files = [
            {"name": k, "type": "file", "size": _size(self.dataframes["metadata"], i)}
            for k, i in self.keysets["metadata"].items()
            if k.startswith(path)
        ]
        if self.lazy and pref != "metadata":
            files.extend(
                [
                    {"name": f"{pref}/{k}", "type": "file", "size": _size(df, i)}
                    for k, i in self.keysets[pref].items()
                    if k.startswith(p)
                ]
            )
        if detail:
            return dirs + files
        return [k["name"] for k in dirs + files]

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        allfiles = self.find(path, detail=True, withdirs=True)
        isdir = path in self.dirs
        subdfiles = [
            p for p in allfiles if p["name"].count("/") == path.count("/") + isdir
        ]
        if detail:
            return subdfiles
        return [p["name"] for p in subdfiles]

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)

        if path in self.dirs:
            return {"name": path, "type": "directory", "Size": 0}
        return self.ls(path, detail=True)[0]


def _size(df, i):
    if isinstance(i, int):
        return len(df["raw"][i]) if df["raw"][i] else df["size"][i]
    return sum(len(df["raw"][_]) if df["raw"][_] else df["size"][_] for _ in i)


def _unmodel_hdf5(references):
    """Special JSON format from HDF5 prototype"""
    # see https://gist.github.com/ajelenak/80354a95b449cedea5cca508004f97a9
    ref = {}
    for key, value in references["metadata"].items():
        if key.endswith(".zchunkstore"):
            source = value.pop("source")["uri"]
            for k, v in value.items():
                ref[k] = (source, v["offset"], v["offset"] + v["size"])
        else:
            ref[key] = json.dumps(value).encode()
    return ref
