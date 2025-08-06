class BaseCache:
    """Pass-though cache: doesn't keep anything, calls every time

    Acts as base class for other cachers

    Parameters
    ----------
    blocksize: int
        How far to read ahead in numbers of bytes
    fetcher: func
        Function of the form f(start, end) which gets bytes from remote as
        specified
    size: int
        How big this file is
    """

    name = "none"

    def __init__(self, blocksize: int, fetcher, size: int) -> None:
        self.blocksize = blocksize
        self.nblocks = 0
        self.fetcher = fetcher
        self.size = size
        self.hit_count = 0
        self.miss_count = 0
        # the bytes that we actually requested
        self.total_requested_bytes = 0

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        if start is None:
            start = 0
        if stop is None:
            stop = self.size
        if start >= self.size or start >= stop:
            return b""
        return self.fetcher(start, stop)

    def _reset_stats(self) -> None:
        """Reset hit and miss counts for a more ganular report e.g. by file."""
        self.hit_count = 0
        self.miss_count = 0
        self.total_requested_bytes = 0

    def _log_stats(self) -> str:
        """Return a formatted string of the cache statistics."""
        if self.hit_count == 0 and self.miss_count == 0:
            # a cache that does nothing, this is for logs only
            return ""
        return f" , {self.name}: {self.hit_count} hits, {self.miss_count} misses, {self.total_requested_bytes} total requested bytes"

    def __repr__(self) -> str:
        # TODO: use rich for better formatting
        return f"""
        <{self.__class__.__name__}:
            block size  :   {self.blocksize}
            block count :   {self.nblocks}
            file size   :   {self.size}
            cache hits  :   {self.hit_count}
            cache misses:   {self.miss_count}
            total requested bytes: {self.total_requested_bytes}>
        """


class ReadAheadCache(BaseCache):
    """Cache which reads only when we get beyond a block of data

    This is a much simpler version of BytesCache, and does not attempt to
    fill holes in the cache or keep fragments alive. It is best suited to
    many small reads in a sequential order (e.g., reading lines from a file).
    """

    name = "readahead"

    def __init__(self, blocksize: int, fetcher, size: int) -> None:
        super().__init__(blocksize, fetcher, size)
        self.cache = b""
        self.start = 0
        self.end = 0

    def _fetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None or end > self.size:
            end = self.size
        if start >= self.size or start >= end:
            return b""
        l = end - start
        if start >= self.start and end <= self.end:
            # cache hit
            self.hit_count += 1
            return self.cache[start - self.start : end - self.start]
        elif self.start <= start < self.end:
            # partial hit
            self.miss_count += 1
            part = self.cache[start - self.start :]
            l -= len(part)
            start = self.end
        else:
            # miss
            self.miss_count += 1
            part = b""
        end = min(self.size, end + self.blocksize)
        self.total_requested_bytes += end - start
        self.cache = self.fetcher(start, end)  # new block replaces old
        self.start = start
        self.end = self.start + len(self.cache)
        return part + self.cache[:l]


class FirstChunkCache(BaseCache):
    """Caches the first block of a file only

    This may be useful for file types where the metadata is stored in the header,
    but is randomly accessed.
    """

    name = "first"

    def __init__(self, blocksize: int, fetcher, size: int) -> None:
        if blocksize > size:
            # this will buffer the whole thing
            blocksize = size
        super().__init__(blocksize, fetcher, size)
        self.cache: bytes | None = None

    def _fetch(self, start: int | None, end: int | None) -> bytes:
        start = start or 0
        if start > self.size:
            return b""

        end = min(end, self.size)

        if start < self.blocksize:
            if self.cache is None:
                self.miss_count += 1
                if end > self.blocksize:
                    self.total_requested_bytes += end
                    data = self.fetcher(0, end)
                    self.cache = data[: self.blocksize]
                    return data[start:]
                self.cache = self.fetcher(0, self.blocksize)
                self.total_requested_bytes += self.blocksize
            part = self.cache[start:end]
            if end > self.blocksize:
                self.total_requested_bytes += end - self.blocksize
                part += self.fetcher(self.blocksize, end)
            self.hit_count += 1
            return part
        else:
            self.miss_count += 1
            self.total_requested_bytes += end - start
            return self.fetcher(start, end)


class BytesCache(BaseCache):
    """Cache which holds data in a in-memory bytes object

    Implements read-ahead by the block size, for semi-random reads progressing
    through the file.

    Parameters
    ----------
    trim: bool
        As we read more data, whether to discard the start of the buffer when
        we are more than a blocksize ahead of it.
    """

    name = "bytes"

    def __init__(self, blocksize: int, fetcher, size: int, trim: bool = True) -> None:
        super().__init__(blocksize, fetcher, size)
        self.cache = b""
        self.start: int | None = None
        self.end: int | None = None
        self.trim = trim

    def _fetch(self, start: int | None, end: int | None) -> bytes:
        # TODO: only set start/end after fetch, in case it fails?
        # is this where retry logic might go?
        if start is None:
            start = 0
        if end is None:
            end = self.size
        if start >= self.size or start >= end:
            return b""
        if (
            self.start is not None
            and start >= self.start
            and self.end is not None
            and end < self.end
        ):
            # cache hit: we have all the required data
            offset = start - self.start
            self.hit_count += 1
            return self.cache[offset : offset + end - start]

        if self.blocksize:
            bend = min(self.size, end + self.blocksize)
        else:
            bend = end

        if bend == start or start > self.size:
            return b""

        if (self.start is None or start < self.start) and (
            self.end is None or end > self.end
        ):
            # First read, or extending both before and after
            self.total_requested_bytes += bend - start
            self.miss_count += 1
            self.cache = self.fetcher(start, bend)
            self.start = start
        else:
            assert self.start is not None
            assert self.end is not None
            self.miss_count += 1

            if start < self.start:
                if self.end is None or self.end - end > self.blocksize:
                    self.total_requested_bytes += bend - start
                    self.cache = self.fetcher(start, bend)
                    self.start = start
                else:
                    self.total_requested_bytes += self.start - start
                    new = self.fetcher(start, self.start)
                    self.start = start
                    self.cache = new + self.cache
            elif self.end is not None and bend > self.end:
                if self.end > self.size:
                    pass
                elif end - self.end > self.blocksize:
                    self.total_requested_bytes += bend - start
                    self.cache = self.fetcher(start, bend)
                    self.start = start
                else:
                    self.total_requested_bytes += bend - self.end
                    new = self.fetcher(self.end, bend)
                    self.cache = self.cache + new

        self.end = self.start + len(self.cache)
        offset = start - self.start
        out = self.cache[offset : offset + end - start]
        if self.trim:
            num = (self.end - self.start) // (self.blocksize + 1)
            if num > 1:
                self.start += self.blocksize * num
                self.cache = self.cache[self.blocksize * num :]
        return out

    def __len__(self) -> int:
        return len(self.cache)


class AllBytes(BaseCache):
    """Cache entire contents of the file"""

    name = "all"

    def __init__(
        self,
        blocksize: int | None = None,
        fetcher=None,
        size: int | None = None,
        data: bytes | None = None,
    ) -> None:
        super().__init__(blocksize, fetcher, size)  # type: ignore[arg-type]
        if data is None:
            self.miss_count += 1
            self.total_requested_bytes += self.size
            data = self.fetcher(0, self.size)
        self.data = data

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        self.hit_count += 1
        return self.data[start:stop]


class KnownPartsOfAFile(BaseCache):
    """
    Cache holding known file parts.

    Parameters
    ----------
    blocksize: int
        How far to read ahead in numbers of bytes
    fetcher: func
        Function of the form f(start, end) which gets bytes from remote as
        specified
    size: int
        How big this file is
    data: dict
        A dictionary mapping explicit `(start, stop)` file-offset tuples
        with known bytes.
    strict: bool, default True
        Whether to fetch reads that go beyond a known byte-range boundary.
        If `False`, any read that ends outside a known part will be zero
        padded. Note that zero padding will not be used for reads that
        begin outside a known byte-range.
    """

    name = "parts"

    def __init__(
        self,
        blocksize: int,
        fetcher,
        size: int,
        data: dict[tuple[int, int], bytes] | None = None,
        strict: bool = True,
        **_,
    ):
        super().__init__(blocksize, fetcher, size)
        self.strict = strict

        # simple consolidation of contiguous blocks
        if data:
            old_offsets = sorted(data.keys())
            offsets = [old_offsets[0]]
            blocks = [data.pop(old_offsets[0])]
            for start, stop in old_offsets[1:]:
                start0, stop0 = offsets[-1]
                if start == stop0:
                    offsets[-1] = (start0, stop)
                    blocks[-1] += data.pop((start, stop))
                else:
                    offsets.append((start, stop))
                    blocks.append(data.pop((start, stop)))

            self.data = dict(zip(offsets, blocks))
        else:
            self.data = {}

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        if start is None:
            start = 0
        if stop is None:
            stop = self.size

        out = b""
        for (loc0, loc1), data in self.data.items():
            # If self.strict=False, use zero-padded data
            # for reads beyond the end of a "known" buffer
            if loc0 <= start < loc1:
                off = start - loc0
                out = data[off : off + stop - start]
                if not self.strict or loc0 <= stop <= loc1:
                    # The request is within a known range, or
                    # it begins within a known range, and we
                    # are allowed to pad reads beyond the
                    # buffer with zero
                    out += b"\x00" * (stop - start - len(out))
                    self.hit_count += 1
                    return out
                else:
                    # The request ends outside a known range,
                    # and we are being "strict" about reads
                    # beyond the buffer
                    start = loc1
                    break

        # We only get here if there is a request outside the
        # known parts of the file. In an ideal world, this
        # should never happen
        if self.fetcher is None:
            # We cannot fetch the data, so raise an error
            raise ValueError(f"Read is outside the known file parts: {(start, stop)}. ")
        # We can fetch the data, but should warn the user
        # that this may be slow
        self.total_requested_bytes += stop - start
        self.miss_count += 1
        return out + super()._fetch(start, stop)


caches: dict[str | None, type[BaseCache]] = {
    # one custom case
    None: BaseCache,
}


def register_cache(cls: type[BaseCache], clobber: bool = False) -> None:
    """'Register' cache implementation.

    Parameters
    ----------
    clobber: bool, optional
        If set to True (default is False) - allow to overwrite existing
        entry.

    Raises
    ------
    ValueError
    """
    name = cls.name
    if not clobber and name in caches:
        raise ValueError(f"Cache with name {name!r} is already known: {caches[name]}")
    caches[name] = cls


for c in (
    BaseCache,
    ReadAheadCache,
    FirstChunkCache,
    AllBytes,
):
    register_cache(c)
