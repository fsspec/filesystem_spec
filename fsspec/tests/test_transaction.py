"""Regression tests for cache invalidation at transaction boundaries."""

from fsspec import AbstractFileSystem


class _TransactionCacheFS(AbstractFileSystem):
    """Filesystem with a dircache and transaction-aware invalidation.

    A path queued for end-of-transaction replay is considered already handled,
    so eviction is skipped; every other call evicts immediately. This models an
    implementation that treats the base-class queue as the single source of
    truth for which listings still need evicting.
    """

    cachable = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dircache = {}

    def invalidate_cache(self, path=None):
        if path not in self._invalidated_caches_in_transaction:
            if path is None:
                self.dircache.clear()
            else:
                self.dircache.pop(path, None)
        super().invalidate_cache(path)


def _new_fs():
    return _TransactionCacheFS(skip_instance_cache=True)


def test_end_transaction_replays_deferred_invalidations():
    fs = _new_fs()
    fs.dircache["/dir"] = ["stale"]

    fs.start_transaction()
    # A deferred write invalidates the directory listing.
    fs.invalidate_cache("/dir")
    # A listing performed later in the transaction repopulates the cache.
    fs.dircache["/dir"] = ["stale"]
    assert "/dir" in fs.dircache

    fs.end_transaction()

    assert "/dir" not in fs.dircache, "deferred invalidation was not replayed"
    assert fs._invalidated_caches_in_transaction == []
    assert fs._intrans is False


def test_invalidate_cache_still_works_outside_transaction():
    fs = _new_fs()
    fs.dircache["/dir"] = ["stale"]

    fs.invalidate_cache("/dir")

    assert "/dir" not in fs.dircache
