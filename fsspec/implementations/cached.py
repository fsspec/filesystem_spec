import ujson
import os
import hashlib
import tempfile
from fsspec import get_filesystem_class
from fsspec.core import MMapCache


def make_caching_class(protocol, storage_options, cache_storage="TMP"):
    FS = get_filesystem_class(protocol)

    class CachingFileSystem(FS):

        def __init__(self, *args, **kwargs):
            if cache_storage == "TMP":
                storage = tempfile.mkdtemp()
            else:
                storage = cache_storage
            os.makedirs(storage, exist_ok=True)
            self.storage = storage
            self.load_cache()
            super().__init__(*args, **kwargs)

        def load_cache(self):
            fn = os.path.join(self.storage, 'cache.json')
            if os.path.exists(fn):
                with open(fn) as f:
                    self.cache = ujson.load(f)
                    for c in self.cache.values():
                        if isinstance(c['blocks'], list):
                            c['blocks'] = set(c['blocks'])
            else:
                self.cache = {}

        def save_cache(self):
            fn = os.path.join(self.storage, 'cache.json')
            cache = {k: v.copy() for k, v in self.cache.items()}
            for c in cache.values():
                if isinstance(c['blocks'], set):
                    c['blocks'] = list(c['blocks'])
            with open(fn, 'w') as f:
                ujson.dump(self.cache, f)

        def _open(self, path, mode='rb', **kwargs):
            if mode != 'rb':
                return super()._open(path, mode=mode, **kwargs)
            if path in self.cache:
                detail = self.cache[path]
                hash, blocks = detail['fn'], detail['blocks']
                fn = os.path.join(self.storage, hash)
                if blocks is True:
                    return open(fn, 'rb')
                else:
                    blocks = set(blocks)
            else:
                hash = hashlib.sha256(path.encode()).hexdigest()
                fn = os.path.join(self.storage, hash)
                blocks = set()
                self.cache[path] = {'fn': hash, 'blocks': blocks}
            f = super()._open(path, mode=mode, cache_type='none', **kwargs)
            f.cache = MMapCache(f.blocksize, f._fetch_range, f.size,
                                fn, blocks)
            close = f.close
            f.close = lambda: self.close_and_update(f, close)
            return f

        def close_and_update(self, f, close):
            c = self.cache[f.path]
            if (c['blocks'] is not True
                    and len(['blocks']) * f.blocksize >= f.size):
                c['blocks'] = True
            self.save_cache()
            close()

    return CachingFileSystem(**storage_options)
