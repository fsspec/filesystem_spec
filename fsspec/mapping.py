
from collections.abc import MutableMapping
from .registry import get_filesystem_class


class FSMap(MutableMapping):
    """Wrap a FileSystem instance as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    root : string
        prefix for all the files
    fs : FileSystem instance
    check : bool (=True)
        performs a touch at the location, to check for write access.

    Examples
    --------
    >>> fs = FileSystem(**parameters) # doctest: +SKIP
    >>> d = FSMap('my-data/path/', fs) # doctest: +SKIP
    or, more likely
    >>> d = fs.get_mapper('my-data/path/')

    >>> d['loc1'] = b'Hello World' # doctest: +SKIP
    >>> list(d.keys()) # doctest: +SKIP
    ['loc1']
    >>> d['loc1'] # doctest: +SKIP
    b'Hello World'
    """

    def __init__(self, root, fs, check=False, create=False):
        self.fs = fs
        self.root = root.rstrip('/')  # we join on '/' in _key_to_str
        if create:
            self.fs.mkdir(root)
        if check:
            if not self.fs.exists(root):
                raise ValueError("Path %s does not exist. Create "
                                 " with the ``create=True`` keyword" %
                                 root)
            self.fs.touch(root+'/a')
            self.fs.rm(root+'/a')

    def clear(self):
        """Remove all keys below root - empties out mapping
        """
        try:
            self.fs.rm(self.root, True)
            self.fs.mkdir(self.root)
        except (IOError, OSError):
            pass

    def _key_to_str(self, key):
        """Generate full path for the key"""
        if isinstance(key, (tuple, list)):
            key = str(tuple(key))
        else:
            key = str(key)
        return '/'.join([self.root, key]) if self.root else key

    def _str_to_key(self, s):
        """Strip path of to leave key name"""
        return s[len(self.root):].lstrip('/')

    def __getitem__(self, key, default=None):
        """Retrieve data"""
        key = self._key_to_str(key)
        try:
            result = self.fs.cat(key)
        except (IOError, OSError):
            if default is not None:
                return default
            raise KeyError(key)
        return result

    def __setitem__(self, key, value):
        """Store value in key"""
        key = self._key_to_str(key)
        with self.fs.open(key, 'wb') as f:
            f.write(value)

    def keys(self):
        """List currently defined keys"""
        return (self._str_to_key(x)
                for x in self.fs.find(self.root))

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        """Remove key"""
        self.fs.rm(self._key_to_str(key))

    def __contains__(self, key):
        """Does key exist in mapping?"""
        return self.fs.exists(self._key_to_str(key))

    def __len__(self):
        """Number of stored elements"""
        return sum(1 for _ in self.keys())

    def __getstate__(self):
        """Mapping should be pickleable"""
        # TODO: replace with reduce to reinstantiate?
        return self.fs, self.root

    def __setstate__(self, state):
        fs, root = state
        self.fs = fs
        self.root = root


def get_mapper(url, check=False, create=False, **kwargs):
    """Create key-value interface for given URL and options

    The URL will be of the form "protocol://location" and point to the root
    of the mapper required. All keys will be file-names below this location,
    and their values the contents of each key.

    Parameters
    ----------
    url: str
        Root URL of mapping
    check: bool
        Whether to attempt to read from the location before instantiation, to
        check that the mapping does exist
    create: bool
        Whether to make the directory corresponding to the root before
        instantiating

    Returns
    -------
    ``FSMap`` instance, the dict-like key-value store.
    """
    protocol = url.split(':', 1)[0]
    cls = get_filesystem_class(protocol)
    fs = cls(**kwargs)
    # Removing protocol here - could defer to each open() on the backend
    return FSMap(fs._strip_protocol(url), fs, check, create)
