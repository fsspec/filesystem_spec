import zipfile
from contextlib import contextmanager
import tempfile
import os
import fsspec


@contextmanager
def tempzip(data={}):
    f = tempfile.mkstemp(suffix='zip')[1]
    with zipfile.ZipFile(f, mode='w') as z:
        for k, v in data.items():
            z.writestr(k, v)
    try:
        yield f
    finally:
        try:
            os.remove(f)
        except (IOError, OSError):
            pass


data = {'a': b'',
        'b': b'hello',
        'deeply/nested/path': b"stuff"}


def test_empty():
    with tempzip() as z:
        fs = fsspec.get_filesystem_class('zip')(fo=z)
        assert fs.find('') == []


def test_mapping():
    with tempzip(data) as z:
        fs = fsspec.get_filesystem_class('zip')(fo=z)
        m = fs.get_mapper('')
        assert list(m) == ['a', 'b', 'deeply/nested/path']
        assert m['b'] == data['b']
