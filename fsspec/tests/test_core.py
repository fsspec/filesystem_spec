import os
import pytest
from fsspec.core import _expand_paths, OpenFile
from fsspec.implementations.tests.test_memory import m


@pytest.mark.parametrize('path, name_function, num, out', [
    [['apath'], None, 1, ['apath']],
    ['apath.*.csv', None, 1, ['apath.0.csv']],
    ['apath.*.csv', None, 2, ['apath.0.csv', 'apath.1.csv']],
    ['a*', lambda x: 'abc'[x], 2, ['aa', 'ab']]
])
def test_expand_paths(path, name_function, num, out):
    assert _expand_paths(path, name_function, num) == out


def test_expand_error():
    with pytest.raises(ValueError):
        _expand_paths("*.*", None, 1)


def test_openfile_api(m):
    m.open('somepath', 'wb').write(b'data')
    of = OpenFile(m, 'somepath')
    assert str(of) == "<OpenFile 'somepath'>"
    assert os.fspath(of) == 'somepath'
    f = of.open()
    assert f.read() == b'data'
    f.close()
    with OpenFile(m, 'somepath', mode='rt') as f:
        f.read() == 'data'
