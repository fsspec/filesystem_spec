import os
import fsspec


def test_mapping_prefix(tmpdir):
    tmpdir = str(tmpdir)
    os.makedirs(os.path.join(tmpdir, 'afolder'))
    open(os.path.join(tmpdir, 'afile'), 'w').write('test')
    open(os.path.join(tmpdir, 'afolder', 'anotherfile'), 'w').write('test2')

    m = fsspec.get_mapper('file://' + tmpdir)
    assert 'afile' in m
    assert m['afolder/anotherfile'] == b'test2'

    fs = fsspec.filesystem('file')
    m2 = fs.get_mapper(tmpdir)
    m3 = fs.get_mapper('file://' + tmpdir)

    assert m == m2 == m3
