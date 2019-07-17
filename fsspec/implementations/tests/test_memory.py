import pytest
import fsspec


@pytest.fixture()
def m():
    m = fsspec.filesystem('memory')
    m.store.clear()
    try:
        yield m
    finally:
        m.store.clear()


def test_1(m):
    m.touch('/somefile')  # NB: is found with or without initial /
    m.touch('afiles/and/anothers')
    assert m.find('') == ['somefile', 'afiles/and/anothers']
    assert list(m.get_mapper('')) == ['somefile', 'afiles/and/anothers']


def test_ls(m):
    m.touch('/dir/afile')
    m.touch('/dir/dir1/bfile')
    m.touch('/dir/dir1/cfile')

    assert m.ls('/', False) == ['/dir/']
    assert m.ls('/dir', False) == ['/dir/afile', '/dir/dir1/']
    assert m.ls('/dir', True)[0]['type'] == 'file'
    assert m.ls('/dir', True)[1]['type'] == 'directory'

    assert len(m.ls('/dir/dir1')) == 2
