import fsspec


def test_1():
    m = fsspec.get_filesystem_class('memory')()
    m.touch('/somefile')  # NB: is found with or without initial /
    m.touch('afiles/and/anothers')
    assert m.find('') == ['somefile', 'afiles/and/anothers']
    assert list(m.get_mapper('')) == ['somefile', 'afiles/and/anothers']
