from __future__ import print_function, division, absolute_import

import gzip
import os
from time import sleep
import sys
from contextlib import contextmanager
import tempfile

import pytest
import fsspec
from fsspec.core import open_files, get_fs_token_paths, OpenFile
from fsspec.implementations.local import LocalFileSystem
from fsspec import compression

files = {'.test.accounts.1.json': (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         '.test.accounts.2.json': (b'{"amount": 500, "name": "Alice"}\n'
                                   b'{"amount": 600, "name": "Bob"}\n'
                                   b'{"amount": 700, "name": "Charlie"}\n'
                                   b'{"amount": 800, "name": "Dennis"}\n')}


csv_files = {'.test.fakedata.1.csv': (b'a,b\n'
                                      b'1,2\n'),
             '.test.fakedata.2.csv': (b'a,b\n'
                                      b'3,4\n')}


@contextmanager
def filetexts(d, open=open, mode='t'):
    """ Dumps a number of textfiles to disk

    d - dict
        a mapping from filename to text like {'a.csv': '1,1\n2,2'}

    Since this is meant for use in tests, this context manager will
    automatically switch to a temporary current directory, to avoid
    race conditions when running tests in parallel.
    """
    odir = os.getcwd()
    dirname = tempfile.mkdtemp()
    try:
        os.chdir(dirname)
        for filename, text in d.items():
            f = open(filename, 'w' + mode)
            try:
                f.write(text)
            finally:
                try:
                    f.close()
                except AttributeError:
                    pass

        yield list(d)

        for filename in d:
            if os.path.exists(filename):
                try:
                    os.remove(filename)
                except (IOError, OSError):
                    pass
    finally:
        os.chdir(odir)


def test_urlpath_inference_strips_protocol(tmpdir):
    tmpdir = str(tmpdir)
    paths = [os.path.join(tmpdir, 'test.%02d.csv' % i) for i in range(20)]

    for path in paths:
        with open(path, 'wb') as f:
            f.write(b'1,2,3\n' * 10)

    # globstring
    protocol = 'file:///' if sys.platform == 'win32' else 'file://'
    urlpath = protocol + os.path.join(tmpdir, 'test.*.csv')
    _, _, paths2 = get_fs_token_paths(urlpath)
    assert paths2 == paths

    # list of paths
    _, _, paths2 = get_fs_token_paths([protocol + p for p in paths])
    assert paths2 == paths


def test_urlpath_inference_errors():
    # Empty list
    with pytest.raises(ValueError) as err:
        get_fs_token_paths([])
    assert 'empty' in str(err.value)

    # Protocols differ
    with pytest.raises(ValueError) as err:
        get_fs_token_paths(['s3://test/path.csv', '/other/path.csv'])
    assert 'same protocol' in str(err.value)

    # Unknown type
    with pytest.raises(TypeError):
        get_fs_token_paths({'sets/are.csv', 'unordered/so/they.csv',
                            'should/not/be.csv' 'allowed.csv'})


def test_urlpath_expand_read():
    """Make sure * is expanded in file paths when reading."""
    # when reading, globs should be expanded to read files by mask
    with filetexts(csv_files, mode='b'):
        _, _, paths = get_fs_token_paths('./.*.csv')
        assert len(paths) == 2
        _, _, paths = get_fs_token_paths(['./.*.csv'])
        assert len(paths) == 2


def test_urlpath_expand_write():
    """Make sure * is expanded in file paths when writing."""
    _, _, paths = get_fs_token_paths('prefix-*.csv', mode='wb', num=2)
    assert all([p.endswith(pa) for p, pa
                in zip(paths, ['/prefix-0.csv', '/prefix-1.csv'])])
    _, _, paths = get_fs_token_paths(['prefix-*.csv'], mode='wb', num=2)
    assert all([p.endswith(pa) for p, pa
                in zip(paths, ['/prefix-0.csv', '/prefix-1.csv'])])
    # we can read with multiple masks, but not write
    with pytest.raises(ValueError):
        _, _, paths = get_fs_token_paths(['prefix1-*.csv', 'prefix2-*.csv'],
                                         mode='wb', num=2)


def test_open_files():
    with filetexts(files, mode='b'):
        myfiles = open_files('./.test.accounts.*')
        assert len(myfiles) == len(files)
        for lazy_file, data_file in zip(myfiles, sorted(files)):
            with lazy_file as f:
                x = f.read()
                assert x == files[data_file]


@pytest.mark.parametrize('encoding', ['utf-8', 'ascii'])
def test_open_files_text_mode(encoding):
    with filetexts(files, mode='b'):
        myfiles = open_files('./.test.accounts.*', mode='rt', encoding=encoding)
        assert len(myfiles) == len(files)
        data = []
        for file in myfiles:
            with file as f:
                data.append(f.read())
        assert list(data) == [files[k].decode(encoding)
                              for k in sorted(files)]


@pytest.mark.parametrize('mode', ['rt', 'rb'])
@pytest.mark.parametrize('fmt', list(compression.compr))
def test_compressions(fmt, mode, tmpdir):
    tmpdir = str(tmpdir)
    if fmt == 'zip':
        # zip implemented read-only
        pytest.skip()
    fn = os.path.join(tmpdir, '.tmp.getsize')
    fs = LocalFileSystem()
    f = OpenFile(fs, fn, compression=fmt, mode='wb')
    data = b'Long line of readily compressible text'
    with f as fo:
        fo.write(data)
    if fmt is None:
        assert fs.size(fn) == len(data)
    else:
        assert fs.size(fn) != len(data)

    f = OpenFile(fs, fn, compression=fmt, mode=mode)
    with f as fo:
        if mode == 'rb':
            assert fo.read() == data
        else:
            assert fo.read() == data.decode()


def test_bad_compression():
    with filetexts(files, mode='b'):
        for func in [open_files]:
            with pytest.raises(ValueError):
                func('./.test.accounts.*', compression='not-found')


def test_not_found():
    fn = 'not-a-file'
    fs = LocalFileSystem()
    with pytest.raises((FileNotFoundError, OSError)) as e:
        with OpenFile(fs, fn, mode='rb'):
            pass


def test_isfile():
    fs = LocalFileSystem()
    with filetexts(files, mode='b'):
        for f in files.keys():
            assert fs.isfile(f)
        assert not fs.isfile('not-a-file')


def test_isdir():
    fs = LocalFileSystem()
    with filetexts(files, mode='b'):
        for f in files.keys():
            assert fs.isdir(os.path.dirname(os.path.abspath(f)))
            assert not fs.isdir(f)
        assert not fs.isdir('not-a-dir')


@pytest.mark.parametrize('compression_opener',
                         [(None, open), ('gzip', gzip.open)])
def test_open_files_write(tmpdir, compression_opener):
    tmpdir = str(tmpdir)
    compression, opener = compression_opener
    fn = str(tmpdir) + "/*.part"
    files = open_files(fn, num=2, mode='wb', compression=compression)
    assert len(files) == 2
    assert {f.mode for f in files} == {'wb'}
    for fil in files:
        with fil as f:
            f.write(b'000')
    files = sorted(os.listdir(tmpdir))
    assert files == ['0.part', '1.part']

    with opener(os.path.join(tmpdir, files[0]), 'rb') as f:
        d = f.read()
    assert d == b'000'


def test_pickability_of_lazy_files(tmpdir):
    tmpdir = str(tmpdir)
    cloudpickle = pytest.importorskip('cloudpickle')

    with filetexts(files, mode='b'):
        myfiles = open_files('./.test.accounts.*')
        myfiles2 = cloudpickle.loads(cloudpickle.dumps(myfiles))

        for f, f2 in zip(myfiles, myfiles2):
            assert f.path == f2.path
            assert type(f.fs) == type(f2.fs)
            with f as f_open, f2 as f2_open:
                assert f_open.read() == f2_open.read()


def test_abs_paths(tmpdir):
    tmpdir = str(tmpdir)
    here = os.getcwd()
    os.chdir(tmpdir)
    with open('tmp', 'w') as f:
        f.write('hi')
    out = LocalFileSystem().glob('./*')
    assert len(out) == 1
    assert os.sep in out[0]
    assert 'tmp' in out[0]

    # I don't know what this was testing - but should avoid local paths anyway
    # fs = LocalFileSystem()
    os.chdir(here)
    # with fs.open('tmp', 'r') as f:
    #     res = f.read()
    # assert res == 'hi'


def test_get_pyarrow_filesystem():
    pa = pytest.importorskip('pyarrow')

    fs = LocalFileSystem()
    assert isinstance(fs, pa.filesystem.FileSystem)

    class UnknownFileSystem(object):
        pass

    assert not isinstance(UnknownFileSystem(), pa.filesystem.FileSystem)
