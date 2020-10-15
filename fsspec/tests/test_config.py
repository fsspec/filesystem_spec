import os
import pytest
import fsspec
from fsspec.config import conf, set_conf_env, set_conf_files


@pytest.fixture
def clean_conf():
    """Tests should start and end with clean config dict"""
    conf.clear()
    yield
    conf.clear()


def test_from_env(clean_conf):
    env = {'FSSPEC_PROTO_KEY': "value",
           'FSSPEC_PROTO_LONG_KEY': "othervalue",
           'FSSPEC_MALFORMED': "novalue"}
    cd = {}
    set_conf_env(conf_dict=cd, envdict=env)
    assert cd == {"proto": {
        "key": "value",
        "long_key": "othervalue"
    }}


def test_from_file_ini(clean_conf, tmpdir):
    file1 = os.path.join(tmpdir, "1.ini")
    file2 = os.path.join(tmpdir, "2.ini")
    with open(file1, 'w') as f:
        f.write("""[proto]
key=value
other_key:othervalue
overwritten=dont_see
        """)
    with open(file2, 'w') as f:
        f.write("""[proto]
overwritten=see
        """)
    cd = {}
    set_conf_files(tmpdir, cd)
    assert cd == {"proto": {
        "key": "value",
        "other_key": "othervalue",
        "overwritten": "see"
    }}


def test_from_file_json(clean_conf, tmpdir):
    file1 = os.path.join(tmpdir, "1.json")
    file2 = os.path.join(tmpdir, "2.json")
    with open(file1, 'w') as f:
        f.write("""{"proto":
{"key": "value", 
"other_key": "othervalue",
"overwritten": false}}
        """)
    with open(file2, 'w') as f:
        f.write("""{"proto":
{"overwritten": true}}
        """)
    cd = {}
    set_conf_files(tmpdir, cd)
    assert cd == {"proto": {
        "key": "value",
        "other_key": "othervalue",
        "overwritten": True
    }}


def test_apply(clean_conf):
    conf['file'] = {'auto_mkdir': "test"}
    fs = fsspec.filesystem("file")
    assert fs.auto_mkdir == "test"
    fs = fsspec.filesystem("file", auto_mkdir=True)
    assert fs.auto_mkdir is True
