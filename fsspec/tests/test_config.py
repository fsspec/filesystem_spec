import os
from warnings import catch_warnings

import pytest

import fsspec
from fsspec.config import conf, set_conf_env, set_conf_files


@pytest.fixture
def clean_conf():
    """Tests should start and end with clean config dict"""
    conf.clear()
    yield
    conf.clear()


def test_from_env_ignored(clean_conf):
    env = {
        "FSSPEC": "missing_protocol",
        "FSSPEC_": "missing_protocol",
        "FSSPEC__INVALID_KEY": "invalid_protocol",
        "FSSPEC_INVALID1": "not_json_dict",
        "FSSPEC_INVALID2": '["not_json_dict"]',
    }
    cd = {}
    with catch_warnings(record=True) as w:
        set_conf_env(conf_dict=cd, envdict=env)
        assert len(w) == 5
        assert "unexpected name" in str(w[0].message)
        assert "unexpected name" in str(w[1].message)
        assert "unexpected name" in str(w[2].message)
        assert "parse failure" in str(w[3].message)
        assert "not being a dict" in str(w[4].message)
    assert cd == {}


def test_from_env_kwargs(clean_conf):
    env = {
        "FSSPEC_PROTO_KEY": "value",
        "FSSPEC_PROTO_LONG_KEY": "othervalue",
        "FSSPEC_MALFORMED": "novalue",
    }
    cd = {}
    with catch_warnings(record=True) as w:
        set_conf_env(conf_dict=cd, envdict=env)
        assert len(w) == 1
        assert "parse failure" in str(w[0].message)
    assert cd == {"proto": {"key": "value", "long_key": "othervalue"}}


def test_from_env_protocol_dict(clean_conf):
    env = {
        "FSSPEC_PROTO": '{"int": 1, "float": 2.3, "bool": true, "dict": {"key": "val"}}'
    }
    cd = {}
    set_conf_env(conf_dict=cd, envdict=env)
    assert cd == {
        "proto": {"int": 1, "float": 2.3, "bool": True, "dict": {"key": "val"}}
    }


def test_from_env_kwargs_override_protocol_dict(clean_conf):
    env = {
        "FSSPEC_PROTO_LONG_KEY": "override1",
        "FSSPEC_PROTO": '{"key": "value1", "long_key": "value2", "otherkey": "value3"}',
        "FSSPEC_PROTO_KEY": "override2",
    }
    cd = {}
    set_conf_env(conf_dict=cd, envdict=env)
    assert cd == {
        "proto": {"key": "override2", "long_key": "override1", "otherkey": "value3"}
    }


def test_from_file_ini(clean_conf, tmpdir):
    file1 = os.path.join(tmpdir, "1.ini")
    file2 = os.path.join(tmpdir, "2.ini")
    with open(file1, "w") as f:
        f.write(
            """[proto]
key=value
other_key:othervalue
overwritten=dont_see
        """
        )
    with open(file2, "w") as f:
        f.write(
            """[proto]
overwritten=see
        """
        )
    cd = {}
    set_conf_files(tmpdir, cd)
    assert cd == {
        "proto": {"key": "value", "other_key": "othervalue", "overwritten": "see"}
    }


def test_from_file_json(clean_conf, tmpdir):
    file1 = os.path.join(tmpdir, "1.json")
    file2 = os.path.join(tmpdir, "2.json")
    with open(file1, "w") as f:
        f.write(
            """{"proto":
{"key": "value",
"other_key": "othervalue",
"overwritten": false}}
        """
        )
    with open(file2, "w") as f:
        f.write(
            """{"proto":
{"overwritten": true}}
        """
        )
    cd = {}
    set_conf_files(tmpdir, cd)
    assert cd == {
        "proto": {"key": "value", "other_key": "othervalue", "overwritten": True}
    }


def test_apply(clean_conf):
    conf["file"] = {"auto_mkdir": "test"}
    fs = fsspec.filesystem("file")
    assert fs.auto_mkdir == "test"
    fs = fsspec.filesystem("file", auto_mkdir=True)
    assert fs.auto_mkdir is True
