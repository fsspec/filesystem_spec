import configparser
import json
import os


conf = {}
default_conf_dir = os.path.join(os.path.expanduser("~"), ".fsspec")
conf_dir = os.environ.get("FSSPEC_CONFIG_DIR", default_conf_dir)


def set_conf_env():
    for key in os.environ:
        if key.startswith("FSSPEC"):
            _, proto, kwarg = key.split("_", 2)
            conf.setdefault(proto.lower(), {})[kwarg.lower()] = os.environ[key]


def set_conf_files(cdir):
    allfiles = os.listdir(cdir)
    for fn in allfiles:
        if fn.endswith(".ini"):
            ini = configparser.ConfigParser()
            ini.read(os.path.join(cdir, fn))
            for key in ini:
                conf.setdefault(key, {}).update(dict(ini[key]))
        if fn.endswith(".json"):
            js = json.load(open(os.path.join(cdir, fn)))
            for key in js:
                conf.setdefault(key, {}).update(dict(js[key]))


def apply_config(cls, kwargs):
    protos = cls.protocol if isinstance(cls.protocol, tuple) else cls.protocol
    for proto in protos:
        # set default kwargs that have NOT been specified
        # using the current state of the config
        if proto in conf:
            kw = conf[proto].copy()
            kw.update(**kwargs)
            kwargs = kw
    return kwargs


set_conf_files(conf_dir)
set_conf_env()
