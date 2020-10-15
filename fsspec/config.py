import configparser
import json
import os


conf = {}
default_conf_dir = os.path.join(os.path.expanduser("~"), ".fsspec")
conf_dir = os.environ.get("FSSPEC_CONFIG_DIR", default_conf_dir)


def set_conf_env(conf_dict):
    """Set config values from environment variables

    Looks for variable of the form ``FSSPEC_<protocol>_<kwarg>``.
    There is no attempt to convert strings.

    Parameters
    ----------
    conf_dict : dict(str, dict)
        This dict will be mutated
    """
    for key in os.environ:
        if key.startswith("FSSPEC"):
            _, proto, kwarg = key.split("_", 2)
            conf_dict.setdefault(proto.lower(), {})[kwarg.lower()] = os.environ[key]


def set_conf_files(cdir, conf_dict):
    """Set config values from files

    Scans for INI and JSON files in the given dictionary, and uses their
    contents to set the config. In case of repeated values, later values
    win.

    In the case of INI files, all values are strings, and these will not
    be converted.

    Parameters
    ----------
    cdir : str
        Directory to search
    conf_dict : dict(str, dict)
        This dict will be mutated
    """
    allfiles = os.listdir(cdir)
    for fn in allfiles:
        if fn.endswith(".ini"):
            ini = configparser.ConfigParser()
            ini.read(os.path.join(cdir, fn))
            for key in ini:
                conf_dict.setdefault(key, {}).update(dict(ini[key]))
        if fn.endswith(".json"):
            js = json.load(open(os.path.join(cdir, fn)))
            for key in js:
                conf_dict.setdefault(key, {}).update(dict(js[key]))


def apply_config(cls, kwargs, conf_dict=conf):
    """Supply default values for kwargs when instantiating class

    Augments the passed kwargs, by finding entries in the config dict
    which match the classes ``.protocol`` attribute (one or more str)

    Parameters
    ----------
    cls : file system implementation
    kwargs : dict
    conf_dict : dict of dict
        Typically this is the global configuration

    Returns
    -------
    dict : the modified set of kwargs
    """
    protos = cls.protocol if isinstance(cls.protocol, tuple) else cls.protocol
    kw = {}
    for proto in protos:
        # default kwargs from the current state of the config
        if proto in conf_dict:
            kw.update(conf_dict[proto])
    # explicit kwargs always win
    kw.update(**kwargs)
    kwargs = kw
    return kwargs


set_conf_files(conf_dir, conf)
set_conf_env(conf)
