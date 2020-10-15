import os


conf = {}


def set_conf_env():
    for key in os.environ:
        if key.startswith("FSSPEC"):
            _, proto, kwarg = key.split("_", 2)
            conf.setdefault(proto.lower(), {})[kwarg.lower()] = os.environ[key]


set_conf_env()
