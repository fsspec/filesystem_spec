class _Callback:
    __slots__ = ["hooks"]

    def __init__(self, hooks=None):
        self.hooks = hooks

    def call(self, hook, *args, **kwargs):
        callback = self.hooks.get(hook)
        if callback is not None:
            return callback(*args, **kwargs)


def callback(*, set_size=None, relative_update=None, absolute_update=None, **hooks):
    return _Callback(
        set_size=set_size,
        relative_update=relative_update,
        absolute_update=absolute_update,
        hooks=hooks,
    )


def is_callback(obj):
    return isinstance(obj, _Callback)
