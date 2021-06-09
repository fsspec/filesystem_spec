class Callback:
    __slots__ = ["hooks"]

    def __init__(self, **hooks):
        self.hooks = hooks

    def call(self, hook, *args, **kwargs):
        callback = self.hooks.get(hook)
        if callback is not None:
            return callback(*args, **kwargs)


def callback(*, set_size=None, relative_update=None, absolute_update=None, **hooks):
    """Create a new callback for filesystem APIs.

    Parameters
    ----------
    set_size: Callable[[int], None] (optional)
        When transferring something quantifiable (e.g bytes in a file, or
        number of files), this hook will be called with the total number of
        items.

    relative_update: Callable[[int], None] (optional)
        Update the total transferred items relative to the previous position.
        If the current cursor is at N, and a relative_update(Q) happens then
        the current cursor should now point at the N+Q.

    absolute_update: Callable[[int], None] (optional)
        Update the total transferred items to an absolute position. If
        the current cursor is at N, and a absolute_update(Q) happens then
        the current cursor should now point at the Q. If another one happens
        it will override the current value.

    hooks: Callable[..., Any]
        Optional hooks that are not generally available.
    """

    return Callback(
        set_size=set_size,
        relative_update=relative_update,
        absolute_update=absolute_update,
        **hooks,
    )
