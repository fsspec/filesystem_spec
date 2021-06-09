class Callback:
    __slots__ = ["hooks"]

    def __init__(self, **hooks):
        self.hooks = hooks

    def call(self, hook, *args, **kwargs):
        """Make a callback to a hook named ``hook``. If it can't
        find the hook, then this function will return None. Otherwise
        the return value of the hook will be used.

        Parameters
        ----------
        hook: str
            The name of the hook
        *args: Any
            All the positional arguments that will be passed
            to the ``hook``, if found.
        **kwargs: Any
            All the keyword arguments that will be passed
            tot the ``hook``, if found.
        """
        callback = self.hooks.get(hook)
        if callback is not None:
            return callback(*args, **kwargs)

    def lazy_call(self, hook, func, *args, **kwargs):
        """Make a callback to a hook named ``hook`` with a
        single value that will be lazily obtained from a call
        to the ``func`` with ``*args`` and ``**kwargs``.

        This method should be used for expensive operations,
        e.g ``len(data)`` since if there is no hook for the
        given ``hook`` parameter, that operation will be wasted.
        With this method, it will only evaluate function, if there
        is a hook attached to the given ``hook`` parameter.

        Parameters
        ----------
        hook: str
            The name of the hook
        func: Callable[..., Any]
            Function that will be called and passed as the argument
            to the hook, if found.
        *args: Any
            All the positional arguments that will be passed
            to the ``func``, if ``hook`` is found.
        **kwargs: Any
            All the keyword arguments that will be passed
            tot the ``func``, if ``hook` is found.
        """
        callback = self.hooks.get(hook)
        if callback is not None:
            return callback(func(*args, **kwargs))


class NoOpCallback(Callback):
    def call(self, hook, *args, **kwargs):
        return None

    def lazy_call(self, hook, *args, **kwargs):
        return None


_DEFAULT_CALLBACK = NoOpCallback()


def callback(
    *,
    set_size=None,
    relative_update=None,
    absolute_update=None,
    properties=None,
    **hooks,
):
    """Create a new callback for filesystem APIs.

    Parameters
    ----------
    set_size: Callable[[Optional[int]], None] (optional)
        When transferring something quantifiable (e.g bytes in a file, or
        number of files), this hook will be called with the total number of
        items. Might set something to None, in that case it should be ignored.

    relative_update: Callable[[int], None] (optional)
        Update the total transferred items relative to the previous position.
        If the current cursor is at N, and a relative_update(Q) happens then
        the current cursor should now point at the N+Q.

    absolute_update: Callable[[int], None] (optional)
        Update the total transferred items to an absolute position. If
        the current cursor is at N, and a absolute_update(Q) happens then
        the current cursor should now point at the Q. If another one happens
        it will override the current value.

    properties: Dict[str, Any] (optional)
        A mapping of config option (callback related) to their values.

    hooks: Callable[..., Any]
        Optional hooks that are not generally available.
    """

    return Callback(
        set_size=set_size,
        relative_update=relative_update,
        absolute_update=absolute_update,
        **hooks,
    )


def as_callback(maybe_callback):
    """Return the no-op callback if the maybe_callback parameter is None

    Parameters
    ----------
    maybe_callback: fsspec.callback.Callback or None

    Returns
    -------
    fsspec.callback.Callback
    """
    if maybe_callback is None:
        return _DEFAULT_CALLBACK
    else:
        return maybe_callback
