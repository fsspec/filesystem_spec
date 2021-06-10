from .utils import stringify_path


class Callback:
    __slots__ = ["properties", "hooks"]

    def __init__(self, properties=None, **hooks):
        self.hooks = hooks
        self.properties = properties or {}

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

    def wrap(self, iterable):
        """Wrap an iterable to send ``relative_update`` hook
        on each iterations.

        Parameters
        ----------
        iterable: Iterable
            The iterable that is being wrapped
        """
        for item in iterable:
            self.call("relative_update", 1)
            yield item


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
    branch=None,
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

    branch: Callable[[os.PathLike, os.PathLike], Optional[fsspec.callbacks.Callback]] (optional)
        When some operations need branching (e.g each ``put()``/``get()`
        operation have their own callbacks, but they will also need to
        branch out for ``put_file()``/``get_file()`` since those might
        require additional child callbacks) the branch hook will be called
        with the paths that are being transffered and it is expected to
        either return a new fsspec.callbacks.Callback instance or None. The
        arguments will be sanitized and will always use the posix convention.

    properties: Dict[str, Any] (optional)
        A mapping of config option (callback related) to their values.

    hooks: Callable[..., Any]
        Optional hooks that are not generally available.

    Returns
    -------
    fsspec.callback.Callback
    """  # noqa: E501

    return Callback(
        properties=properties,
        set_size=set_size,
        relative_update=relative_update,
        absolute_update=absolute_update,
        branch=branch,
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


def branch(callback, path_1, path_2, kwargs=None):
    """Branch out from an existing callback.

    Parameters
    ----------
    callback: fsspec.callback.Callback
        Parent callback
    path_1: os.PathLike
        Left path
    path_2: os.PathLike
        Right path
    kwargs: Dict[str, Any] (optional)
        Update the ``callback`` key on the given ``kwargs``
        if there is a brancher attached to the ``callback``.


    Returns
    -------
    fsspec.callback.Callback or None
    """
    from .implementations.local import make_path_posix

    path_1 = make_path_posix(stringify_path(path_1))
    path_2 = make_path_posix(stringify_path(path_2))
    branched = callback.call("branch", path_1, path_2)
    if branched is None or branched is _DEFAULT_CALLBACK:
        return None

    if kwargs is not None:
        kwargs["callback"] = branched
    return branched
