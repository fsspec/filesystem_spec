from .utils import stringify_path


class Callback:
    def __init__(
        self,
        properties=None,
        size=None,
        value=0,
        stringify_paths=False,
        posixify_paths=False,
        **hooks,
    ):
        self.properties = properties
        self.size = size
        self.value = value
        self.stringify_paths = stringify_paths
        self.posixify_paths = posixify_paths
        self.hooks = hooks

    def set_size(self, size):
        self.size = size
        self.call()

    def absolute_update(self, value):
        self.value = value
        self.call()

    def relative_update(self, inc=1):
        self.value += inc
        self.call()

    def call(self, hook_name=None, **kwargs):
        if hook_name:
            if hook_name not in self.hooks:
                return
            return self.hooks[hook_name](self.size, self.value, **kwargs)
        for hook in self.hooks.values() or []:
            hook(self.size, self.value, **kwargs)

    def wrap(self, iterable):
        """Wrap an iterable to send ``relative_update`` hook
        on each iterations.

        Parameters
        ----------
        iterable: Iterable
            The iterable that is being wrapped
        """
        for item in iterable:
            self.relative_update()
            yield item

    def branch(self, path_1, path_2, kwargs):
        # TODO: mutating kwargs is an odd thing to do
        from .implementations.local import make_path_posix

        if self.stringify_paths:
            path_1 = stringify_path(path_1)
            path_2 = stringify_path(path_2)

        if self.posixify_paths:
            path_1 = make_path_posix(path_1)
            path_2 = make_path_posix(path_2)

        return None

    @classmethod
    def as_callback(cls, maybe_callback=None):
        if maybe_callback is None:
            return _DEFAULT_CALLBACK
        return maybe_callback


class NoOpCallback(Callback):
    def call(self, *args, **kwargs):
        return None


class DotPrinterCallback(Callback):
    # Almost identical to Callback with a hookthat prints a char; here we
    # demonstrate how the outer layer may print "#" and the inner layer "."

    def __init__(self, chr_to_print="#", **kwargs):
        self.chr = chr_to_print
        super().__init__(**kwargs)

    def branch(self, path_1, path_2, kwargs):
        kwargs["callback"] = DotPrinterCallback(".")

    def call(self, **kwargs):
        print(self.chr)


_DEFAULT_CALLBACK = NoOpCallback()
