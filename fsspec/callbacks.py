class Callback:
    def __init__(self, size=None, value=0, hooks=None, **kwargs):
        self.size = size
        self.value = value
        self.hooks = hooks or {}
        self.kw = kwargs

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
        if not self.hooks:
            return
        kw = self.kw.copy()
        kw.update(kwargs)
        if hook_name:
            if hook_name not in self.hooks:
                return
            return self.hooks[hook_name](self.size, self.value, **kw)
        for hook in self.hooks.values() or []:
            hook(self.size, self.value, **kw)

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
        return None

    def no_op(self, *_, **__):
        pass

    def __getattr__(self, item):
        return self.no_op

    @classmethod
    def as_callback(cls, maybe_callback=None):
        if maybe_callback is None:
            return _DEFAULT_CALLBACK
        return maybe_callback


class NoOpCallback(Callback):
    def call(self, *args, **kwargs):
        return None


class DotPrinterCallback(Callback):
    # Almost identical to Callback with a hook that prints a char; here we
    # demonstrate how the outer layer may print "#" and the inner layer "."

    def __init__(self, chr_to_print="#", **kwargs):
        self.chr = chr_to_print
        super().__init__(**kwargs)

    def branch(self, path_1, path_2, kwargs):
        kwargs["callback"] = DotPrinterCallback(".")

    def call(self, **kwargs):
        print(self.chr)


_DEFAULT_CALLBACK = NoOpCallback()
