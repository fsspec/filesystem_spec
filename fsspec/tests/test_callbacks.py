from fsspec.callbacks import Callback


def test_callbacks():
    empty_callback = Callback()
    assert empty_callback.call("something", somearg=None) is None

    hooks = dict(something=lambda *_, arg=None: arg + 2)
    simple_callback = Callback(hooks=hooks)
    assert simple_callback.call("something", arg=2) == 4

    hooks = dict(something=lambda *_, arg1=None, arg2=None: arg1 + arg2)
    multi_arg_callback = Callback(hooks=hooks)
    assert multi_arg_callback.call("something", arg1=2, arg2=2) == 4


def test_callbacks_as_callback():
    empty_callback = Callback.as_callback(None)
    assert empty_callback.call("something", arg="somearg") is None
    assert Callback.as_callback(None) is Callback.as_callback(None)

    hooks = dict(something=lambda *_, arg=None: arg + 2)
    real_callback = Callback.as_callback(Callback(hooks=hooks))
    assert real_callback.call("something", arg=2) == 4


def test_callbacks_wrap():
    events = []

    class TestCallback(Callback):
        def relative_update(self, inc=1):
            events.append(inc)

    callback = TestCallback()
    for _ in callback.wrap(range(10)):
        ...

    assert events == [1] * 10
