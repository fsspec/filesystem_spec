from fsspec.callbacks import Callback


def test_callbacks():
    empty_callback = Callback()
    assert empty_callback.call("something", somearg=None) is None

    simple_callback = Callback(something=lambda *_, arg=None: arg + 2)
    assert simple_callback.call("something", arg=2) == 4

    multi_arg_callback = Callback(
        something=lambda *_, arg1=None, arg2=None: arg1 + arg2
    )
    assert multi_arg_callback.call("something", arg1=2, arg2=2) == 4


def test_callbacks_as_callback():
    empty_callback = Callback.as_callback(None)
    assert empty_callback.call("something", arg="somearg") is None
    assert Callback.as_callback(None) is Callback.as_callback(None)

    real_callback = Callback.as_callback(
        Callback(something=lambda *_, arg=None: arg + 2)
    )
    assert real_callback.call("something", arg=2) == 4


def test_callbacks_lazy_call():
    empty_callback = Callback.as_callback(None)
    simple_callback = Callback(something=lambda *_, arg=None: arg + 2)

    total_called = 0

    def expensive_func(n):
        nonlocal total_called
        total_called += 1
        return n

    assert empty_callback.lazy_call("something", expensive_func, 8) is None
    assert simple_callback.lazy_call("nonexistent callback", expensive_func, 8) is None
    assert total_called == 0

    assert simple_callback.lazy_call("something", expensive_func, 8) == 10
    assert total_called == 1


def test_callbacks_wrap():
    events = []
    callback = Callback(relative_update=lambda _1, _2: events.append(_2))
    for _ in callback.wrap(range(10)):
        ...

    assert len(events) == 10
    assert sum(events) == sum(range(1, 11))
