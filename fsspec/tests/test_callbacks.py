from fsspec import callbacks


def test_callbacks():
    empty_callback = callbacks.callback()
    assert empty_callback.call("something", "somearg") is None

    simple_callback = callbacks.callback(something=lambda arg: arg + 2)
    assert simple_callback.call("something", 2) == 4

    multi_arg_callback = callbacks.callback(something=lambda arg1, arg2: arg1 + arg2)
    assert multi_arg_callback.call("something", 2, 2) == 4


def test_callbacks_as_callback():
    empty_callback = callbacks.as_callback(None)
    assert empty_callback.call("something", "somearg") is None
    assert callbacks.as_callback(None) is callbacks.as_callback(None)

    real_callback = callbacks.as_callback(
        callbacks.callback(something=lambda arg: arg + 2)
    )
    assert real_callback.call("something", 2) == 4


def test_callbacks_lazy_call():
    empty_callback = callbacks.as_callback(None)
    simple_callback = callbacks.callback(something=lambda arg: arg + 2)

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
    callback = callbacks.callback(relative_update=events.append)
    for _ in callback.wrap(range(10)):
        ...

    assert len(events) == 10
    assert sum(events) == 10
