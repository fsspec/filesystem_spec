import pytest

from fsspec.callbacks import Callback, TqdmCallback


def test_callbacks():
    empty_callback = Callback()
    assert empty_callback.call("something", somearg=None) is None

    hooks = {"something": lambda *_, arg=None: arg + 2}
    simple_callback = Callback(hooks=hooks)
    assert simple_callback.call("something", arg=2) == 4

    hooks = {"something": lambda *_, arg1=None, arg2=None: arg1 + arg2}
    multi_arg_callback = Callback(hooks=hooks)
    assert multi_arg_callback.call("something", arg1=2, arg2=2) == 4


def test_callbacks_as_callback():
    empty_callback = Callback.as_callback(None)
    assert empty_callback.call("something", arg="somearg") is None
    assert Callback.as_callback(None) is Callback.as_callback(None)

    hooks = {"something": lambda *_, arg=None: arg + 2}
    real_callback = Callback.as_callback(Callback(hooks=hooks))
    assert real_callback.call("something", arg=2) == 4


def test_callbacks_as_context_manager(mocker):
    spy_close = mocker.spy(Callback, "close")

    with Callback() as cb:
        assert isinstance(cb, Callback)

    spy_close.assert_called_once()


def test_callbacks_branched():
    callback = Callback()
    kwargs = {"key": "value"}

    branch = callback.branched("path_1", "path_2", kwargs)

    assert branch is not callback
    assert isinstance(branch, Callback)
    assert kwargs == {"key": "value"}


@pytest.mark.asyncio
async def test_callbacks_branch_coro(mocker):
    async_fn = mocker.AsyncMock(return_value=10)
    callback = Callback()
    wrapped_fn = callback.branch_coro(async_fn)
    spy = mocker.spy(callback, "branched")

    assert await wrapped_fn("path_1", "path_2", key="value") == 10

    spy.assert_called_once_with("path_1", "path_2", {"key": "value"})
    async_fn.assert_called_once_with(
        "path_1", "path_2", callback=spy.spy_return, key="value"
    )


def test_callbacks_wrap():
    events = []

    class TestCallback(Callback):
        def relative_update(self, inc=1):
            events.append(inc)

    callback = TestCallback()
    for _ in callback.wrap(range(10)):
        ...

    assert events == [1] * 10


@pytest.mark.parametrize("tqdm_kwargs", [{}, {"desc": "A custom desc"}])
def test_tqdm_callback(tqdm_kwargs, mocker):

    callback = TqdmCallback(tqdm_kwargs=tqdm_kwargs)
    mocker.patch.object(callback, "_tqdm_cls")
    callback.set_size(10)
    for _ in callback.wrap(range(10)):
        ...

    assert callback.tqdm.update.call_count == 11
    if not tqdm_kwargs:
        callback._tqdm_cls.assert_called_with(total=10)
    else:
        callback._tqdm_cls.assert_called_with(total=10, **tqdm_kwargs)
