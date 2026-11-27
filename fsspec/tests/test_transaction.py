import sys
import types

import pytest

from fsspec.implementations.memory import MemoryFileSystem
from fsspec.transaction import DaskTransaction


class _Result:
    def __init__(self, func):
        self.func = func

    def result(self):
        return self.func()


class _FakeActor:
    """Stands in for a dask actor proxy: each call returns a future-like object"""

    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def _call(self, name):
        def run():
            self.calls.append(name)
            if name == self.fail_on:
                raise RuntimeError(f"{name} failed")

        return _Result(run)

    def commit(self):
        return self._call("commit")

    def discard(self):
        return self._call("discard")


@pytest.fixture
def fake_distributed(monkeypatch):
    actor = _FakeActor()
    client = types.SimpleNamespace(
        submit=lambda cls, actor=False: _Result(lambda: client.actor)
    )
    client.actor = actor
    module = types.ModuleType("distributed")
    module.default_client = lambda: client
    monkeypatch.setitem(sys.modules, "distributed", module)
    return actor


@pytest.mark.parametrize(
    "commit, fail_on",
    [(True, None), (False, None), (True, "commit"), (False, "discard")],
)
def test_dask_transaction_complete_ends_transaction(fake_distributed, commit, fail_on):
    fake_distributed.fail_on = fail_on
    fs = MemoryFileSystem(skip_instance_cache=True)
    transaction = DaskTransaction(fs)
    # not transaction.start(): it replaces the dask actor in .files with a deque
    fs._transaction = transaction
    fs._intrans = True

    if fail_on:
        with pytest.raises(RuntimeError):
            transaction.complete(commit=commit)
    else:
        transaction.complete(commit=commit)

    assert fake_distributed.calls == ["commit" if commit else "discard"]
    assert fs._intrans is False
    assert fs._transaction is None
    assert transaction.fs is None
