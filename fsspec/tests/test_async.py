import inspect

import fsspec.asyn


def test_sync_methods():
    inst = fsspec.asyn.AsyncFileSystem()
    assert inspect.iscoroutinefunction(inst._info)
    assert hasattr(inst, "info")
    assert not inspect.iscoroutinefunction(inst.info)
