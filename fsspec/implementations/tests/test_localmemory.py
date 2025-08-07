import pytest

import fsspec
from fsspec.implementations.localmemory import LocalMemoryFileSystem


def test_protocol():
    # this should not throw: ValueError: Protocol not known: localmemory
    fsspec.filesystem("localmemory")


def test_init():
    fs1 = LocalMemoryFileSystem()
    fs2 = LocalMemoryFileSystem()

    # check that fs1 and fs2 are different instances of LocalMemoryFileSystem
    assert id(fs1) != id(fs2)
    assert id(fs1.store) != id(fs2.store)
    assert id(fs1.pseudo_dirs) != id(fs2.pseudo_dirs)

    fs1.touch("/fs1.txt")
    fs2.touch("/fs2.txt")
    assert fs1.ls("/", detail=False) == ["/fs1.txt"]
    assert fs2.ls("/", detail=False) == ["/fs2.txt"]
