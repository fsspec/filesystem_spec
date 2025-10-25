import os
import shutil
import subprocess
import sys
import time
from collections import deque
from collections.abc import Generator, Sequence

import pytest

import fsspec


@pytest.fixture()
def m():
    """
    Fixture providing a memory filesystem.
    """
    m = fsspec.filesystem("memory")
    m.store.clear()
    m.pseudo_dirs.clear()
    m.pseudo_dirs.append("")
    try:
        yield m
    finally:
        m.store.clear()
        m.pseudo_dirs.clear()
        m.pseudo_dirs.append("")


class InstanceCacheInspector:
    """
    Helper class to inspect instance caches of filesystem classes in tests.
    """

    @staticmethod
    def classes_from_refs(
        cls_reference: "tuple[str | type[fsspec.AbstractFileSystem], ...]",
        /,
        *,
        empty_is_all: bool = True,
    ) -> deque[type[fsspec.AbstractFileSystem]]:
        """
        Convert class references (strings or types) to a deque of filesystem classes.

        Parameters
        ----------
        cls_reference:
            Tuple of class references as strings or types.
            Supports fqns, protocol names, or the class types themselves.
        empty_is_all:
            If True and no classes are specified, include all imported filesystem classes.

        Returns
        -------
        fs_classes:
            Deque of filesystem classes corresponding to the provided references.
        """
        classes: deque[type[fsspec.AbstractFileSystem]] = deque()

        for ref in cls_reference:
            if isinstance(ref, str):
                try:
                    cls = fsspec.get_filesystem_class(ref)
                except ValueError:
                    module_name, _, class_name = ref.rpartition(".")
                    module = __import__(module_name, fromlist=[class_name])
                    cls = getattr(module, class_name)
                classes.append(cls)
            else:
                classes.append(ref)
        if empty_is_all and not classes:
            classes.append(fsspec.spec.AbstractFileSystem)
        return classes

    def clear(
        self,
        *cls_reference: "str | type[fsspec.AbstractFileSystem]",
        recursive: bool = True,
    ) -> None:
        """
        Clear instance caches of specified filesystem classes.
        """
        classes = self.classes_from_refs(cls_reference)
        # Clear specified classes and optionally their subclasses
        while classes:
            cls = classes.popleft()
            cls.clear_instance_cache()
            if recursive:
                subclasses = cls.__subclasses__()
                classes.extend(subclasses)

    def gather_counts(
        self,
        *cls_reference: "str | type[fsspec.AbstractFileSystem]",
        omit_zero: bool = True,
        recursive: bool = True,
    ) -> dict[str, int]:
        """
        Gather counts of filesystem instances in the instance caches of all loaded classes.

        Parameters
        ----------
        cls_reference:
            class references as strings or types.
        omit_zero:
            Whether to omit instance types with no cached instances.
        recursive:
            Whether to include subclasses of the specified classes.
        """
        out: dict[str, int] = {}
        classes = self.classes_from_refs(cls_reference)
        while classes:
            cls = classes.popleft()
            count = len(cls._cache)
            # note: skip intermediate AbstractFileSystem subclasses
            #   if they proxy the protocol attribute via a property.
            if isinstance(cls.protocol, (Sequence, str)):
                key = cls.protocol if isinstance(cls.protocol, str) else cls.protocol[0]
                if count or not omit_zero:
                    out[key] = count
            if recursive:
                subclasses = cls.__subclasses__()
                classes.extend(subclasses)
        return out


@pytest.fixture(scope="function", autouse=True)
def instance_caches() -> Generator[InstanceCacheInspector, None, None]:
    """
    Fixture to ensure empty filesystem instance caches before and after a test.

    Used by default for all tests.
    Clears caches of all imported filesystem classes.
    Can be used to write test assertions about instance caches.

    Usage:

        def test_something(instance_caches):
            # Test code here
            fsspec.open("file://abc")
            fsspec.open("memory://foo/bar")

            # Test assertion
            assert instance_caches.gather_counts() == {"file": 1, "memory": 1}

    Returns
    -------
    instance_caches: An instance cache inspector for clearing and inspecting caches.
    """
    ic = InstanceCacheInspector()

    ic.clear()
    try:
        yield ic
    finally:
        ic.clear()


@pytest.fixture(scope="function")
def ftp_writable(tmpdir):
    """
    Fixture providing a writable FTP filesystem.
    """
    pytest.importorskip("pyftpdlib")

    d = str(tmpdir)
    with open(os.path.join(d, "out"), "wb") as f:
        f.write(b"hello" * 10000)
    P = subprocess.Popen(
        [sys.executable, "-m", "pyftpdlib", "-d", d, "-u", "user", "-P", "pass", "-w"]
    )
    try:
        time.sleep(1)
        yield "localhost", 2121, "user", "pass"
    finally:
        P.terminate()
        P.wait()
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass
