import pytest


@pytest.fixture
def prefix():
    """The prefix to use as the root fo the filesystem."""
    raise NotImplementedError("Downstream implementations should define 'prefix'.")


@pytest.fixture
def fs():
    """
    An fsspec-compatible subclass of AbstractFileSystem with the following properties:

    **These files exist with these contents**

    * /<prefix>/exists: data from /exists
    """
    raise NotImplementedError("Downstream implementations should define 'fs'.")
