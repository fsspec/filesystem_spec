Copying files and directories
=============================

This documents the expected behavior of the ``fsspec``  file and directory copying functions.
There are three functions of interest here: :meth:`~fsspec.spec.AbstractFileSystem.copy`,
:meth:`~fsspec.spec.AbstractFileSystem.get` and :meth:`~fsspec.spec.AbstractFileSystem.put`.
Each of these copies files and/or directories from a ``source`` to a ``target`` location.
If we refer to our filesystem of interest, derived from :class:`~fsspec.spec.AbstractFileSystem`,
as the remote filesystem (even though it may be local) then the difference between the three
functions is:

    - :meth:`~fsspec.spec.AbstractFileSystem.copy` copies from a remote ``source`` to a remote ``target``
    - :meth:`~fsspec.spec.AbstractFileSystem.get` copies from a remote ``source`` to a local ``target``
    - :meth:`~fsspec.spec.AbstractFileSystem.put` copies from a local ``source`` to a remote ``target``

The ``source`` and ``target`` are the first two arguments passed to these functions, and each
consists of one or more files, directories and/or ``glob`` (wildcard) patterns.
The behavior of the ``fsspec`` copy functions is intended to be the same as that obtained using
POSIX command line ``cp`` but the ``fsspec`` functions have extra functionality because:

    - They support more than one ``target`` whereas command line ``cp`` is restricted to one.
    - They can create new directories, either automatically or via the ``auto_mkdir`` kwarg,
      whereas command line ``cp`` only does this as part of a recursive copy.

Expected behavior
-----------------

There follows a comprehensive list of the expected behavior of the ``fsspec`` copying functions
that also forms the basis of a set of tests that all classes that derive from
:class:`~fsspec.spec.AbstractFileSystem` can be tested against to confirm that they conform.
For all scenarios the ``source`` filesystem contains the following directories and files::

    📁 source
    ├── 📄 file1
    ├── 📄 file2
    └── 📁 subdir
        ├── 📄 subfile1
        ├── 📄 subfile2
        └── 📁 nesteddir
            └── 📄 nestedfile

and before each scenario the ``target`` directory exists and is empty unless otherwise noted::

    📁 target

All example code uses :meth:`~fsspec.spec.AbstractFileSystem.cp` which is an alias of
:meth:`~fsspec.spec.AbstractFileSystem.copy`; equivalent behavior is produced by
:meth:`~fsspec.spec.AbstractFileSystem.get` and :meth:`~fsspec.spec.AbstractFileSystem.put`.
Forward slashes are used for directory separators throughout.

1. Single source to single target
---------------------------------

.. dropdown:: 1a. File to existing directory

    .. code-block:: python

        cp("source/subdir/subfile1", "target")

    results in::

        📁 target
        └── 📄 subfile1

    The same result is obtained if the target has a trailing slash: ``"target/"``.

.. dropdown:: 1b. File to new directory

    .. code-block:: python

        cp("source/subdir/subfile1", "target/newdir/")

    results in::

        📁 target
        └── 📁 newdir
            └── 📄 subfile1

    This fails if the ``target`` file system is not capable of creating the directory, for example
    if it is write-only or if ``auto_mkdir=False``. There is no command line equivalent of this
    scenario without an explicit ``mkdir`` to create the new directory.

    The trailing slash is required on the new directory otherwise it is interpreted as a filename
    which is a different scenario (1d. File to file in new directory).

.. dropdown:: 1c. File to file in existing directory

    .. code-block:: python

        cp("source/subdir/subfile1", "target/newfile")

    results in::

        📁 target
        └── 📄 newfile

    The same result is obtained if the target has a trailing slash: ``target/newfile/``.

.. dropdown:: 1d. File to file in new directory

    .. code-block:: python

        cp("source/subdir/subfile1", "target/newdir/newfile")

    creates the new directory and copies the file into it::

        📁 target
        └── 📁 newdir
            └── 📄 newfile

    This fails if the ``target`` file system is not capable of creating the directory, for example
    if it is write-only or if ``auto_mkdir=False``. There is no command line equivalent of this
    scenario without an explicit ``mkdir`` to create the new directory.

    If there is a trailing slash on the target ``target/newdir/newfile/`` then it is interpreted as
    a new directory which is a different scenario (1b. File to new directory).

2. Multiple source to single target
-----------------------------------
