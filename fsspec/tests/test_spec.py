import glob
import json
import os
import pickle
import subprocess
from collections import defaultdict

import numpy as np
import pytest

import fsspec
from fsspec.implementations.ftp import FTPFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

PATHS_FOR_GLOB_TESTS = (
    {"name": "test0.json", "type": "file", "size": 100},
    {"name": "test0.yaml", "type": "file", "size": 100},
    {"name": "test0", "type": "directory", "size": 0},
    {"name": "test0/test0.json", "type": "file", "size": 100},
    {"name": "test0/test0.yaml", "type": "file", "size": 100},
    {"name": "test0/test1", "type": "directory", "size": 0},
    {"name": "test0/test1/test0.json", "type": "file", "size": 100},
    {"name": "test0/test1/test0.yaml", "type": "file", "size": 100},
    {"name": "test0/test1/test2", "type": "directory", "size": 0},
    {"name": "test0/test1/test2/test0.json", "type": "file", "size": 100},
    {"name": "test0/test1/test2/test0.yaml", "type": "file", "size": 100},
    {"name": "test0/test2", "type": "directory", "size": 0},
    {"name": "test0/test2/test0.json", "type": "file", "size": 100},
    {"name": "test0/test2/test0.yaml", "type": "file", "size": 100},
    {"name": "test0/test2/test1", "type": "directory", "size": 0},
    {"name": "test0/test2/test1/test0.json", "type": "file", "size": 100},
    {"name": "test0/test2/test1/test0.yaml", "type": "file", "size": 100},
    {"name": "test0/test2/test1/test3", "type": "directory", "size": 0},
    {"name": "test0/test2/test1/test3/test0.json", "type": "file", "size": 100},
    {"name": "test0/test2/test1/test3/test0.yaml", "type": "file", "size": 100},
    {"name": "test1.json", "type": "file", "size": 100},
    {"name": "test1.yaml", "type": "file", "size": 100},
    {"name": "test1", "type": "directory", "size": 0},
    {"name": "test1/test0.json", "type": "file", "size": 100},
    {"name": "test1/test0.yaml", "type": "file", "size": 100},
    {"name": "test1/test0", "type": "directory", "size": 0},
    {"name": "test1/test0/test0.json", "type": "file", "size": 100},
    {"name": "test1/test0/test0.yaml", "type": "file", "size": 100},
    {"name": "special_chars", "type": "directory", "size": 0},
    {"name": "special_chars/f\\oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f.oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f+oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f(oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f)oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f|oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f^oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f$oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f{oo.txt", "type": "file", "size": 100},
    {"name": "special_chars/f}oo.txt", "type": "file", "size": 100},
)

GLOB_POSIX_TESTS = [
    ("nonexistent", []),
    ("test0.json", ["test0.json"]),
    ("test0", ["test0"]),
    ("test0/", ["test0"]),
    ("test1/test0.yaml", ["test1/test0.yaml"]),
    ("test0/test[1-2]", ["test0/test1", "test0/test2"]),
    ("test0/test[1-2]/", ["test0/test1", "test0/test2"]),
    (
        "test0/test[1-2]/*",
        [
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test2/test0.json",
            "test0/test2/test0.yaml",
            "test0/test2/test1",
        ],
    ),
    ("test0/test[1-2]/*.[j]*", ["test0/test1/test0.json", "test0/test2/test0.json"]),
    ("special_chars/f\\oo.*", ["special_chars/f\\oo.txt"]),
    ("special_chars/f.oo.*", ["special_chars/f.oo.txt"]),
    ("special_chars/f+oo.*", ["special_chars/f+oo.txt"]),
    ("special_chars/f(oo.*", ["special_chars/f(oo.txt"]),
    ("special_chars/f)oo.*", ["special_chars/f)oo.txt"]),
    ("special_chars/f|oo.*", ["special_chars/f|oo.txt"]),
    ("special_chars/f^oo.*", ["special_chars/f^oo.txt"]),
    ("special_chars/f$oo.*", ["special_chars/f$oo.txt"]),
    ("special_chars/f{oo.*", ["special_chars/f{oo.txt"]),
    ("special_chars/f}oo.*", ["special_chars/f}oo.txt"]),
    (
        "*",
        [
            "special_chars",
            "test0.json",
            "test0.yaml",
            "test0",
            "test1.json",
            "test1.yaml",
            "test1",
        ],
    ),
    ("*.yaml", ["test0.yaml", "test1.yaml"]),
    (
        "**",
        [
            "special_chars",
            "special_chars/f$oo.txt",
            "special_chars/f(oo.txt",
            "special_chars/f)oo.txt",
            "special_chars/f+oo.txt",
            "special_chars/f.oo.txt",
            "special_chars/f\\oo.txt",
            "special_chars/f^oo.txt",
            "special_chars/f{oo.txt",
            "special_chars/f|oo.txt",
            "special_chars/f}oo.txt",
            "test0.json",
            "test0.yaml",
            "test0",
            "test0/test0.json",
            "test0/test0.yaml",
            "test0/test1",
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test1/test2/test0.json",
            "test0/test1/test2/test0.yaml",
            "test0/test2",
            "test0/test2/test0.json",
            "test0/test2/test0.yaml",
            "test0/test2/test1",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
            "test0/test2/test1/test3/test0.json",
            "test0/test2/test1/test3/test0.yaml",
            "test1.json",
            "test1.yaml",
            "test1",
            "test1/test0.json",
            "test1/test0.yaml",
            "test1/test0",
            "test1/test0/test0.json",
            "test1/test0/test0.yaml",
        ],
    ),
    ("*/", ["special_chars", "test0", "test1"]),
    (
        "**/",
        [
            "special_chars",
            "test0",
            "test0/test1",
            "test0/test1/test2",
            "test0/test2",
            "test0/test2/test1",
            "test0/test2/test1/test3",
            "test1",
            "test1/test0",
        ],
    ),
    ("*/*.yaml", ["test0/test0.yaml", "test1/test0.yaml"]),
    (
        "**/*.yaml",
        [
            "test0.yaml",
            "test0/test0.yaml",
            "test0/test1/test0.yaml",
            "test0/test1/test2/test0.yaml",
            "test0/test2/test0.yaml",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3/test0.yaml",
            "test1.yaml",
            "test1/test0.yaml",
            "test1/test0/test0.yaml",
        ],
    ),
    (
        "*/test1/*",
        ["test0/test1/test0.json", "test0/test1/test0.yaml", "test0/test1/test2"],
    ),
    ("*/test1/*.yaml", ["test0/test1/test0.yaml"]),
    (
        "**/test1/*",
        [
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
            "test1/test0.json",
            "test1/test0.yaml",
            "test1/test0",
        ],
    ),
    (
        "**/test1/*.yaml",
        ["test0/test1/test0.yaml", "test0/test2/test1/test0.yaml", "test1/test0.yaml"],
    ),
    ("*/test1/*/", ["test0/test1/test2"]),
    ("**/test1/*/", ["test0/test1/test2", "test0/test2/test1/test3", "test1/test0"]),
    (
        "*/test1/**",
        [
            "test0/test1",
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test1/test2/test0.json",
            "test0/test1/test2/test0.yaml",
        ],
    ),
    (
        "**/test1/**",
        [
            "test0/test1",
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test1/test2/test0.json",
            "test0/test1/test2/test0.yaml",
            "test0/test2/test1",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
            "test0/test2/test1/test3/test0.json",
            "test0/test2/test1/test3/test0.yaml",
            "test1",
            "test1/test0.json",
            "test1/test0.yaml",
            "test1/test0",
            "test1/test0/test0.json",
            "test1/test0/test0.yaml",
        ],
    ),
    ("*/test1/**/", ["test0/test1", "test0/test1/test2"]),
    (
        "**/test1/**/",
        [
            "test0/test1",
            "test0/test1/test2",
            "test0/test2/test1",
            "test0/test2/test1/test3",
            "test1",
            "test1/test0",
        ],
    ),
    ("test0/*", ["test0/test0.json", "test0/test0.yaml", "test0/test1", "test0/test2"]),
    ("test0/*.yaml", ["test0/test0.yaml"]),
    (
        "test0/**",
        [
            "test0",
            "test0/test0.json",
            "test0/test0.yaml",
            "test0/test1",
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test1/test2/test0.json",
            "test0/test1/test2/test0.yaml",
            "test0/test2",
            "test0/test2/test0.json",
            "test0/test2/test0.yaml",
            "test0/test2/test1",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
            "test0/test2/test1/test3/test0.json",
            "test0/test2/test1/test3/test0.yaml",
        ],
    ),
    ("test0/*/", ["test0/test1", "test0/test2"]),
    (
        "test0/**/",
        [
            "test0",
            "test0/test1",
            "test0/test1/test2",
            "test0/test2",
            "test0/test2/test1",
            "test0/test2/test1/test3",
        ],
    ),
    ("test0/*/*.yaml", ["test0/test1/test0.yaml", "test0/test2/test0.yaml"]),
    (
        "test0/**/*.yaml",
        [
            "test0/test0.yaml",
            "test0/test1/test0.yaml",
            "test0/test1/test2/test0.yaml",
            "test0/test2/test0.yaml",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3/test0.yaml",
        ],
    ),
    (
        "test0/*/test1/*",
        [
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
        ],
    ),
    ("test0/*/test1/*.yaml", ["test0/test2/test1/test0.yaml"]),
    (
        "test0/**/test1/*",
        [
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
        ],
    ),
    (
        "test0/**/test1/*.yaml",
        ["test0/test1/test0.yaml", "test0/test2/test1/test0.yaml"],
    ),
    ("test0/*/test1/*/", ["test0/test2/test1/test3"]),
    ("test0/**/test1/*/", ["test0/test1/test2", "test0/test2/test1/test3"]),
    (
        "test0/*/test1/**",
        [
            "test0/test2/test1",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
            "test0/test2/test1/test3/test0.json",
            "test0/test2/test1/test3/test0.yaml",
        ],
    ),
    (
        "test0/**/test1/**",
        [
            "test0/test1",
            "test0/test1/test0.json",
            "test0/test1/test0.yaml",
            "test0/test1/test2",
            "test0/test1/test2/test0.json",
            "test0/test1/test2/test0.yaml",
            "test0/test2/test1",
            "test0/test2/test1/test0.json",
            "test0/test2/test1/test0.yaml",
            "test0/test2/test1/test3",
            "test0/test2/test1/test3/test0.json",
            "test0/test2/test1/test3/test0.yaml",
        ],
    ),
    ("test0/*/test1/**/", ["test0/test2/test1", "test0/test2/test1/test3"]),
    (
        "test0/**/test1/**/",
        [
            "test0/test1",
            "test0/test1/test2",
            "test0/test2/test1",
            "test0/test2/test1/test3",
        ],
    ),
]


class DummyTestFS(AbstractFileSystem):
    protocol = "mock"
    _file_class = AbstractBufferedFile
    _fs_contents = (
        {"name": "top_level", "type": "directory"},
        {"name": "top_level/second_level", "type": "directory"},
        {"name": "top_level/second_level/date=2019-10-01", "type": "directory"},
        {
            "name": "top_level/second_level/date=2019-10-01/a.parquet",
            "type": "file",
            "size": 100,
        },
        {
            "name": "top_level/second_level/date=2019-10-01/b.parquet",
            "type": "file",
            "size": 100,
        },
        {"name": "top_level/second_level/date=2019-10-02", "type": "directory"},
        {
            "name": "top_level/second_level/date=2019-10-02/a.parquet",
            "type": "file",
            "size": 100,
        },
        {"name": "top_level/second_level/date=2019-10-04", "type": "directory"},
        {
            "name": "top_level/second_level/date=2019-10-04/a.parquet",
            "type": "file",
            "size": 100,
        },
        {"name": "misc", "type": "directory"},
        {"name": "misc/foo.txt", "type": "file", "size": 100},
    )

    def __init__(self, fs_content=None, **kwargs):
        if fs_content is not None:
            self._fs_contents = fs_content
        super().__init__(**kwargs)

    def __getitem__(self, name):
        for item in self._fs_contents:
            if item["name"] == name:
                return item
        raise IndexError("{name} not found!".format(name=name))

    def ls(self, path, detail=True, refresh=True, **kwargs):
        if kwargs.pop("strip_proto", True):
            path = self._strip_protocol(path)

        files = not refresh and self._ls_from_cache(path)
        if not files:
            files = [
                file for file in self._fs_contents if path == self._parent(file["name"])
            ]
            files.sort(key=lambda file: file["name"])
            self.dircache[path.rstrip("/")] = files

        if detail:
            return files
        return [file["name"] for file in files]

    @classmethod
    def get_test_paths(cls, start_with=""):
        """Helper to return directory and file paths with no details"""
        all = [
            file["name"]
            for file in cls._fs_contents
            if file["name"].startswith(start_with)
        ]
        return all

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return self._file_class(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )


@pytest.mark.parametrize(
    ["test_paths", "recursive", "maxdepth", "expected"],
    [
        (
            (
                "top_level/second_level",
                "top_level/sec*",
                "top_level/sec*vel",
                "top_level/*",
            ),
            True,
            None,
            [
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            (
                "top_level/second_level",
                "top_level/sec*",
                "top_level/sec*vel",
                "top_level/*",
            ),
            False,
            None,
            [
                "top_level/second_level",
            ],
        ),
        (
            ("top_level/second_level",),
            True,
            1,
            [
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-04",
            ],
        ),
        (
            ("top_level/second_level",),
            True,
            2,
            [
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            ("top_level/*", "top_level/sec*", "top_level/sec*vel", "top_level/*"),
            True,
            1,
            ["top_level/second_level"],
        ),
        (
            ("top_level/*", "top_level/sec*", "top_level/sec*vel", "top_level/*"),
            True,
            2,
            [
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-04",
            ],
        ),
        (
            ("top_level/**",),
            False,
            None,
            [
                "top_level",
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            ("top_level/**",),
            True,
            None,
            [
                "top_level",
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (("top_level/**",), True, 1, ["top_level", "top_level/second_level"]),
        (
            ("top_level/**",),
            True,
            2,
            [
                "top_level",
                "top_level/second_level",
                "top_level/second_level/date=2019-10-01",
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-01/b.parquet",
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            ("top_level/**/a.*",),
            False,
            None,
            [
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            ("top_level/**/a.*",),
            True,
            None,
            [
                "top_level/second_level/date=2019-10-01/a.parquet",
                "top_level/second_level/date=2019-10-02/a.parquet",
                "top_level/second_level/date=2019-10-04/a.parquet",
            ],
        ),
        (
            ("top_level/**/second_level/date=2019-10-02",),
            False,
            2,
            [
                "top_level/second_level/date=2019-10-02",
            ],
        ),
        (
            ("top_level/**/second_level/date=2019-10-02",),
            True,
            2,
            [
                "top_level/second_level/date=2019-10-02",
                "top_level/second_level/date=2019-10-02/a.parquet",
            ],
        ),
        [("misc/foo.txt", "misc/*.txt"), False, None, ["misc/foo.txt"]],
        [("misc/foo.txt", "misc/*.txt"), True, None, ["misc/foo.txt"]],
        (
            ("",),
            False,
            None,
            [DummyTestFS.root_marker],
        ),
        (
            ("",),
            True,
            None,
            DummyTestFS.get_test_paths() + [DummyTestFS.root_marker],
        ),
    ],
)
def test_expand_path(test_paths, recursive, maxdepth, expected):
    """Test a number of paths and then their combination which should all yield
    the same set of expanded paths"""
    test_fs = DummyTestFS()

    # test single query
    for test_path in test_paths:
        paths = test_fs.expand_path(test_path, recursive=recursive, maxdepth=maxdepth)
        assert sorted(paths) == sorted(expected)

    # test with all queries
    paths = test_fs.expand_path(
        list(test_paths), recursive=recursive, maxdepth=maxdepth
    )
    assert sorted(paths) == sorted(expected)


def test_expand_paths_with_wrong_args():
    test_fs = DummyTestFS()

    with pytest.raises(ValueError):
        test_fs.expand_path("top_level", recursive=True, maxdepth=0)
    with pytest.raises(ValueError):
        test_fs.expand_path("top_level", maxdepth=0)
    with pytest.raises(FileNotFoundError):
        test_fs.expand_path("top_level/**/second_level/date=2019-10-02", maxdepth=1)
    with pytest.raises(FileNotFoundError):
        test_fs.expand_path("nonexistent/*")


@pytest.mark.xfail
def test_find():
    """Test .find() method on debian server (ftp, https) with constant folder"""
    filesystem, host, test_path = (
        FTPFileSystem,
        "ftp.fau.de",
        "ftp://ftp.fau.de/debian-cd/current/amd64/log/success",
    )
    test_fs = filesystem(host)
    filenames_ftp = test_fs.find(test_path)
    assert filenames_ftp

    filesystem, host, test_path = (
        HTTPFileSystem,
        "https://ftp.fau.de",
        "https://ftp.fau.de/debian-cd/current/amd64/log/success",
    )
    test_fs = filesystem()
    filenames_http = test_fs.find(test_path)
    roots = [f.rsplit("/", 1)[-1] for f in filenames_http]

    assert all(f.rsplit("/", 1)[-1] in roots for f in filenames_ftp)


def test_find_details():
    test_fs = DummyTestFS()
    filenames = test_fs.find("/")
    details = test_fs.find("/", detail=True)
    for filename in filenames:
        assert details[filename] == test_fs.info(filename)


def test_find_file():
    test_fs = DummyTestFS()

    filename = "misc/foo.txt"
    assert test_fs.find(filename) == [filename]
    assert test_fs.find(filename, detail=True) == {filename: {}}


def test_cache():
    fs = DummyTestFS()
    fs2 = DummyTestFS()
    assert fs is fs2

    assert DummyTestFS.current() is fs
    assert len(fs._cache) == 1
    del fs2
    assert len(fs._cache) == 1
    del fs

    # keeps and internal reference, doesn't get collected
    assert len(DummyTestFS._cache) == 1

    DummyTestFS.clear_instance_cache()
    assert len(DummyTestFS._cache) == 0


def test_current():
    fs = DummyTestFS()
    fs2 = DummyTestFS(arg=1)

    assert fs is not fs2
    assert DummyTestFS.current() is fs2

    DummyTestFS()
    assert DummyTestFS.current() is fs


def test_alias():
    with pytest.warns(FutureWarning, match="add_aliases"):
        DummyTestFS(add_aliases=True)


def test_add_docs_warns():
    with pytest.warns(FutureWarning, match="add_docs"):
        AbstractFileSystem(add_docs=True)


def test_cache_options():
    fs = DummyTestFS()
    f = AbstractBufferedFile(fs, "misc/foo.txt", cache_type="bytes")
    assert f.cache.trim

    # TODO: dummy buffered file
    f = AbstractBufferedFile(
        fs, "misc/foo.txt", cache_type="bytes", cache_options=dict(trim=False)
    )
    assert f.cache.trim is False

    f = fs.open("misc/foo.txt", cache_type="bytes", cache_options=dict(trim=False))
    assert f.cache.trim is False


def test_trim_kwarg_warns():
    fs = DummyTestFS()
    with pytest.warns(FutureWarning, match="cache_options"):
        AbstractBufferedFile(fs, "misc/foo.txt", cache_type="bytes", trim=False)


def tests_file_open_error(monkeypatch):
    class InitiateError(ValueError):
        ...

    class UploadError(ValueError):
        ...

    class DummyBufferedFile(AbstractBufferedFile):
        can_initiate = False

        def _initiate_upload(self):
            if not self.can_initiate:
                raise InitiateError

        def _upload_chunk(self, final=False):
            raise UploadError

    monkeypatch.setattr(DummyTestFS, "_file_class", DummyBufferedFile)

    fs = DummyTestFS()
    with pytest.raises(InitiateError):
        with fs.open("misc/foo.txt", "wb") as stream:
            stream.write(b"hello" * stream.blocksize * 2)

    with pytest.raises(UploadError):
        with fs.open("misc/foo.txt", "wb") as stream:
            stream.can_initiate = True
            stream.write(b"hello" * stream.blocksize * 2)


def test_eq():
    fs = DummyTestFS()
    result = fs == 1
    assert result is False


def test_pickle_multiple():
    a = DummyTestFS(1)
    b = DummyTestFS(2, bar=1)

    x = pickle.dumps(a)
    y = pickle.dumps(b)

    del a, b
    DummyTestFS.clear_instance_cache()

    result = pickle.loads(x)
    assert result.storage_args == (1,)
    assert result.storage_options == {}

    result = pickle.loads(y)
    assert result.storage_args == (2,)
    assert result.storage_options == dict(bar=1)


def test_json():
    a = DummyTestFS(1)
    b = DummyTestFS(2, bar=1)

    outa = a.to_json()
    outb = b.to_json()

    assert json.loads(outb)  # is valid JSON
    assert a != b
    assert "bar" in outb

    assert DummyTestFS.from_json(outa) is a
    assert DummyTestFS.from_json(outb) is b


def test_ls_from_cache():
    fs = DummyTestFS()
    uncached_results = fs.ls("top_level/second_level/", refresh=True)

    assert fs.ls("top_level/second_level/", refresh=False) == uncached_results

    # _strip_protocol removes everything by default though
    # for the sake of testing the _ls_from_cache interface
    # directly, we need run one time more without that call
    # to actually verify that our stripping in the client
    # function works.
    assert (
        fs.ls("top_level/second_level/", refresh=False, strip_proto=False)
        == uncached_results
    )


@pytest.mark.parametrize(
    "dt",
    [
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.float32,
        np.float64,
    ],
)
def test_readinto_with_numpy(tmpdir, dt):
    store_path = str(tmpdir / "test_arr.npy")
    arr = np.arange(10, dtype=dt)
    arr.tofile(store_path)

    arr2 = np.empty_like(arr)
    with fsspec.open(store_path, "rb") as f:
        f.readinto(arr2)

    assert np.array_equal(arr, arr2)


@pytest.mark.parametrize(
    "dt",
    [
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.float32,
        np.float64,
    ],
)
def test_readinto_with_multibyte(ftp_writable, tmpdir, dt):
    host, port, user, pw = ftp_writable
    ftp = FTPFileSystem(host=host, port=port, username=user, password=pw)

    with ftp.open("/out", "wb") as fp:
        arr = np.arange(10, dtype=dt)
        fp.write(arr.tobytes())

    with ftp.open("/out", "rb") as fp:
        arr2 = np.empty_like(arr)
        fp.readinto(arr2)

    assert np.array_equal(arr, arr2)


class DummyOpenFS(DummyTestFS):
    blocksize = 10

    def _open(self, path, mode="rb", **kwargs):
        stream = open(path, mode)
        stream.size = os.stat(path).st_size
        return stream


class BasicCallback(fsspec.Callback):
    def __init__(self, **kwargs):
        self.events = []
        super(BasicCallback, self).__init__(**kwargs)

    def set_size(self, size):
        self.events.append(("set_size", size))

    def relative_update(self, inc=1):
        self.events.append(("relative_update", inc))


def imitate_transfer(size, chunk, *, file=True):
    events = [("set_size", size)]
    events.extend(("relative_update", size // chunk) for _ in range(chunk))
    if file:
        # The reason that there is a relative_update(0) at the
        # end is that, we don't have an early exit on the
        # implementations of get_file/put_file so it needs to
        # go through the callback to get catch by the while's
        # condition and then it will stop the transfer.
        events.append(("relative_update", 0))

    return events


def get_files(tmpdir, amount=10):
    src, dest, base = [], [], []
    for index in range(amount):
        src_path = tmpdir / f"src_{index}.txt"
        src_path.write_text("x" * 50, "utf-8")

        src.append(str(src_path))
        dest.append(str(tmpdir / f"dst_{index}.txt"))
        base.append(str(tmpdir / f"file_{index}.txt"))
    return src, dest, base


def test_dummy_callbacks_file(tmpdir):
    fs = DummyOpenFS()
    callback = BasicCallback()

    file = tmpdir / "file.txt"
    source = tmpdir / "tmp.txt"
    destination = tmpdir / "tmp2.txt"

    size = 100
    source.write_text("x" * 100, "utf-8")

    fs.put_file(source, file, callback=callback)

    # -1 here since put_file no longer has final zero-size put
    assert callback.events == imitate_transfer(size, 10)[:-1]
    callback.events.clear()

    fs.get_file(file, destination, callback=callback)
    assert callback.events == imitate_transfer(size, 10)
    callback.events.clear()

    assert destination.read_text("utf-8") == "x" * 100


def test_dummy_callbacks_files(tmpdir):
    fs = DummyOpenFS()
    callback = BasicCallback()
    src, dest, base = get_files(tmpdir)

    fs.put(src, base, callback=callback)
    assert callback.events == imitate_transfer(10, 10, file=False)
    callback.events.clear()

    fs.get(base, dest, callback=callback)
    assert callback.events == imitate_transfer(10, 10, file=False)


class BranchableCallback(BasicCallback):
    def __init__(self, source, dest=None, events=None, **kwargs):
        super(BranchableCallback, self).__init__(**kwargs)
        if dest:
            self.key = source, dest
        else:
            self.key = (source,)
        self.events = events or defaultdict(list)

    def branch(self, path_1, path_2, kwargs):
        from fsspec.implementations.local import make_path_posix

        path_1 = make_path_posix(path_1)
        path_2 = make_path_posix(path_2)
        kwargs["callback"] = BranchableCallback(path_1, path_2, events=self.events)

    def set_size(self, size):
        self.events[self.key].append(("set_size", size))

    def relative_update(self, inc=1):
        self.events[self.key].append(("relative_update", inc))


def test_dummy_callbacks_files_branched(tmpdir):
    fs = DummyOpenFS()
    src, dest, base = get_files(tmpdir)

    callback = BranchableCallback("top-level")

    def check_events(lpaths, rpaths):
        from fsspec.implementations.local import make_path_posix

        base_keys = zip(make_path_posix(lpaths), make_path_posix(rpaths))
        assert set(callback.events.keys()) == {("top-level",), *base_keys}
        assert callback.events[
            "top-level",
        ] == imitate_transfer(10, 10, file=False)

        for key in base_keys:
            assert callback.events[key] == imitate_transfer(50, 5)

    fs.put(src, base, callback=callback)
    check_events(src, base)
    callback.events.clear()

    fs.get(base, dest, callback=callback)
    check_events(base, dest)
    callback.events.clear()


def _clean_paths(paths, prefix=""):
    """
    Helper to cleanup paths results by doing the following:
      - remove the prefix provided from all paths
      - remove the trailing slashes from all paths
      - remove duplicates paths
      - remove empty paths after removing the prefix
      - sort all paths
    """
    paths_list = paths
    if isinstance(paths, dict):
        paths_list = list(paths)
    paths_list = [p.replace(prefix, "").strip("/") for p in paths_list]
    paths_list = [p for p in sorted(set(paths_list)) if p]
    if isinstance(paths, dict):
        return {p: paths[p] for p in paths_list}
    return paths_list


@pytest.fixture(scope="function")
def local_fake_dir(tmp_path):
    fs = LocalFileSystem(auto_mkdir=True)
    for path_info in PATHS_FOR_GLOB_TESTS:
        if path_info["type"] == "file":
            fs.touch(path=f"{str(tmp_path)}/{path_info['name']}")
    return str(tmp_path)


@pytest.fixture(scope="function")
def glob_fs():
    return DummyTestFS(fs_content=PATHS_FOR_GLOB_TESTS)


@pytest.mark.parametrize(
    ("path", "expected"),
    GLOB_POSIX_TESTS,
)
def test_posix_tests(path, expected, local_fake_dir):
    """
    Tests against python glob and bash stat to check if our posix tests are accurate.
    """
    abs_path = f"{local_fake_dir}/{path}"

    # Test python glob
    python_output = glob.glob(pathname=abs_path, recursive=True)
    assert _clean_paths(python_output, local_fake_dir) == _clean_paths(expected)

    # Test bash glob
    bash_abs_path = (
        abs_path.replace("\\", "\\\\")
        .replace("$", "\\$")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("|", "\\|")
    )
    bash_output = subprocess.run(
        ["bash", "-c", f"shopt -s globstar && stat -c %N {bash_abs_path}"],
        capture_output=True,
    )
    bash_output = bash_output.stdout.decode("utf-8").replace("'", "").split("\n")
    assert _clean_paths(bash_output, local_fake_dir) == _clean_paths(expected)


@pytest.mark.parametrize(
    ("path", "expected"),
    GLOB_POSIX_TESTS,
)
def test_glob_posix_rules(path, expected, glob_fs):
    output = glob_fs.glob(path=f"mock://{path}")
    assert _clean_paths(output) == _clean_paths(expected)

    detailed_output = glob_fs.glob(path=f"mock://{path}", detail=True)
    for name, info in _clean_paths(detailed_output).items():
        assert info == glob_fs[name]


@pytest.mark.parametrize(
    ("path", "maxdepth", "expected"),
    [
        (
            "test1**",
            None,
            [
                "test1",
                "test1.json",
                "test1.yaml",
                "test1/test0",
                "test1/test0.json",
                "test1/test0.yaml",
                "test1/test0/test0.json",
                "test1/test0/test0.yaml",
            ],
        ),
        ("test1**/", None, ["test1", "test1/test0"]),
        (
            "**.yaml",
            None,
            [
                "test0.yaml",
                "test0/test0.yaml",
                "test0/test1/test0.yaml",
                "test0/test1/test2/test0.yaml",
                "test0/test2/test0.yaml",
                "test0/test2/test1/test0.yaml",
                "test0/test2/test1/test3/test0.yaml",
                "test1.yaml",
                "test1/test0.yaml",
                "test1/test0/test0.yaml",
            ],
        ),
        ("**1/", None, ["test0/test1", "test0/test2/test1", "test1"]),
        (
            "**1/*.yaml",
            None,
            [
                "test0/test1/test0.yaml",
                "test0/test2/test1/test0.yaml",
                "test1/test0.yaml",
            ],
        ),
        (
            "test0**1**.yaml",
            None,
            [
                "test0/test1/test2/test0.yaml",
                "test0/test1/test0.yaml",
                "test0/test2/test1/test0.yaml",
                "test0/test2/test1/test3/test0.yaml",
            ],
        ),
        (
            "test0/t**.yaml",
            None,
            [
                "test0/test0.yaml",
                "test0/test1/test0.yaml",
                "test0/test1/test2/test0.yaml",
                "test0/test2/test0.yaml",
                "test0/test2/test1/test0.yaml",
                "test0/test2/test1/test3/test0.yaml",
            ],
        ),
        ("test0/t**1/", None, ["test0/test1", "test0/test2/test1"]),
        (
            "test0/t**1/*.yaml",
            None,
            ["test0/test1/test0.yaml", "test0/test2/test1/test0.yaml"],
        ),
        (
            "test0/**",
            1,
            [
                "test0",
                "test0/test0.json",
                "test0/test0.yaml",
                "test0/test1",
                "test0/test2",
            ],
        ),
        (
            "test0/**",
            2,
            [
                "test0",
                "test0/test0.json",
                "test0/test0.yaml",
                "test0/test1",
                "test0/test1/test0.json",
                "test0/test1/test0.yaml",
                "test0/test1/test2",
                "test0/test2",
                "test0/test2/test0.json",
                "test0/test2/test0.yaml",
                "test0/test2/test1",
            ],
        ),
        ("test0/**/test1/*", 1, []),
        (
            "test0/**/test1/*",
            2,
            ["test0/test1/test0.json", "test0/test1/test0.yaml", "test0/test1/test2"],
        ),
        ("test0/**/test1/**", 1, ["test0/test1"]),
        (
            "test0/**/test1/**",
            2,
            [
                "test0/test1",
                "test0/test1/test0.json",
                "test0/test1/test0.yaml",
                "test0/test1/test2",
                "test0/test2/test1",
            ],
        ),
        (
            "test0/test[1-2]/**",
            1,
            [
                "test0/test1",
                "test0/test1/test0.yaml",
                "test0/test1/test0.json",
                "test0/test1/test2",
                "test0/test2",
                "test0/test2/test0.json",
                "test0/test2/test0.yaml",
                "test0/test2/test1",
            ],
        ),
        (
            "test0/test[1-2]/**",
            2,
            [
                "test0/test1",
                "test0/test1/test0.yaml",
                "test0/test1/test0.json",
                "test0/test1/test2",
                "test0/test1/test2/test0.json",
                "test0/test1/test2/test0.yaml",
                "test0/test2",
                "test0/test2/test0.json",
                "test0/test2/test0.yaml",
                "test0/test2/test1",
                "test0/test2/test1/test0.yaml",
                "test0/test2/test1/test0.json",
                "test0/test2/test1/test3",
            ],
        ),
    ],
)
def test_glob_non_posix_rules(path, maxdepth, expected, glob_fs):
    output = glob_fs.glob(path=f"mock://{path}", maxdepth=maxdepth)
    assert _clean_paths(output) == _clean_paths(expected)

    detailed_output = glob_fs.glob(
        path=f"mock://{path}", maxdepth=maxdepth, detail=True
    )
    for name, info in _clean_paths(detailed_output).items():
        assert info == glob_fs[name]


def test_glob_with_wrong_args(glob_fs):
    with pytest.raises(ValueError):
        _ = glob_fs.glob(path="mock://test0/*", maxdepth=0)
