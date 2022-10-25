import bz2
import gzip
import lzma
import os
import pickle
import tarfile
import tempfile
import zipfile
from contextlib import contextmanager
from io import BytesIO

import pytest

import fsspec

# The blueprint to create synthesized archive files from.
archive_data = {"a": b"", "b": b"hello", "deeply/nested/path": b"stuff"}


@contextmanager
def tempzip(data=None):
    """
    Provide test cases with temporary synthesized Zip archives.
    """
    data = data or {}
    f = tempfile.mkstemp(suffix=".zip")[1]
    with zipfile.ZipFile(f, mode="w") as z:
        for k, v in data.items():
            z.writestr(k, v)
    try:
        yield f
    finally:
        try:
            os.remove(f)
        except (IOError, OSError):
            pass


@contextmanager
def temparchive(data=None):
    """
    Provide test cases with temporary synthesized 7-Zip archives.
    """
    data = data or {}
    libarchive = pytest.importorskip("libarchive")
    f = tempfile.mkstemp(suffix=".7z")[1]
    with libarchive.file_writer(f, "7zip") as archive:
        for k, v in data.items():
            archive.add_file_from_memory(entry_path=k, entry_size=len(v), entry_data=v)
    try:
        yield f
    finally:
        try:
            os.remove(f)
        except (IOError, OSError):
            pass


@contextmanager
def temptar(data=None, mode="w", suffix=".tar"):
    """
    Provide test cases with temporary synthesized .tar archives.
    """
    data = data or {}
    fn = tempfile.mkstemp(suffix=suffix)[1]
    with tarfile.TarFile.open(fn, mode=mode) as t:
        touched = {}
        for name, data in data.items():

            # Create directory hierarchy.
            # https://bugs.python.org/issue22208#msg225558
            if "/" in name and name not in touched:
                parts = os.path.dirname(name).split("/")
                for index in range(1, len(parts) + 1):
                    info = tarfile.TarInfo("/".join(parts[:index]))
                    info.type = tarfile.DIRTYPE
                    t.addfile(info)
                touched[name] = True

            # Add file content.
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            t.addfile(info, BytesIO(data))

    try:
        yield fn
    finally:
        try:
            os.remove(fn)
        except (IOError, OSError):
            pass


@contextmanager
def temptargz(data=None, mode="w", suffix=".tar.gz"):
    """
    Provide test cases with temporary synthesized .tar.gz archives.
    """

    with temptar(data=data, mode=mode) as tarname:

        fn = tempfile.mkstemp(suffix=suffix)[1]
        with open(tarname, "rb") as tar:
            cf = gzip.GzipFile(filename=fn, mode=mode)
            cf.write(tar.read())
            cf.close()

        try:
            yield fn
        finally:
            try:
                os.remove(fn)
            except (IOError, OSError):
                pass


@contextmanager
def temptarbz2(data=None, mode="w", suffix=".tar.bz2"):
    """
    Provide test cases with temporary synthesized .tar.bz2 archives.
    """

    with temptar(data=data, mode=mode) as tarname:

        fn = tempfile.mkstemp(suffix=suffix)[1]
        with open(tarname, "rb") as tar:
            cf = bz2.BZ2File(filename=fn, mode=mode)
            cf.write(tar.read())
            cf.close()

        try:
            yield fn
        finally:
            try:
                os.remove(fn)
            except (IOError, OSError):
                pass


@contextmanager
def temptarxz(data=None, mode="w", suffix=".tar.xz"):
    """
    Provide test cases with temporary synthesized .tar.xz archives.
    """

    with temptar(data=data, mode=mode) as tarname:

        fn = tempfile.mkstemp(suffix=suffix)[1]
        with open(tarname, "rb") as tar:
            cf = lzma.open(filename=fn, mode=mode, format=lzma.FORMAT_XZ)
            cf.write(tar.read())
            cf.close()

        try:
            yield fn
        finally:
            try:
                os.remove(fn)
            except (IOError, OSError):
                pass


class ArchiveTestScenario:
    """
    Describe a test scenario for any type of archive.
    """

    def __init__(self, protocol=None, provider=None, variant=None):
        # The filesystem protocol identifier. Any of "zip", "tar" or "libarchive".
        self.protocol = protocol
        # A contextmanager function to provide temporary synthesized archives.
        self.provider = provider
        # The filesystem protocol variant identifier. Any of "gz", "bz2" or "xz".
        self.variant = variant


def pytest_generate_tests(metafunc):
    """
    Generate test scenario parametrization arguments with appropriate labels (idlist).

    On the one hand, this yields an appropriate output like::

        fsspec/implementations/tests/test_archive.py::TestArchive::test_empty[zip] PASSED  # noqa

    On the other hand, it will support perfect test discovery, like::

        pytest fsspec -vvv -k "zip or tar or libarchive"

    https://docs.pytest.org/en/latest/example/parametrize.html#a-quick-port-of-testscenarios
    """
    idlist = []
    argnames = ["scenario"]
    argvalues = []
    for scenario in metafunc.cls.scenarios:
        scenario: ArchiveTestScenario = scenario
        label = scenario.protocol
        if scenario.variant:
            label += "-" + scenario.variant
        idlist.append(label)
        argvalues.append([scenario])
    metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")


# Define test scenarios.
scenario_zip = ArchiveTestScenario(protocol="zip", provider=tempzip)
scenario_tar = ArchiveTestScenario(protocol="tar", provider=temptar)
scenario_targz = ArchiveTestScenario(protocol="tar", provider=temptargz, variant="gz")
scenario_tarbz2 = ArchiveTestScenario(
    protocol="tar", provider=temptarbz2, variant="bz2"
)
scenario_tarxz = ArchiveTestScenario(protocol="tar", provider=temptarxz, variant="xz")
scenario_libarchive = ArchiveTestScenario(protocol="libarchive", provider=temparchive)


class TestAnyArchive:
    """
    Validate that all filesystem adapter implementations for archive files
    will adhere to the same specification.
    """

    scenarios = [
        scenario_zip,
        scenario_tar,
        scenario_targz,
        scenario_tarbz2,
        scenario_tarxz,
        scenario_libarchive,
    ]

    def test_repr(self, scenario: ArchiveTestScenario):
        with scenario.provider() as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            assert repr(fs).startswith("<Archive-like object")

    def test_empty(self, scenario: ArchiveTestScenario):
        with scenario.provider() as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            assert fs.find("") == []
            assert fs.find("", withdirs=True) == []
            with pytest.raises(FileNotFoundError):
                fs.info("")
            assert fs.ls("") == []

    def test_glob(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            assert fs.glob("*/*/*th") == ["deeply/nested/path"]

    def test_mapping(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            m = fs.get_mapper()
            assert list(m) == ["a", "b", "deeply/nested/path"]
            assert m["b"] == archive_data["b"]

    def test_pickle(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            fs2 = pickle.loads(pickle.dumps(fs))
            assert fs2.cat("b") == b"hello"

    def test_all_dirnames(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)

            # fx are files, dx are a directories
            assert fs._all_dirnames([]) == set()
            assert fs._all_dirnames(["f1"]) == set()
            assert fs._all_dirnames(["f1", "f2"]) == set()
            assert fs._all_dirnames(["f1", "f2", "d1/f1"]) == {"d1"}
            assert fs._all_dirnames(["f1", "d1/f1", "d1/f2"]) == {"d1"}
            assert fs._all_dirnames(["f1", "d1/f1", "d2/f1"]) == {"d1", "d2"}
            assert fs._all_dirnames(["d1/d1/d1/f1"]) == {"d1", "d1/d1", "d1/d1/d1"}

    def test_ls(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)

            assert fs.ls("") == ["a", "b", "deeply/"]
            assert fs.ls("/") == fs.ls("")

            assert fs.ls("deeply") == ["deeply/nested/"]
            assert fs.ls("deeply/") == fs.ls("deeply")

            assert fs.ls("deeply/nested") == ["deeply/nested/path"]
            assert fs.ls("deeply/nested/") == fs.ls("deeply/nested")

    def test_find(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)

            assert fs.find("") == ["a", "b", "deeply/nested/path"]
            assert fs.find("", withdirs=True) == [
                "a",
                "b",
                "deeply/",
                "deeply/nested/",
                "deeply/nested/path",
            ]

            assert fs.find("deeply") == ["deeply/nested/path"]
            assert fs.find("deeply/") == fs.find("deeply")

    @pytest.mark.parametrize("topdown", [True, False])
    def test_walk(self, scenario: ArchiveTestScenario, topdown):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            expected = [
                # (dirname, list of subdirs, list of files)
                ("", ["deeply"], ["a", "b"]),
                ("deeply", ["nested"], []),
                ("deeply/nested", [], ["path"]),
            ]
            if not topdown:
                expected.reverse()
            for lhs, rhs in zip(fs.walk("", topdown=topdown), expected):
                assert lhs[0] == rhs[0]
                assert sorted(lhs[1]) == sorted(rhs[1])
                assert sorted(lhs[2]) == sorted(rhs[2])

    def test_info(self, scenario: ArchiveTestScenario):

        # https://github.com/Suor/funcy/blob/1.15/funcy/colls.py#L243-L245
        def project(mapping, keys):
            """Leaves only given keys in mapping."""
            return dict((k, mapping[k]) for k in keys if k in mapping)

        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)

            with pytest.raises(FileNotFoundError):
                fs.info("i-do-not-exist")

            # Iterate over all directories.
            for d in fs._all_dirnames(archive_data.keys()):
                lhs = project(fs.info(d), ["name", "size", "type"])
                expected = {"name": f"{d}/", "size": 0, "type": "directory"}
                assert lhs == expected

            # Iterate over all files.
            for f, v in archive_data.items():
                lhs = fs.info(f)
                assert lhs["name"] == f
                assert lhs["size"] == len(v)
                assert lhs["type"] == "file"

    @pytest.mark.parametrize("scale", [128, 512, 4096])
    def test_isdir_isfile(self, scenario: ArchiveTestScenario, scale: int):
        def make_nested_dir(i):
            x = f"{i}"
            table = x.maketrans("0123456789", "ABCDEFGHIJ")
            return "/".join(x.translate(table))

        scaled_data = {f"{make_nested_dir(i)}/{i}": b"" for i in range(1, scale + 1)}
        with scenario.provider(scaled_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)

            lhs_dirs, lhs_files = (
                fs._all_dirnames(scaled_data.keys()),
                scaled_data.keys(),
            )

            # Warm-up the Cache, this is done in both cases anyways...
            fs._get_dirs()

            entries = lhs_files | lhs_dirs

            assert lhs_dirs == {e for e in entries if fs.isdir(e)}
            assert lhs_files == {e for e in entries if fs.isfile(e)}

    def test_read_empty_file(self, scenario: ArchiveTestScenario):
        with scenario.provider(archive_data) as archive:
            fs = fsspec.filesystem(scenario.protocol, fo=archive)
            assert fs.open("a").read() == b""
