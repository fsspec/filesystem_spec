from itertools import product

import pytest

from fsspec.tests.conftest import GLOB_TESTS


class AbstractGetTests:
    def test_get_file_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1a
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)
        assert local_fs.isdir(target)

        target_file2 = local_join(target, "file2")
        target_subfile1 = local_join(target, "subfile1")

        # Copy from source directory
        fs.get(fs_join(source, "file2"), target)
        assert local_fs.isfile(target_file2)

        # Copy from sub directory
        fs.get(fs_join(source, "subdir", "subfile1"), target)
        assert local_fs.isfile(target_subfile1)

        # Remove copied files
        local_fs.rm([target_file2, target_subfile1])
        assert not local_fs.exists(target_file2)
        assert not local_fs.exists(target_subfile1)

        # Repeat with trailing slash on target
        fs.get(fs_join(source, "file2"), target + "/")
        assert local_fs.isdir(target)
        assert local_fs.isfile(target_file2)

        fs.get(fs_join(source, "subdir", "subfile1"), target + "/")
        assert local_fs.isfile(target_subfile1)

    def test_get_file_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1b
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        fs.get(
            fs_join(source, "subdir", "subfile1"), local_join(target, "newdir/")
        )  # Note trailing slash

        assert local_fs.isdir(target)
        assert local_fs.isdir(local_join(target, "newdir"))
        assert local_fs.isfile(local_join(target, "newdir", "subfile1"))

    def test_get_file_to_file_in_existing_directory(
        self,
        fs,
        fs_join,
        fs_path,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1c
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        fs.get(fs_join(source, "subdir", "subfile1"), local_join(target, "newfile"))
        assert local_fs.isfile(local_join(target, "newfile"))

    def test_get_file_to_file_in_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1d
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        fs.get(
            fs_join(source, "subdir", "subfile1"),
            local_join(target, "newdir", "newfile"),
        )
        assert local_fs.isdir(local_join(target, "newdir"))
        assert local_fs.isfile(local_join(target, "newdir", "newfile"))

    def test_get_directory_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1e
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        for source_slash, target_slash in zip([False, True], [False, True]):
            s = fs_join(source, "subdir")
            if source_slash:
                s += "/"
            t = target + "/" if target_slash else target

            # Without recursive does nothing
            # ERROR: erroneously creates new directory
            # fs.get(s, t)
            # assert fs.ls(target) == []

            # With recursive
            fs.get(s, t, recursive=True)
            if source_slash:
                assert local_fs.isfile(local_join(target, "subfile1"))
                assert local_fs.isfile(local_join(target, "subfile2"))
                assert local_fs.isdir(local_join(target, "nesteddir"))
                assert local_fs.isfile(local_join(target, "nesteddir", "nestedfile"))

                local_fs.rm(
                    [
                        local_join(target, "subfile1"),
                        local_join(target, "subfile2"),
                        local_join(target, "nesteddir"),
                    ],
                    recursive=True,
                )
            else:
                assert local_fs.isdir(local_join(target, "subdir"))
                assert local_fs.isfile(local_join(target, "subdir", "subfile1"))
                assert local_fs.isfile(local_join(target, "subdir", "subfile2"))
                assert local_fs.isdir(local_join(target, "subdir", "nesteddir"))
                assert local_fs.isfile(
                    local_join(target, "subdir", "nesteddir", "nestedfile")
                )

                local_fs.rm(local_join(target, "subdir"), recursive=True)
            assert local_fs.ls(target) == []

            # Limit by maxdepth
            # ERROR: maxdepth ignored here

    def test_get_directory_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1f
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        for source_slash, target_slash in zip([False, True], [False, True]):
            s = fs_join(source, "subdir")
            if source_slash:
                s += "/"
            t = local_join(target, "newdir")
            if target_slash:
                t += "/"

            # Without recursive does nothing
            # ERROR: erroneously creates new directory
            # fs.get(s, t)
            # assert fs.ls(target) == []

            # With recursive
            fs.get(s, t, recursive=True)
            assert local_fs.isdir(local_join(target, "newdir"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile1"))
            assert local_fs.isfile(local_join(target, "newdir", "subfile2"))
            assert local_fs.isdir(local_join(target, "newdir", "nesteddir"))
            assert local_fs.isfile(
                local_join(target, "newdir", "nesteddir", "nestedfile")
            )

            local_fs.rm(local_join(target, "newdir"), recursive=True)
            assert local_fs.ls(target) == []

            # Limit by maxdepth
            # ERROR: maxdepth ignored here

    @pytest.mark.parametrize(
        ("path", "recursive", "maxdepth", "expected"),
        GLOB_TESTS,
    )
    def test_get_glob(
        self,
        path,
        recursive,
        maxdepth,
        expected,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0_glob,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 1g
        source = fs_bulk_operations_scenario_0_glob

        target = local_target

        for new_dir, target_slash in product([True, False], [True, False]):
            local_fs.mkdir(target)

            t = local_join(target, "newdir") if new_dir else target
            t = t + "/" if target_slash else t

            fs.get(fs_join(source, path), t, recursive=recursive, maxdepth=maxdepth)

            output = local_fs.find(target)
            if new_dir:
                prefixed_expected = [local_join(target, "newdir", p) for p in expected]
            else:
                prefixed_expected = [local_join(target, p) for p in expected]
            assert sorted(output) == sorted(prefixed_expected)

            try:
                local_fs.rm(target, recursive=True)
            except FileNotFoundError:
                pass

    def test_get_list_of_files_to_existing_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 2a
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        source_files = [
            fs_join(source, "file1"),
            fs_join(source, "file2"),
            fs_join(source, "subdir", "subfile1"),
        ]

        for target_slash in [False, True]:
            t = target + "/" if target_slash else target

            fs.get(source_files, t)
            assert local_fs.isfile(local_join(target, "file1"))
            assert local_fs.isfile(local_join(target, "file2"))
            assert local_fs.isfile(local_join(target, "subfile1"))

            local_fs.rm(local_fs.find(target))
            assert local_fs.ls(target) == []

    def test_get_list_of_files_to_new_directory(
        self,
        fs,
        fs_join,
        fs_bulk_operations_scenario_0,
        local_fs,
        local_join,
        local_target,
    ):
        # Copy scenario 2b
        source = fs_bulk_operations_scenario_0

        target = local_target
        local_fs.mkdir(target)

        source_files = [
            fs_join(source, "file1"),
            fs_join(source, "file2"),
            fs_join(source, "subdir", "subfile1"),
        ]

        fs.get(source_files, local_join(target, "newdir") + "/")  # Note trailing slash
        assert local_fs.isdir(local_join(target, "newdir"))
        assert local_fs.isfile(local_join(target, "newdir", "file1"))
        assert local_fs.isfile(local_join(target, "newdir", "file2"))
        assert local_fs.isfile(local_join(target, "newdir", "subfile1"))

    def test_get_directory_recursive(
        self, fs, fs_join, fs_path, local_fs, local_join, local_target
    ):
        # https://github.com/fsspec/filesystem_spec/issues/1062
        # Recursive cp/get/put of source directory into non-existent target directory.
        src = fs_join(fs_path, "src")
        src_file = fs_join(src, "file")
        fs.mkdir(src)
        fs.touch(src_file)

        target = local_target

        # get without slash
        assert not local_fs.exists(target)
        for loop in range(2):
            fs.get(src, target, recursive=True)
            assert local_fs.isdir(target)

        if loop == 0:
            assert local_fs.isfile(local_join(target, "file"))
            assert not local_fs.exists(local_join(target, "src"))
        else:
            assert local_fs.isfile(local_join(target, "file"))
            assert local_fs.isdir(local_join(target, "src"))
            assert local_fs.isfile(local_join(target, "src", "file"))

        local_fs.rm(target, recursive=True)

        # get with slash
        assert not local_fs.exists(target)
        for loop in range(2):
            fs.get(src + "/", target, recursive=True)
            assert local_fs.isdir(target)
            assert local_fs.isfile(local_join(target, "file"))
            assert not local_fs.exists(local_join(target, "src"))
