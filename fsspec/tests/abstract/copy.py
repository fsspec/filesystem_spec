class AbstractCopyTests:
    def test_copy_file_to_existing_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 1a
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)
        if not self.supports_empty_directories():
            fs.touch(fs_join(target, "dummy"))
        assert fs.isdir(target)

        target_file2 = fs_join(target, "file2")
        target_subfile1 = fs_join(target, "subfile1")

        # Copy from source directory
        fs.cp(fs_join(source, "file2"), target)
        assert fs.isfile(target_file2)

        # Copy from sub directory
        fs.cp(fs_join(source, "subdir", "subfile1"), target)
        assert fs.isfile(target_subfile1)

        # Remove copied files
        fs.rm([target_file2, target_subfile1])
        assert not fs.exists(target_file2)
        assert not fs.exists(target_subfile1)

        # Repeat with trailing slash on target
        fs.cp(fs_join(source, "file2"), target + "/")
        assert fs.isdir(target)
        assert fs.isfile(target_file2)

        fs.cp(fs_join(source, "subdir", "subfile1"), target + "/")
        assert fs.isfile(target_subfile1)

    def test_copy_file_to_new_directory(self, fs, fs_join, fs_path, fs_scenario_cp):
        # Copy scenario 1b
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        fs.cp(
            fs_join(source, "subdir", "subfile1"), fs_join(target, "newdir/")
        )  # Note trailing slash
        assert fs.isdir(target)
        assert fs.isdir(fs_join(target, "newdir"))
        assert fs.isfile(fs_join(target, "newdir", "subfile1"))

    def test_copy_file_to_file_in_existing_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 1c
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        fs.cp(fs_join(source, "subdir", "subfile1"), fs_join(target, "newfile"))
        assert fs.isfile(fs_join(target, "newfile"))

    def test_copy_file_to_file_in_new_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 1d
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        fs.cp(
            fs_join(source, "subdir", "subfile1"), fs_join(target, "newdir", "newfile")
        )
        assert fs.isdir(fs_join(target, "newdir"))
        assert fs.isfile(fs_join(target, "newdir", "newfile"))

    def test_copy_directory_to_existing_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 1e
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        for source_slash, target_slash in zip([False, True], [False, True]):
            s = fs_join(source, "subdir")
            if source_slash:
                s += "/"
            t = target + "/" if target_slash else target

            # Without recursive does nothing
            fs.cp(s, t)
            assert fs.ls(target) == []

            # With recursive
            fs.cp(s, t, recursive=True)
            if source_slash:
                assert fs.isfile(fs_join(target, "subfile1"))
                assert fs.isfile(fs_join(target, "subfile2"))
                assert fs.isdir(fs_join(target, "nesteddir"))
                assert fs.isfile(fs_join(target, "nesteddir", "nestedfile"))

                fs.rm(
                    [
                        fs_join(target, "subfile1"),
                        fs_join(target, "subfile2"),
                        fs_join(target, "nesteddir"),
                    ],
                    recursive=True,
                )
            else:
                assert fs.isdir(fs_join(target, "subdir"))
                assert fs.isfile(fs_join(target, "subdir", "subfile1"))
                assert fs.isfile(fs_join(target, "subdir", "subfile2"))
                assert fs.isdir(fs_join(target, "subdir", "nesteddir"))
                assert fs.isfile(fs_join(target, "subdir", "nesteddir", "nestedfile"))

                fs.rm(fs_join(target, "subdir"), recursive=True)
            assert fs.ls(target) == []

            # Limit by maxdepth
            # ERROR: maxdepth ignored here

    def test_copy_directory_to_new_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 1f
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        for source_slash, target_slash in zip([False, True], [False, True]):
            s = fs_join(source, "subdir")
            if source_slash:
                s += "/"
            t = fs_join(target, "newdir")
            if target_slash:
                t += "/"

            # Without recursive does nothing
            fs.cp(s, t)
            assert fs.ls(target) == []

            # With recursive
            fs.cp(s, t, recursive=True)
            assert fs.isdir(fs_join(target, "newdir"))
            assert fs.isfile(fs_join(target, "newdir", "subfile1"))
            assert fs.isfile(fs_join(target, "newdir", "subfile2"))
            assert fs.isdir(fs_join(target, "newdir", "nesteddir"))
            assert fs.isfile(fs_join(target, "newdir", "nesteddir", "nestedfile"))

            fs.rm(fs_join(target, "newdir"), recursive=True)
            assert fs.ls(target) == []

            # Limit by maxdepth
            # ERROR: maxdepth ignored here

    def test_copy_glob_to_existing_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 1g
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        # for target_slash in [False, True]:
        for target_slash in [False]:
            t = target + "/" if target_slash else target

            # Without recursive
            fs.cp(fs_join(source, "subdir", "*"), t)
            assert fs.isfile(fs_join(target, "subfile1"))
            assert fs.isfile(fs_join(target, "subfile2"))
            # assert not fs.isdir(fs_join(target, "nesteddir"))  # ERROR
            assert not fs.isdir(fs_join(target, "subdir"))

            # With recursive

            # Limit by maxdepth

    def test_copy_glob_to_new_directory(self, fs, fs_join, fs_path, fs_scenario_cp):
        # Copy scenario 1h
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        for target_slash in [False, True]:
            t = fs_join(target, "newdir")
            if target_slash:
                t += "/"

            # Without recursive
            fs.cp(fs_join(source, "subdir", "*"), t)
            assert fs.isdir(fs_join(target, "newdir"))
            assert fs.isfile(fs_join(target, "newdir", "subfile1"))
            assert fs.isfile(fs_join(target, "newdir", "subfile2"))
            # ERROR - do not copy empty directory
            # assert not fs.exists(fs_join(target, "newdir", "nesteddir"))

            fs.rm(fs_join(target, "newdir"), recursive=True)
            assert fs.ls(target) == []

            # With recursive
            fs.cp(fs_join(source, "subdir", "*"), t, recursive=True)
            assert fs.isdir(fs_join(target, "newdir"))
            assert fs.isfile(fs_join(target, "newdir", "subfile1"))
            assert fs.isfile(fs_join(target, "newdir", "subfile2"))
            assert fs.isdir(fs_join(target, "newdir", "nesteddir"))
            assert fs.isfile(fs_join(target, "newdir", "nesteddir", "nestedfile"))

            fs.rm(fs_join(target, "newdir"), recursive=True)
            assert fs.ls(target) == []

            # Limit by maxdepth
            # ERROR: this is not correct

    def test_copy_list_of_files_to_existing_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 2a
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        source_files = [
            fs_join(source, "file1"),
            fs_join(source, "file2"),
            fs_join(source, "subdir", "subfile1"),
        ]

        for target_slash in [False, True]:
            t = target + "/" if target_slash else target

            fs.cp(source_files, t)
            assert fs.isfile(fs_join(target, "file1"))
            assert fs.isfile(fs_join(target, "file2"))
            assert fs.isfile(fs_join(target, "subfile1"))

            fs.rm(fs.find(target))
            assert fs.ls(target) == []

    def test_copy_list_of_files_to_new_directory(
        self, fs, fs_join, fs_path, fs_scenario_cp
    ):
        # Copy scenario 2b
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        source_files = [
            fs_join(source, "file1"),
            fs_join(source, "file2"),
            fs_join(source, "subdir", "subfile1"),
        ]

        fs.cp(source_files, fs_join(target, "newdir") + "/")  # Note trailing slash
        assert fs.isdir(fs_join(target, "newdir"))
        assert fs.isfile(fs_join(target, "newdir", "file1"))
        assert fs.isfile(fs_join(target, "newdir", "file2"))
        assert fs.isfile(fs_join(target, "newdir", "subfile1"))

    def test_copy_two_files_new_directory(self, fs, fs_join, fs_path, fs_scenario_cp):
        # This is a duplicate of test_copy_list_of_files_to_new_directory and
        # can eventually be removed.
        source = fs_scenario_cp

        target = fs_join(fs_path, "target")
        assert not fs.exists(target)
        fs.cp([fs_join(source, "file1"), fs_join(source, "file2")], target)

        assert fs.isdir(target)
        assert fs.isfile(fs_join(target, "file1"))
        assert fs.isfile(fs_join(target, "file2"))
