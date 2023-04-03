class AbstractCopyTests:
    def test_copy_file_to_existing_directory(self, fs, fs_join, fs_path):
        # Copy scenario 1a
        source = fs_join(fs_path, "source")
        file2 = fs_join(source, "file2")
        subdir = fs_join(source, "subdir")
        subfile1 = fs_join(subdir, "subfile1")
        fs.makedirs(subdir)
        fs.touch(subfile1)
        fs.touch(file2)

        target = fs_join(fs_path, "target")
        fs.mkdir(target)
        if not self.supports_empty_directories():
            fs.touch(fs_join(target, "dummy"))
        assert fs.isdir(target)

        target_file2 = fs_join(target, "file2")
        target_subfile1 = fs_join(target, "subfile1")

        # Copy from source directory
        fs.cp(file2, target)
        assert fs.isfile(target_file2)

        # Copy from sub directory
        fs.cp(subfile1, target)
        assert fs.isfile(target_subfile1)

        # Remove copied files
        fs.rm([target_file2, target_subfile1])
        assert not fs.exists(target_file2)
        assert not fs.exists(target_subfile1)

        # Repeat with trailing slash on target
        fs.cp(file2, target + "/")
        assert fs.isdir(target)
        assert fs.isfile(target_file2)

        fs.cp(subfile1, target + "/")
        assert fs.isfile(target_subfile1)

    def test_copy_file_to_new_directory(self, fs, fs_join, fs_path):
        # Copy scenario 1b
        source = fs_join(fs_path, "source")
        subdir = fs_join(source, "subdir")
        subfile1 = fs_join(subdir, "subfile1")
        fs.mkdir(subdir)
        fs.touch(subfile1)

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        fs.cp(subfile1, fs_join(target, "newdir/"))  # Note trailing slash
        assert fs.isdir(target)
        assert fs.isdir(fs_join(target, "newdir"))
        assert fs.isfile(fs_join(target, "newdir", "subfile1"))

    def test_copy_file_to_file_in_existing_directory(self, fs, fs_join, fs_path):
        # Copy scenario 1c
        source = fs_join(fs_path, "source")
        subdir = fs_join(source, "subdir")
        subfile1 = fs_join(subdir, "subfile1")
        fs.mkdir(subdir)
        fs.touch(subfile1)

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        fs.cp(subfile1, fs_join(target, "newfile"))
        assert fs.isfile(fs_join(target, "newfile"))

    def test_copy_file_to_file_in_new_directory(self, fs, fs_join, fs_path):
        # Copy scenario 1d
        source = fs_join(fs_path, "source")
        subdir = fs_join(source, "subdir")
        subfile1 = fs_join(subdir, "subfile1")
        fs.mkdir(subdir)
        fs.touch(subfile1)

        target = fs_join(fs_path, "target")
        fs.mkdir(target)

        fs.cp(subfile1, fs_join(target, "newdir", "newfile"))
        assert fs.isdir(fs_join(target, "newdir"))
        assert fs.isfile(fs_join(target, "newdir", "newfile"))

    def test_copy_two_files_new_directory(self, fs, fs_join, fs_path):
        source = fs_join(fs_path, "src")
        file0 = fs_join(source, "file0")
        file1 = fs_join(source, "file1")
        fs.mkdir(source)
        fs.touch(file0)
        fs.touch(file1)

        target = fs_join(fs_path, "target")
        assert not fs.exists(target)
        fs.cp([file0, file1], target)

        assert fs.isdir(target)
        assert fs.isfile(fs_join(target, "file0"))
        assert fs.isfile(fs_join(target, "file1"))
