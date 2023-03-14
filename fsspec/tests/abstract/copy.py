class AbstractCopyTests:
    def test_copy_two_files_new_directory(self, fs, fs_path):
        source = self.fs_join(fs_path, "src")
        file0 = self.fs_join(source, "file0")
        file1 = self.fs_join(source, "file1")
        fs.mkdir(source)
        fs.touch(file0)
        fs.touch(file1)

        target = self.fs_join(fs_path, "target")
        assert not fs.exists(target)
        fs.cp([file0, file1], target)

        assert fs.isdir(target)
        assert fs.isfile(self.fs_join(target, "file0"))
        assert fs.isfile(self.fs_join(target, "file1"))
