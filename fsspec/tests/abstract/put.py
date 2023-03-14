class AbstractPutTests:
    def test_put_directory_recursive(self, fs, fs_path, local_fs, local_path):
        # https://github.com/fsspec/filesystem_spec/issues/1062
        # Recursive cp/get/put of source directory into non-existent target directory.
        src = self.local_join(local_path, "src")
        src_file = self.local_join(src, "file")
        local_fs.mkdir(src)
        local_fs.touch(src_file)

        target = self.fs_join(fs_path, "target")

        # put without slash
        assert not fs.exists(target)
        for loop in range(2):
            fs.put(src, target, recursive=True)
            assert fs.isdir(target)

            if loop == 0:
                assert fs.isfile(self.fs_join(target, "file"))
                assert not fs.exists(self.fs_join(target, "src"))
            else:
                assert fs.isfile(self.fs_join(target, "file"))
                assert fs.isdir(self.fs_join(target, "src"))
                assert fs.isfile(self.fs_join(target, "src", "file"))

        fs.rm(target, recursive=True)

        # put with slash
        assert not fs.exists(target)
        for loop in range(2):
            fs.put(src + "/", target, recursive=True)  # May fail on windows
            assert fs.isdir(target)
            assert fs.isfile(self.local_join(target, "file"))
            assert not fs.exists(self.local_join(target, "src"))
