class AbstractGetTests:
    def test_get_directory_recursive(self, fs, fs_path, local_fs, local_path):
        # https://github.com/fsspec/filesystem_spec/issues/1062
        # Recursive cp/get/put of source directory into non-existent target directory.
        src = self.fs_join(fs_path, "src")
        src_file = self.fs_join(src, "file")
        fs.mkdir(src)
        fs.touch(src_file)

        target = self.local_join(local_path, "target")

        # get without slash
        assert not local_fs.exists(target)
        for loop in range(2):
            fs.get(src, target, recursive=True)
            assert local_fs.isdir(target)

        if loop == 0:
            assert local_fs.isfile(self.local_join(target, "file"))
            assert not local_fs.exists(self.local_join(target, "src"))
        else:
            assert local_fs.isfile(self.local_join(target, "file"))
            assert local_fs.isdir(self.local_join(target, "src"))
            assert local_fs.isfile(self.local_join(target, "src", "file"))

        local_fs.rm(target, recursive=True)

        # get with slash
        assert not local_fs.exists(target)
        for loop in range(2):
            fs.get(src + "/", target, recursive=True)
            assert local_fs.isdir(target)
            assert local_fs.isfile(self.local_join(target, "file"))
            assert not local_fs.exists(self.local_join(target, "src"))
