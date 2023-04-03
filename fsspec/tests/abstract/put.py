class AbstractPutTests:
    def test_put_directory_recursive(
        self, fs, fs_join, fs_path, local_fs, local_join, local_path
    ):
        # https://github.com/fsspec/filesystem_spec/issues/1062
        # Recursive cp/get/put of source directory into non-existent target directory.
        src = local_join(local_path, "src")
        src_file = local_join(src, "file")
        local_fs.mkdir(src)
        local_fs.touch(src_file)

        target = fs_join(fs_path, "target")

        # put without slash
        assert not fs.exists(target)
        for loop in range(2):
            fs.put(src, target, recursive=True)
            assert fs.isdir(target)

            if loop == 0:
                assert fs.isfile(fs_join(target, "file"))
                assert not fs.exists(fs_join(target, "src"))
            else:
                assert fs.isfile(fs_join(target, "file"))
                assert fs.isdir(fs_join(target, "src"))
                assert fs.isfile(fs_join(target, "src", "file"))

        fs.rm(target, recursive=True)

        # put with slash
        assert not fs.exists(target)
        for loop in range(2):
            fs.put(src + "/", target, recursive=True)
            assert fs.isdir(target)
            assert fs.isfile(fs_join(target, "file"))
            assert not fs.exists(fs_join(target, "src"))
