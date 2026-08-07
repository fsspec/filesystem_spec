import random
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from fsspec.dircache import DirCache


class TestDirCache(unittest.TestCase):
    def test_basic_ls_set_and_get(self):
        cache = DirCache()
        files = [
            {"name": "dir/file1", "type": "file", "size": 100},
            {"name": "dir/file2", "type": "file", "size": 200},
        ]
        cache["dir"] = files

        # Lookup directory
        self.assertIn("dir", cache)
        retrieved = cache["dir"]
        self.assertEqual(len(retrieved), 2)
        self.assertEqual(retrieved[0]["name"], "dir/file1")
        self.assertEqual(retrieved[1]["name"], "dir/file2")

    def test_single_info_lookup(self):
        cache = DirCache()
        files = [
            {"name": "dir/file1", "type": "file", "size": 100},
            {"name": "dir/file2", "type": "file", "size": 200},
        ]
        cache["dir"] = files

        # O(1) single item lookup
        info1 = cache.get_info("dir/file1")
        self.assertIsNotNone(info1)
        self.assertEqual(info1["size"], 100)

        # Non-existent item
        self.assertIsNone(cache.get_info("dir/file3"))

    def test_save_info_standalone(self):
        cache = DirCache()
        # Save info for a standalone file without dir listing
        cache.save_info(
            "dir/file_standalone",
            {"name": "dir/file_standalone", "type": "file", "size": 500},
        )

        # Single item lookup hits
        info = cache.get_info("dir/file_standalone")
        self.assertIsNotNone(info)
        self.assertEqual(info["size"], 500)

        # Full dir listing for parent should raise KeyError (not fully cached)
        self.assertNotIn("dir", cache._fully_cached_dirs)
        with self.assertRaises(KeyError):
            _ = cache["dir"]

    def test_dict_assignment_shortcut(self):
        cache = DirCache()
        info = {"name": "path/file.txt", "type": "file", "size": 1234}
        cache["path/file.txt"] = info

        self.assertEqual(cache.get_info("path/file.txt"), info)
        self.assertIn("path/file.txt", cache)

    def test_child_deletion_invalidates_parent_listing(self):
        cache = DirCache()
        files = [
            {"name": "dir/file1", "type": "file", "size": 100},
            {"name": "dir/file2", "type": "file", "size": 200},
        ]
        cache["dir"] = files

        # Delete single file
        del cache["dir/file1"]
        self.assertIsNone(cache.get_info("dir/file1"))

        # Parent directory full listing should now be invalidated
        self.assertNotIn("dir", cache._fully_cached_dirs)
        with self.assertRaises(KeyError):
            _ = cache["dir"]

    def test_eviction_invalidates_parent_listing(self):
        cache = DirCache(max_paths=2)
        # Store listing of 2 items under 'dir'
        cache["dir"] = [
            {"name": "dir/file1", "type": "file", "size": 100},
            {"name": "dir/file2", "type": "file", "size": 200},
        ]
        self.assertIn("dir", cache._fully_cached_dirs)

        # Adding a 3rd item forces LRU eviction of file1
        cache.save_info(
            "dir2/file3", {"name": "dir2/file3", "type": "file", "size": 300}
        )

        # file1 was evicted, so 'dir' completeness MUST be invalidated
        self.assertNotIn("dir", cache._fully_cached_dirs)

    def test_ttl_expiry(self):
        cache = DirCache(listings_expiry_time=0.1)
        cache.save_info("file1", {"name": "file1", "size": 10})
        self.assertIsNotNone(cache.get_info("file1"))

        time.sleep(0.15)
        self.assertIsNone(cache.get_info("file1"))

    def test_clear_and_len(self):
        cache = DirCache()
        cache["dir"] = [
            {"name": "dir/file1", "type": "file", "size": 100},
            {"name": "dir/file2", "type": "file", "size": 200},
        ]
        self.assertEqual(len(cache), 2)

        cache.clear()
        self.assertEqual(len(cache), 0)
        self.assertNotIn("dir", cache)
        self.assertIsNone(cache.get_info("dir/file1"))

    def test_high_stress_concurrency(self):
        """
        Stress test thread safety under heavy contention:
        50 threads performing 2000 random operations (reads, writes, deletes, iterations)
        with a small max_paths limit to continuously force LRU evictions during active access.
        """
        max_capacity = 25
        cache = DirCache(max_paths=max_capacity, listings_expiry_time=1.0)
        errors = []

        def worker(thread_id):
            try:
                for i in range(40):
                    folder_id = random.randint(0, 5)
                    item_id = random.randint(0, 50)
                    dir_name = f"folder_{folder_id}"
                    file_name = f"{dir_name}/file_{item_id}.dat"

                    op = random.choice(
                        [
                            "save_info",
                            "get_info",
                            "set_dir",
                            "get_dir",
                            "del_item",
                            "iter_cache",
                            "contains",
                        ]
                    )

                    if op == "save_info":
                        cache.save_info(file_name, {"name": file_name, "size": item_id})
                    elif op == "get_info":
                        _ = cache.get_info(file_name)
                    elif op == "set_dir":
                        cache[dir_name] = [
                            {"name": f"{dir_name}/item_{k}", "type": "file", "size": k}
                            for k in range(3)
                        ]
                    elif op == "get_dir":
                        try:
                            _ = cache[dir_name]
                        except KeyError:
                            pass
                    elif op == "del_item":
                        try:
                            del cache[file_name]
                        except KeyError:
                            pass
                    elif op == "iter_cache":
                        _ = list(cache)
                    elif op == "contains":
                        _ = file_name in cache
                        _ = dir_name in cache

                    # Enforce capacity check
                    self.assertLessEqual(len(cache), max_capacity)

            except Exception as exc:
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(worker, tid) for tid in range(50)]
            for f in futures:
                f.result()

        self.assertEqual(
            len(errors), 0, f"Concurrent execution produced errors: {errors}"
        )
        self.assertLessEqual(len(cache), max_capacity)


if __name__ == "__main__":
    unittest.main()
