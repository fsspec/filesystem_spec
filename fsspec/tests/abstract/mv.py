import pytest

import fsspec


def test_move_raises_error_with_tmpdir(tmpdir):
    # Create a file in the temporary directory
    source = tmpdir.join("source_file.txt")
    source.write("content")

    # Define a destination that simulates a protected or invalid path
    destination = tmpdir.join("non_existent_directory/destination_file.txt")

    # Instantiate the filesystem (assuming the local file system interface)
    fs = fsspec.filesystem("file")

    # Use the actual file paths as string
    with pytest.raises(FileNotFoundError):
        fs.mv(str(source), str(destination))
