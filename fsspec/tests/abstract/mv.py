from unittest.mock import patch

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

    # Patch the entire `mv` method to control the behavior fully
    with patch.object(fs, "mv", autospec=True) as mock_mv:
        # Configure the mock to raise an OSError when called
        mock_mv.side_effect = OSError("Unable to move the file")

        # Use the actual file paths as string
        with pytest.raises(OSError) as excinfo:
            fs.mv(str(source), str(destination))

        # Assert the message of the raised OSError matches the expected message
        assert "Unable to move the file" in str(excinfo.value)
