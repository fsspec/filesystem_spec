# s3fs tests are not currently accessible to import via a normal install.
# We need to instead do an editible install. Further, s3fs is very specific
# about the version of fsspec that it installs.

rm -r -f downstream
mkdir ./downstream

# Download source to get tests
git clone https://github.com/fsspec/s3fs
mv s3fs ./downstream/s3fs

# s3fs is pinned to a specific version of fsspec
pip install -e ./downstream/s3fs # installs all deps, including latest released fsspec
