# s3fs tests are not currently accessible to import via a normal install.
# We need to instead do an editible install. Further, s3fs is very specific
# about the version of fsspec that it installs.

pip uninstall fsspec s3fs -y

rm -r -f downstream
mkdir ./downstream

# Download source to get tests
pip download --no-deps --no-binary :all: s3fs==2024.3.1
tar -xf s3fs-2024.3.1.tar.gz
mv s3fs-2024.3.1 ./downstream/s3fs

pip install --no-deps s3fs
# s3fs is pinned to a specific version of fsspec
pip install -e ./downstream/s3fs
pip install -e .
