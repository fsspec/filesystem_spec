# Install in such an order to enable using downstream tests

pip uninstall fsspec dask distributed s3fs -y
pip install "moto >4,<5" flask

rm -r -f downstream
mkdir ./downstream

pip download --no-deps --no-binary :all: s3fs==2024.3.1
tar -xf s3fs-2024.3.1.tar.gz
mv s3fs-2024.3.1 ./downstream/s3fs

pip download --no-deps --no-binary :all: dask==2024.3.1
tar -xf dask-2024.3.1.tar.gz
mv ./dask-2024.3.1 ./downstream/dask

pip install -e ./downstream/s3fs
pip install -e ./downstream/dask[dataframe,test]
pip uninstall fsspec -y

pip install -e .[test,test_downstream]
