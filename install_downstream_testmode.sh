# Install in such an order to enable using downstream tests

pip uninstall fsspec dask distributed s3fs -y
pip install "moto >4,<5" flask
pip download --no-deps --no-binary :all: s3fs==2024.3.1
tar -xf s3fs-2024.3.1.tar.gz

pip download --no-deps --no-binary :all: dask==2024.3.1
tar -xf dask-2024.3.1.tar.gz

pip install -e ./s3fs-2024.3.1
pip install -e ./dask-2024.3.1[dataframe,test]
pip uninstall fsspec -y

pip install -e .[test,test_downstream]
