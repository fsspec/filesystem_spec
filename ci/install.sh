#!/usr/bin/env bash

# install FUSE
sudo apt-get install libfuse-dev

# install conda
source $(dirname $BASH_SOURCE)/install_conda.sh

# Install dependencies
conda create -n test -c conda-forge python=3.7 pip pytest paramiko requests zstandard python-snappy lz4 distributed \
    dask pyarrow  pyftpdlib cloudpickle pyarrow pytest-cov -y -c defaults -c conda-forge
pip install hadoop-test-cluster==0.1.0 fusepy
source activate test
pip install . --no-deps
