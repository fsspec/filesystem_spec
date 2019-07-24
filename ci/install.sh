#!/usr/bin/env bash
# install FUSE
# sudo apt-get install libfuse-dev

# Install conda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no
conda update conda

# Install dependencies
conda create -n test -c conda-forge python=3.7 pip pytest paramiko requests zstandard python-snappy lz4 distributed \
    dask fusepy pyarrow  pyftpdlib cloudpickle pyarrow pytest-cov -y -c defaults -c conda-forge
pip install hadoop-test-cluster==0.1.0 # fusepy
source activate test
pip install . --no-deps
