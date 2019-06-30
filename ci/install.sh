#!/usr/bin/env bash
# Install conda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no
conda update conda

# Install dependencies
conda create -n test -c conda-forge python=3.7 pip
source activate test
pip install intake/filesystem_spec --no-deps
