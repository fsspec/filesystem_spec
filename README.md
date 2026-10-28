# filesystem_spec

[![PyPI version](https://badge.fury.io/py/fsspec.svg)](https://pypi.python.org/pypi/fsspec/)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/fsspec/badges/version.svg)](https://anaconda.org/conda-forge/fsspec)
![Build](https://github.com/fsspec/filesystem_spec/workflows/CI/badge.svg)
[![Docs](https://readthedocs.org/projects/filesystem-spec/badge/?version=latest)](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest)

A specification for pythonic filesystems.

## Install

```bash
pip install fsspec
```

would install the base fsspec. Various optionally supported features might require specification of custom
extra require, e.g. `pip install fsspec[ssh]` will install dependencies for `ssh` backends support.
Use `pip install fsspec[full]` for installation of all known extra dependencies.

Up-to-date package also provided through conda-forge distribution:

```bash
conda install -c conda-forge fsspec
```


## Purpose

To produce a template or specification for a file-system interface, that specific implementations should follow,
so that applications making use of them can rely on a common behaviour and not have to worry about the specific
internal implementation decisions with any given backend. Many such implementations are included in this package,
or in sister projects such as `s3fs` and `gcsfs`.

In addition, if this is well-designed, then additional functionality, such as a key-value store or FUSE
mounting of the file-system implementation may be available for all implementations "for free".

## Documentation

Please refer to [RTD](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest)

## Develop

fsspec uses GitHub Actions for CI. The [CI workflow](.github/workflows/main.yaml)
uses [ci/environment-linux.yml](ci/environment-linux.yml) for Linux and
[ci/environment-win.yml](ci/environment-win.yml) for Windows. The Linux jobs
select the Python version from the workflow's test matrix. For local development,
create your own environment and install the development dependencies as shown below.

```bash
# For a new environment (mamba / conda).
mamba create -n fsspec -c conda-forge  python=3.10 -y
conda activate fsspec

# Standard dev install with docs and tests.
pip install -e ".[dev,doc,test]"

# Full tests except for downstream
pip install s3fs
pip uninstall s3fs
pip install -e .[dev,doc,test_full]
pip install s3fs --no-deps
pytest -v

# Downstream tests.
sh install_s3fs.sh
# Windows powershell.
install_s3fs.sh
```

### Testing

Tests can be run in the dev environment, if activated, via ``pytest fsspec``.

The full fsspec suite requires a system-level docker, docker-compose, and fuse
installation. If only making changes to one backend implementation, it is
not generally necessary to run all tests locally.

It is expected that contributors ensure that any change to fsspec does not
cause issues or regressions for either other fsspec-related packages such
as gcsfs and s3fs, nor for downstream users of fsspec. The "downstream" CI
run and corresponding environment file run a set of tests from the dask
test suite, and very minimal tests against pandas and zarr from the
test_downstream.py module in this repo.

### Type Hints

fsspec uses type stub files for type hints instead of inline hints. Use ``stubtest <module>``
to verify if type stubs are in sync with implementation.

### Code Formatting

fsspec uses [Ruff](https://docs.astral.sh/ruff/) for Python linting and formatting.
The development dependencies installed above include Ruff and pre-commit.
From the repository root, run:

```bash
ruff check fsspec
ruff format --check fsspec
```

To apply formatting changes, run `ruff format fsspec` without `--check`.

Optionally, you may wish to setup [pre-commit hooks](https://pre-commit.com) to
run the project's checks when you make a git commit.
Run ``pre-commit install --install-hooks`` from the root of the
filesystem_spec repository to set up pre-commit hooks. The hooks include Ruff,
whitespace checks, and spell checking. Run `pre-commit run --all-files` to check
the whole repository without making a commit.

## Support

Work on this repository is supported in part by:

"Anaconda, Inc. - Advancing AI through open source."

<a href="https://anaconda.com/"><img src="https://camo.githubusercontent.com/b8555ef2222598ed37ce38ac86955febbd25de7619931bb7dd3c58432181d3b6/68747470733a2f2f626565776172652e6f72672f636f6d6d756e6974792f6d656d626572732f616e61636f6e64612f616e61636f6e64612d6c617267652e706e67" alt="anaconda logo" width="40%"/></a>
