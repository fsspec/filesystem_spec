# filesystem_spec

[![PyPI version](https://badge.fury.io/py/fsspec.svg)](https://pypi.python.org/pypi/fsspec/)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/fsspec/badges/version.svg)](https://anaconda.org/conda-forge/fsspec)
![Build](https://github.com/intake/filesystem_spec/workflows/CI/badge.svg)
[![Docs](https://readthedocs.org/projects/filesystem-spec/badge/?version=latest)](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest)

A specification for pythonic filesystems.

## Install

```bash
pip install fsspec
```
or
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

fsspec uses [tox](https://tox.readthedocs.io/en/latest/) and
[tox-conda](https://github.com/tox-dev/tox-conda) to manage dev and test
environments. First, install conda with tox and tox-conda in a base environment
(eg. ``conda install -c conda-forge tox tox-conda``). Calls to ``tox`` can then be
used to configure a development environment and run tests.

First, setup a development conda environment via ``tox -e {env}`` where ``env`` is one of ``{py36,py37,py38,py39}``.
This will install fspec dependencies, test & dev tools, and install fsspec in develop
mode. You may activate the dev environment under ``.tox/{env}`` via ``conda activate .tox/{env}``.

### Testing

Tests can be run in the dev environment, if activated, via ``pytest fsspec``.

Alternatively, the full fsspec test suite can also be run via ``tox``, which will
also build the appropriate environment (see above), with the environment specified
by the TOXENV environment variable.

The full fsspec suite requires a system-level docker, docker-compose, and fuse
installation.

### Code Formatting

fsspec uses [Black](https://black.readthedocs.io/en/stable) to ensure
a consistent code format throughout the project.
Run ``black fsspec`` from the root of the filesystem_spec repository to
auto-format your code. Additionally, many editors have plugins that will apply
``black`` as you edit files. ``black`` is included in the ``tox`` environments.


Optionally, you may wish to setup [pre-commit hooks](https://pre-commit.com) to
automatically run ``black`` when you make a git commit.
Run ``pre-commit install --install-hooks`` from the root of the
filesystem_spec repository to setup pre-commit hooks. ``black`` will now be run
before you commit, reformatting any changed files. You can format without
committing via ``pre-commit run`` or skip these checks with ``git commit
--no-verify``.
