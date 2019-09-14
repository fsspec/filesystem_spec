# filesystem_spec

[![Build Status](https://travis-ci.org/intake/filesystem_spec.svg?branch=master)](https://travis-ci.org/martindurant/filesystem_spec)
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

### Code Formatting

Filesystem_spec uses [Black](https://black.readthedocs.io/en/stable) to ensure
a consistent code format throughout the project. ``black`` can be installed
with `pip install black` or `conda install black`.

Then, run `black fsspec` from the root of the filesystem_spec repository to
auto-format your code. Additionally, many editors have plugins that will apply
`black` as you edit files.

Optionally, you may wish to setup [pre-commit hooks](https://pre-commit.com) to
automatically run `black` when you make a git commit. This can be done by
installing pre-commit via `pip install pre-commit` or `conda install
pre-commit`.

Then, run `pre-commit install --install-hooks` from the root of the
filesystem_spec repository to setup pre-commit hooks. `black` will now be run
before you commit, reformatting any changed files. You can format without
committing via `pre-commit run` or skip these checks with `git commit
--no-verify`.
