# filesystem_spec

[![Build Status](https://travis-ci.org/martindurant/filesystem_spec.svg?branch=master)](https://travis-ci.org/martindurant/filesystem_spec)
[![Docs](https://readthedocs.org/projects/filesystem-spec/badge/?version=latest)](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest)

A specification for pythonic filesystems.

## Purpose

To produce a template or specification for a file-system interface, that specific implementations should follow,
so that applications making use of them can rely on a common behaviour and not have to worry about the specific
internal implementation decisions with any given backend.

In addition, if this is well-designed, then additional functionality, such as a key-value store or FUSE
mounting of the file-system implementation may be available for all implementations "for free".

## Documentation

Please refer to [RTD](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest)
