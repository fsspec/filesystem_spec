# filesystem_spec

[![Build Status](https://travis-ci.org/martindurant/filesystem_spec.svg?branch=master)](https://travis-ci.org/martindurant/filesystem_spec)

A specification for pythonic filesystems.

## Purpose

To produce a template or specification for a file-system interface, that specific implementations should follow,
so that applications making use of them can rely on a common behaviour and not have to worry about the specific
internal implementation decisions with any given backend.

In addition, if this is well-designed, then additional functionality, such as a key-value store or FUSE
mounting of the file-system implementation may be available for all implementations "for free".

## Background

Python provides a standard interface for open files, so that alternate implementations of file-like object can
work seamlessly with many function which rely only on the methods of that standard interface. A number of libraries
have implemented a similar concept for file-systems, where file operations can be performed on a logical file-system
which may be local, structured data store or some remote service. 

This repository is intended to be a place to define a standard interface that such file-systems should adhere to,
such that code using them should not have to know the details of the implementation in order to operate on any of
a number of backends.

Everything here is up for discussion, and although a little code has already been included to kick things off, it
is only meant as a suggestion of one possible way of doing things. With hope, the community can come together to
define an interface that is the best for the highest number of users, and having the specification, makes developing
other file-system implementations simpler.

There is no specific model (yet) of how the contents of this repo would be used, whether as a spec to refer to,
or perhaps something to subclass or use as a mixin, that can also form part of the conversation.

#### History

I (Martin Durant) have been involved in building a number of remote-data file-system implementations, principally
in the context of the [Dask](http://dask.pydata.org/en/latest/) project. In particular, several are listed
in [the docs](http://dask.pydata.org/en/latest/remote-data-services.html) with links to the specific repositories.
With common authership, there is much that is similar between the implementations, for example posix-like naming
of the operations, and this has allowed Dask to be able to interact with the various backends and parse generic
URLs in order to select amongst them. However, *some* extra code was required in each case to adapt the peculiarities
of each implementation with the generic usage that Dask demanded. People may find the 
[code](https://github.com/dask/dask/blob/master/dask/bytes/core.py#L266) which parses URLs and creates file-system
instances interesting.

At the same time, the Apache [Arrow](https://arrow.apache.org/) project was also concerned with a similar problem,
particularly a common interface to local and HDFS files, for example the 
[hdfs](https://arrow.apache.org/docs/python/filesystems.html) interface (which actually communicated with HDFS
with a choice of driver). These are mostly used internally within Arrow, but Dask was modified in order to be able 
to use the alternate HDFS interface (which solves some security issues with `hdfs3`). In the process, a 
[conversation](https://github.com/dask/dask/issues/2880)
was started, and I invite all interested parties to continue the conversation in this location.

There is a good argument that this type of code has no place in Dask, which is concerned with making graphs 
representing computations, and executing those graphs on a scheduler. Indeed, the file-systems are generally useful,
and each has a user-base wider than just those that work via Dask.

## Influences

The following places to consider, when chosing the definitions of how we would like the file-system specification 
to look:

- pythons [os](https://docs.python.org/3/library/os.html) moduler and its `path` namespace; also other file-connected
  functionality in the standard library
- posix/bash method naming conventions that linux/unix/osx users are familiar with; or perhaps their Windows variants
- the existing implementations for the various backends (e.g., 
  [gcsfs](http://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem) or Arrow's 
  [hdfs](https://arrow.apache.org/docs/python/filesystems.html#hdfs-api))
- [pyfilesystems](https://docs.pyfilesystem.org/en/latest/index.html), an attempt to do something similar, with a 
  plugin architecture. This conception has several types of local file-system, and a lot of well-thought-out
  validation code.
  
## Contents of the Repo

The main proposal here is in `fsspec/spec.py`, a single class with methods and doc-strings, and a little code. The
initial method names were copied from `gcsfs`, but this reflects only lazyness on the part of the inital committer.
Although the directory and files appear like a python package, they are not meant for installation or execution
until possibly some later date - or maybe never, if this is to be only loose reference specification.

In addition `fsspec/utils.py` contains a couple of useful functions that Dask happens to rely on; it is envisaged
that if the spec here matures to real code, then a number of helpful functions may live alongside the main
definitions. Furthermore, `fsspec/mapping.py` shows how a key-value map may be easily implemented for all file-systems
for free, by adhering to a single definition of the structure. This is meant as a motivator, and happens to be
particularly useful for the [zarr](https://zarr.readthedocs.io) project.
