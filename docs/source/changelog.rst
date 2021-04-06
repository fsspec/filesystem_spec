Changelog
=========

Dev
---


0.9.0
-----

Major Changes:

- avoid nested sync calls by copying code (#581, #586, docs #593)
- release again for py36 (#564, #575)

Enhancements:

- logging in mmap cacher, explicitly close files (#559)
- make LocalFileOpener an IOBase (#589)
- better reference file system (#568, #582, #584, #585)
- first-chunk cache (#580)
- sftp listdir (#571)
- http logging and fetch all (#551, #558)
- doc: entry points (#548)

Fixes:

- get_mapper for caching filesystems (#559)
- fix cross-device file move (#547)
- store paths without trailing "/" for DBFS (#557)
- errors that happen on ``_initiate_upload`` when closing the
  ``AbstractBufferedFile`` will now be propagated (#587)
- infer_compressions with upper case suffix ($595)
- file initialiser errors (#587)
- CI fix (#563)
- local file commit cross-device (#547)

Version 0.8.7
-------------

Fixes:

- fix error with pyarrow metadata for some pythons (#546)

Version 0.8.6
-------------

Features:

- Add dbfs:// support (#504, #514)

Enhancements

- don't import pyarrow (#503)
- update entry points syntax (#515)
- ci precommit hooks (#534)

Fixes:

- random appending of a directory within the filesystems ``find()`` method (#507, 537)
- fix git tests (#501)
- fix recursive memfs operations (#502)
- fix recorsive/maxdepth for cp (#508)
- fix listings cache timeout (#513)
- big endian bytes tests (#519)
- docs syntax (#535, 524, 520, 542)
- transactions and reads (#533)

Version 0.8.5
-------------

Features:

- config system
- libarchive implementation
- add reference file system implementation

Version 0.8.4
-------------

Features:

- function ``can_be_local`` to see whether URL is compatible with ``open_local``
- concurrent cat with filecaches, if backend supports it
- jupyter FS

Fixes:

- dircache expiry after transaction
- blockcache garbage collection
- close for HDFS
- windows tests
- glob depth with "**"

Version 0.8.3
-------------

Features:

- error options for cat
- memory fs created time in detailed `ls`


Fixes:

- duplicate directories could appear in MemoryFileSystem
- Added support for hat dollar lbrace rbrace regex character escapes in glob
- Fix blockcache (was doing unnecessary work)
- handle multibyte dtypes in readinto
- Fix missing kwargs in call to _copy in asyn

Other:

- Stop inheriting from pyarrow.filesystem for pyarrow>=2.0
- Raise low-level program friendly OSError.
- Guard against instance reuse in new processes
- Make hash_name a method on CachingFileSystem to make it easier to change.
- Use get_event_loop for py3.6 compatibility

Version 0.8.2
-------------

Fixes:

- More careful strip for caching

Version 0.8.1
-------------

Features:

- add sign to base class
- Allow calling of coroutines from normal code when running async
- Implement writing for cached many files
- Allow concurrent caching of remote files
- Add gdrive:// protocol

Fixes:

- Fix memfs with exact ls
- HTTPFileSystem requires requests and aiohttp in registry

Other:

- Allow http kwargs to clientSession
- Use extras_require in setup.py for optional dependencies
- Replacing md5 with sha256 for hash (CVE req)
- Test against Python 3.8, drop 3.5 testing
- add az alias for abfs

Version 0.8.0
-------------

Major release allowing async implementations with concurrent batch
operations.

Features:

- async filesystem spec, first applied to HTTP
- OpenFiles cContext for multiple files
- Document async, and ensure docstrings
- Make LocalFileOpener iterable
- handle smb:// protocol using smbprotocol package
- allow Path object in open
- simplecache write mode

Fixes:

- test_local: fix username not in home path
- Tighten cacheFS if dir deleted
- Fix race condition of lzma import when using threads
- properly rewind MemoryFile
- OpenFile newline in reduce

Other:

- Add aiobotocore to deps for s3fs check
- Set default clobber=True on impl register
- Use _get_kwargs_from_url when unchaining
- Add cache_type and cache_options to HTTPFileSystem constructor

Version 0.7.5
-------------

* async implemented for HTTP as prototype (read-only)
* write for simplecache
* added SMB (Samba, protocol >=2) implementation

Version 0.7.4
-------------

* panel-based GUI

0.7.3 series
------------

* added ``git`` and ``github`` interfaces
* added chained syntax for open, open_files and get_mapper
* adapt webHDFS for HttpFS
* added open_local
* added ``simplecache``, and compression to both file caches


Version 0.6.2
-------------

* Added ``adl`` and ``abfs`` protocols to the known implementations registry (:pr:`209`)
* Fixed issue with whole-file caching and implementations providing multiple protocols (:pr:`219`)

Version 0.6.1
-------------

* ``LocalFileSystem`` is now considered a filestore by pyarrow (:pr:`211`)
* Fixed bug in HDFS filesystem with ``cache_options`` (:pr:`202`)
* Fixed instance caching bug with multiple instances (:pr:`203`)


Version 0.6.0
-------------

* Fixed issues with filesystem instance caching. This was causing authorization errors
  in downstream libraries like ``gcsfs`` and ``s3fs`` in multi-threaded code (:pr:`155`, :pr:`181`)
* Changed the default file caching strategy to :class:`fsspec.caching.ReadAheadCache` (:pr:`193`)
* Moved file caches to the new ``fsspec.caching`` module. They're still available from
  their old location in ``fsspec.core``, but we recommend using the new location for new code (:pr:`195`)
* Added a new file caching strategy, :class:`fsspec.caching.BlockCache` for fetching and caching
  file reads in blocks (:pr:`191`).
* Fixed equality checks for file system instance to return ``False`` when compared to objects
  other than file systems (:pr:`192`)
* Fixed a bug in :meth:`fsspec.FSMap.keys` returning a generator, which was consumed upon iteration (:pr:`189`).
* Removed the magic addition of aliases in ``AbstractFileSystem.__init__``. Now alias methods are always
  present (:pr:`177`)
* Deprecated passing ``trim`` to :class:`fsspec.spec.AbstractBufferedFile`. Pass it in ``storage_options`` instead (:pr:`188`)
* Improved handling of requests for :class:`fsspec.implementations.http.HTTPFileSystem` when the
  HTTP server responds with an (incorrect) content-length of 0 (:pr:`163`)
* Added a ``detail=True`` parameter to :meth:`fsspec.spec.AbstractFileSystem.ls` (:pr:`168`)
* Fixed handling of UNC/DFS paths (:issue:`154`)
