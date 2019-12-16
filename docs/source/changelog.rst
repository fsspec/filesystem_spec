Changelog
=========

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
