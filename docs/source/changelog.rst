Changelog
=========

2022.11.0
---------

Enhancements

- Speed up FSMap._key_to_str (#1101)
- Add modified/created to Memory and Arrow (#1096)
- Clear expired cache method (#1092)
- Allow seekable arrow file (#1091)
- Allow append for arrow (#1089)
- recursive for sftp.get (#1082)
- topdown arg to walk() (#1081)

Fixes

- fix doc warnings (#1106, #1084)
- Fix HDFS _strip_protocol (#1103)
- Allow URLs with protocol for HDFS (#1099)
- yarl in doc deps (#1095)
- missing await in genericFS.cp (#1094)
- explicit IPv4 for test HTTP server (#1088)
- sort when merging ranges for referenceFS (#1087)

Other

- Check that snappy is snappy (#1079)

2022.10.0
---------

Enhancements

- referenceFS consolidates reads in the same remote file (#1063)
- localfs: add link/symlink/islink (#1059)
- asyncfs: make mirroring methods optional (#1054)
- local: info: provide st_ino and st_nlink from stat (#1053)
- arrow_hdfs replaces hdfs (#1051)
- Add read/write_text (#1047)
- Add pipe/cat to genericFS (#1038)

Fixes

- SSH write doesn't return number of bytes (#1072)
- wrap flush method for LocalFileOpened (#1070)
- localfs: fix support for pathlib/os.PathLike objects in rm (#1058)
- don't get_file remote FTP directory (#1056)
- fix zip write to remote (#1046)
- fix zip del following failed init (#1040)

Other

- add asynclocalfs to the registry (#1060)
- add DVCFileSystem to the registry (#1049)
- add downstream tests (#1037)
- Don't auto-close OpenFiles (#1035)

2022.8.2
--------

- don't close OpenFile on del (#1035)

2022.8.1
--------

- revert #1024 (#1029), with strciter requirements on OpenFile usage

2022.8.0
--------

Enhancements

- writable ZipFileSystem (#1017)
- make OpenFile behave like files and remove dynamic closer in .open() (#1024)
- use isal gunzip (#1008)

Fixes

- remove strip from _parent (#1022)
- disallow aiohttp prereleases (#1018)
- be sure to close cached file (#1016)
- async rm in reverse order (#1014)
- expose fileno in LocalFileOpener (#1010, #1005)
- remove temp files with simplecache writing (#1006)
- azure paths (#1003)
- copy dircache keys before iter


2022.7.1
--------

Fixes

- Remove fspath from LocalFileOpener (#1005)
- Revert 988 (#1003)

2022.7.0
--------

Enhancements

- added fsspec-xrootd implementation to registry (#1000)
- memory file not to copy bytes (#999)
- Filie details passed to FUSE (#972)

Fixes

- Return info for root path of archives (#996)
- arbitrary kwargs passed through in pipe_file (#993)
- special cases for host in URLs for azure (#988)
- unstrip protocol criterion (#980)
- HTTPFile serialisation (#973)

Other

- Show erroring path in FileNotFounds (#989)
- Reference file info without searching directory tree (#985)
- Truncate for local files (#975)


2022.5.0
--------

Enhancements

- mutable ReferenceFS (#958)

Fixes

- Make archive FSs not cachable (#966)
- glob fixes (#961)
- generic copy with unknown size (#959)
- zstd open (#956)
- empty archive file (#954)
- tar chaining (#950, 947)
- missing exceptions in mapper (#940)

Other

- update registry (#852)
- allow None cache (#942)
- mappers to remember init arguments (#939)
- cache docstrings (#933)

2022.03.0
---------

Enhancements

- tqdm example callback with simple methods (#931, 902)
- Allow empty root in get_mapper (#930)
- implement real info for reference FS (#919)
- list known implementations and compressions (#913)

Fixes

- git branch for testing git backend (#929)
- maintain mem FS's root (#926)
- kargs to FS in parquet module (#921)
- fix on_error in references (#917)
- tar ls consistency (#9114)
- pyarrow: don't decompress twice (#911)
- fix FUSE tests (#905)


2022.02.0
---------

Enhancements

- reference FS performance (#892, 900)
- directory/prefix FS (#745)

Fixes

- FUSE (#905, 891)
- iteration in threads (#893)
- OpenFiles slicing (#887)

Other

- drop py36 (#889, 901)

2022.01.0
---------

Fixes

- blocks cache metadata (#746)
- default SMB port (#853)
- caching fixes (#856, 855)
- explicit close for http files (#866)
- put_file to continue when no bytes (#869, 870)

Other

- temporary files location (#851, 871)
- async abstract methods (#858, 859, 860)
- md5 for FIPS (#872)
- remove deprecated pyarrow/distutils (#880, 881)

2021.11.1
---------

Enhancements

- allow compression for fs.open (#826)
- batch more async operations (#824)
- allow github backend for alternate URL (#815)
- speec up reference filesystem (#811)

Fixes

- fixes for parquet functionality (#821, 817)
- typos and docs (#839, 833, 816)
- local root (#829)

Other

- remove BlockSizeError for http (#830)

2021.11.0
---------

Enhancement

- parquet-specific module and cache type (#813, #806)

Fixes

- empty ranges (#802, 801, 803)
- doc typos (#791, 808)
- entrypoints processing (#784)
- cat in ZIP (#789)

Other

- move to fsspec org
- doc deps (#786, 791)

2021.10.1
---------

Fixes

- Removed inaccurate ``ZipFileSystem.cat()`` override so that the base
  class' version is used (#789)
- fix entrypoint processing (#784)
- case where no blocks of a block-cache have yet been loaded (#801)
- don't fetch empty ranges (#802, 803)

Other

- simplify doc deps (#786, 791)


2021.10.0
---------

Fixes

- only close http connector if present (#779)
- hdfs strip protocol (#778)
- fix filecache with check_files (#772)
- put_file to use _parent (#771)

Other

- add kedro link (#781)

2021.09.0
---------

Enhancement

- http put from file-like (#764)
- implement webhdfs cp/rm_file (#762)
- multiple (and concurrent) cat_ranges (#744)

Fixes

- sphinx warnings (#769)
- lexists for links (#757)
- update versioneer (#750)
- hdfs updates (#749)
- propagate async timeout error (#746)
- fix local file seekable (#743)
- fix http isdir when does not exist (#741)

Other

- ocifs, arrow added (#754, #765)
- promote url_to_fs to top level (#753)

2021.08.1
---------

Enhancements

- HTTP get_file/put_file APIs now support callbacks (#731)
- New HTTP put_file method for transferring data to the remote server (chunked) (#731)
- Customizable HTTP client initializers (through passing ``get_client`` argument) (#731, #701)
- Support for various checksum / fingerprint headers in HTTP ``info()`` (#731)
- local implementation of rm_file (#736)
- local speed improvements (#711)
- sharing options in SMB (#706)
- streaming cat/get for ftp (#700)

Fixes

- check for remote directory when putting (#737)
- storage_option update handling (#734(
- await HTTP call before checking status (#726)
- ftp connect (#722)
- bytes conversion of times in mapper (#721)
- variable overwrite in WholeFileCache cat (#719)
- http file size again (#718)
- rm and create directories in ftp (#716, #703)
- list of files in async put (#713)
- bytes to dict in cat (#710)


2021.07.0
---------

Enhancements

- callbacks (#697)

2021.06.1
---------

Enhancements

- Introduce ``fsspec.asyn.fsspec_loop`` to temporarily switch to the fsspec loop. (#671)
- support list for local rm (#678)

Fixes

- error when local mkdir twice (#679)
- fix local info regression for pathlike (#667)

Other

- link to wandbfs (#664)

2021.06.0
---------

Enhancements

- Better testing and folder handling for Memory (#654)
- Negative indexes for cat_file (#653)
- optimize local file listing (#647)

Fixes

- FileNoteFound in http and range exception subclass (#649, 646)
- async timeouts (#643, 645)
- stringify path for pyarrow legacy (#630)


Other

- The ``fsspec.asyn.get_loop()`` will always return a loop of a selector policy (#658)
- add helper to construct Range headers for cat_file (#655)


2021.05.0
---------


Enhancements

- Enable listings cache for HTTP filesystem (#560)
- Fold ZipFileSystem and LibArchiveFileSystem into a generic implementation and
  add new TarFileSystem (#561)
- Use throttling for the ``get``/``put`` methods of ``AsyncFileSystem`` (#629)
- rewrite for archive filesystems (#624)
- HTTP listings caching (#623)

Fixes

- gcsfs tests (#638)
- stringify_path for arrow (#630)

Other

- s3a:// alias


2021.04.0
---------

Major changes

- calendar versioning

Enhancements

- better link and size finding for HTTP (#610, %99)
- link following in Local (#608)
- ReferenceFileSystem dev (#606, #604, #602)

Fixes

- drop metadata dep (#605)


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
- fix recursive/maxdepth for cp (#508)
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
- memory fs created time in detailed ``ls```


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
* Fixed a bug in ``fsspec.FSMap.keys`` returning a generator, which was consumed upon iteration (:pr:`189`).
* Removed the magic addition of aliases in ``AbstractFileSystem.__init__``. Now alias methods are always
  present (:pr:`177`)
* Deprecated passing ``trim`` to :class:`fsspec.spec.AbstractBufferedFile`. Pass it in ``storage_options`` instead (:pr:`188`)
* Improved handling of requests for :class:`fsspec.implementations.http.HTTPFileSystem` when the
  HTTP server responds with an (incorrect) content-length of 0 (:pr:`163`)
* Added a ``detail=True`` parameter to :meth:`fsspec.spec.AbstractFileSystem.ls` (:pr:`168`)
* Fixed handling of UNC/DFS paths (:issue:`154`)
