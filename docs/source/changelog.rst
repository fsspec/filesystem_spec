Changelog
=========

2024.12.0
---------

Enhancements

- "exclusive" mode for writing (#1762, 1756, 174+)
- "tree" text display of filesystem contents (#1750)
- async wrapper for sync FSs (#1745)
- new known implementation: tosfs (#1739)
- consilidate block fetch requests (#1733)

Fixes

- better webHDFS proxies (#
- syn FSs in referenceFS (#1755)
- don't serialize file caches (#1753)
- race condition in local ls() (#1744)
- missing/nan references in parquet (#1738)
- _un_chain kwargs (@1736)
- async _cat_file in referenceFS (#1734)

Other

- fallback implementation for _fetch_range (#1732)

2024.10.0
---------

Fixes

- Performance of memoryFS rm (#1725)
- Performance of git FS info (#1712)
- Avoid git hex for newer pygit (#1703)
- tests fix for zip (#1700, 1691)
- missing open_async for dirFS (#1698)
- handle pathlib in zip (#1689)
- skip tests needing kerchunk if not installed (#1689)
- allow repeated kwargs in unchain (#1673)

Other

- Code style (#1704, 1706)
- allow pyarrow in referenceFS parquet (#1692)
- don't hardcode test port for parallel runs (#1690)


2024.9.0
--------

Enhancements

- fewer stat calls in localFS (#1659)
- faster find in ZIP (#1664)

Fixes

- paths without "/" in dirFS (#1638)
- paths with "/" in FTS (#1643, 1644)
- ls in parquet-based nested reference sets, and append (#1645, 1657)
- exception handling for SMB (#1650)


Other

- style (#1640, 1641, 1660)
- docs: xrootd (#1646)
- CI back on miniconda (#1658)

2024.6.1
--------

Fixes

- fix appending to non-dict reference sets (#1634)
- don't let generic edit info dicts (#1633)
- set https's loop before calling super (#1633)
- cached write file doesn't need to update it's size on close (#1633)
- fix JSON serialize for FSs with interior FSs (#1628, 1627)
- option to strip "password" when pickling (#1625)
- fix filecache write (#1622)


2024.6.0
--------

Enhancements

- allow dicts (not just bytes) for referenceFS (#1616
- make filesystems JSON serializeable (#1612)
- implement multifile cat() for github (#1620)

Fixes

- implement auto_mkdir for SMB (#1604)

Other

- add doc deps to pyproject (#1613)
- re-remove test from package (#1611)
- formatting (#1610, 1608, 1602)
- change monthly download badge (#1607)

2024.5.0
--------

Enhancements

- log hits/misses in bytes cachers (#1566)

Fixes

- SMB flaky tests (#1597)
- rsync: only delete files if there are some to delete (#1596)
- don't let files equal bytes objects (#1594)
- url_to_fs to stringify paths (#1591)
- assert types in MemoryFS (#1574)
- restore _strip_protocol signature for local (#1567)
- convert list to set when loading cache metadata (#1556)

Other

- remove mv_file (#1585)
- mv() should not swallow errors (#1576)
- change versioning template, allows easier co-install of dev s3fs (#1569)
- in ls_from_cache, avoid dounble lookup (#1561)
- expand=True in open() (#1558)
- build system to hatch (#1553)

2024.3.1
--------

Fixes

- allow override of expand in open() (#1549)
- root handling in local file paths, fix for windows (#1477)

2024.3.0
--------

Enhancements

- coroutines throttle to stream pool rather than batches (#1544)
- write transactions in simplecache (#1531)
- allow deep nested refs in referenceFS/parquet (#1530)

Fixes

- Fixes bug (#1476) that made open_files ignore expand=False (#1536)
- remove extra calling mapper contains (#1546)
- connection retry for SMB (#1533)
- zip64 should be on is allowZip64 is (#1532)

Other

- HTTP logging (#1547)
- url_to_fs exposed in package root (#1540)
- sort known_implementations (#1549)
- code quality/stype (#1538, 1537, 1528, 1526)

2024.2.0
--------

Enhancements

- add 9P known implementation (#1513)
- allow TqdmCallback subclassing (#1497, 1480)
- callbacks/branching kwargs handling and scopes (#1496, 1495, 1460)
- add aluuxioFS to known implementations (#1469)
- implement pipe_file for dirFS (#1465)

Fixes

- infer compression for .lzma files (#1514)
- fix append to categorical/parquet references (#1510)
- allow for FTP servers that list with leading "total" line (#1503)
- convert FTP failure to FileNotFound (#1494)
- out of order reference fix (#1492)
- retry "backoff" response for DBFS (#1491)
- referenceFS case for scalar arrays (#1487)
- fix create_parents for SFTP (#1484)
- fix local .ls() on files (#1479)
- allow Path and similar in _expand_path (#1475)
- make lazy references editable (#1468)
- fix eq for abstract buffered files (#1466)
- fit tqdm cleanup (#1463)
- fix passing kwargs from cached file to underlying FS (#1462)

Other

- fix tests for supports_empty_directories=False (#1512)
- don't read references in init for referenceFS (#1521)
- code cleaning (#1518, 1502, 1499, 1493, 1481)
- pass through "replication" for HDFS (#1486)
- record more info for HTTP info() (#1483)
- add timeout argument to githubFS (#1473)
- add more security pars to webHDFS (#1472)

2023.12.2
---------

Fixes

- top-level glob in ZIP (#1454)
- append mode on local ZIP files/truncate (#1449)
- restrict ":" as protocol marker to data: (#1452)
- sftp relative paths (#1451)
- http encoding in HTTP FS put_file (#1450)


2023.12.1
---------

Fixes

- Remove trailing "/" from directory names in zipFS/archive (#1445)

2023.12.0
---------

Enhancements

- allow HTTP size guess in more circumstances (#1440)
- allow kwargs passed to GUI to be dict (#1437)
- transaction support for writing via a cache FS (#1434)
- make cached FSs work better with async backends (#1429)
- allow FSs to set their transaction implementation (#1424)
- add dataFS (#1421, 1415)
- allow basic auth in webHDFS (#1409)

Fixes

- in referenceFS, maintain order when some keys are omitted in cat (#1436)
- nested subdirectory listing in referenceFS (#1433)
- allow "=" in webHDF paths (#1428)
- fix file mode to consistent "r+b" format (#1426)
- pass on kwargs in HTTP glob (#1422)
- allow Path in can_be_local and open_local (#1419, #1418)
- fix parent for cachedFS (#1413)
- "ends" list in _cat_ranges was incorrect (#1402)

Other

- smarter handling of exceptions when doing auto_mkdir (#1406)


2023.10.0
---------

Enhancements

- alias "local://" to "file://" (#1381)
- get size of file cache (#1377)

Fixes

- stop unexpected kwargs for SMB (#1391)
- dos formatting (#1383)

Other

- small optimisations in referenceFS (#1393)
- define ordering behaviour for entrypoints (#1389)
- style (#1387, 1386, 1385)
- add LazyReferenceMapper to API docs (#1378)
- add PyPI badge to README (#1376)

2023.9.2
--------

Fixes

- revert #1358: auto_mkdir in open() (#1365)

Other

- code style updates (#1373, 1372, 1371, 1370, 1369, 1364)
- update CI setup (#1386)

2023.9.1
--------

Enhancements

- #1353, save file cache metadata in JSON
- #1352, remove some unnecessary list iterations

Fixes

- #1361, re-allow None for default port for SMB
- #1360, initialising GUI widget FS with kwargs
- #1358, pass auto_mkdir vi url_to_fs again

Other

- #1354, auto delete temp cache directory

2023.9.0
--------

Enhancements

- #1346, add ocilake protocol
- #1345, implement async-sync and async-async generic cp and rsync
- #1344, add lakefs protocol
- #1337 add goatcounter to docs
- #1323, 1328, add xethub protocol
- #1320, in HTTP, check content-encoding when getting length
- #1303, add on_error in walk
- #1302, add dirfs attribute to mappers
- #1293, configure port for smb

Fixes

- #1349, don't reorder paths in bulk ops if source and dest are both lists
- #1333, allow mode="x" in get_fs_token_paths
- #1324, allow generic to work with complex URLs
- #1316, exclude bytes-cache kwargs in url_to_fs
- #1314, remote utcnow/utcfromtimestamp
- #1311, dirFS's protocol
- #1305, use get_file rather than get in file caching
- #1295, allow bz2 to be optional

Other

- #1340, 1339, 1329 more bulk ops testing
- #1326, 1296 separate out classes in file caching for future enhancements

2023.6.0
--------

Enhancements

- #1259, add maxdepth fo cp/get/put
- #1263, allow dir modification during walk()
- #1264, add boxfs to registry
- #1266, optimise referenceFS lazy lookups, especially for writing parquet
- #1287, 1288 "encoding" for FTP

Fixes

- #1273, (re)allow reading .zstd reference sets
- #1275, resource.error for win32
- #1278, range reads in dbfs
- #1282, create parent directories in get_file
- #1283, off-by-one in reference block writing
- #1286, strip protocol in local rm_file

Other

- #1267, async bulk tests
- #1268, types and mypy
- #1277, 1279, drop outdated forms io.open, IOError

2023.5.0
--------

Enhancements

- #1236, allow writing ReferenceFS references directly to parquet

Fixes

- #1255, copy of glob to single output directory
- #1254, non-recursive copy of directory (no-op)
- #1253, cleanup fix on close of ZIP FS
- #1250, ignore dirs when copying list of files
- #1249, don't error on register without clobber is registering same thing again
- #1245, special case for other_files and relative path

Other

- #1248, add test harness into released wheel package
- #1247, docs and tests around common bulk file operations


2023.4.0
--------

Enhancements

- #1225, comprehensive docs of expected behaviour of cp/get/put and tests
- #1216, test harness for any backend

Fixes

- #1224, small fixes in reference and dask FSs
- #1218, mv is no-op when origin and destination are the same
- #1217, await in AbstractStreamedFile
- #1215, docbuild fixes
- #1214, unneeded maxdepth manipulation in expand_path
- #1213, pyarros and posixpath related test fixes
- #1211, BackgroundBlockCache: keep a block longer if not yet used
- #1210, webHDFS: location parameter

Other

- #1241, add HfFileSystem to registry
- #1237, register_implementation clobber default changes to False
- #1228, "full" and "devel" installation options
- #1227, register_cache and reporting collision
- #1221, docs about implementations and protocols

2023.3.0
--------

Enhancements

- #1201, add directory FS to the registry and constructable from URLs
- #1194, allow JSON for setting dict-like kwargs in the config
- #1181, give arrow FS proper place in the registry
- #1178, add experimental background-thread buffering cache
- #1162, make ZipFS writable

Fixes

- #1202, fix on_error="omit" when using caching's cat
- #1199, 1163, get/put/cp consistency and empty directories
- #1197, 1183 use bytes for setting value on mapper using numpy
- #1191, clean up open files in spec get_file
- #1164, pass on kwargs correctly to http

Other

- #1186, make seekable=True default for pyarrow files
- #1184, 1185, set minimum python version to 3.8

2023.1.0
--------

Enhancements

- experimental DFReferenceFileSystem (#1157, 1138)
- pyarrow seeking (#1154)
- tar thread safety (#1132)
- fsid method (#1122)

Fixes

- ReferenceFS order fix (#1158)
- fix du and maxdepth (#1128, 1151)
- http ranges (#1141)

Other

- coverage on referenceFS (#1133, 1123)
- docs (#1152, 1150
- remove code duplication in unchain (#1143, 1156, 1121)

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

.. raw:: html

    <script data-goatcounter="https://fsspec.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
