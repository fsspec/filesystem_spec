#!/usr/bin/env python
import os

from setuptools import setup

import versioneer

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

extras_require = {
    "entrypoints": [],
    "abfs": ["adlfs"],
    "adl": ["adlfs"],
    "dask": ["dask", "distributed"],
    "dropbox": ["dropboxdrivefs", "requests", "dropbox"],
    "gcs": ["gcsfs"],
    "git": ["pygit2"],
    "github": ["requests"],
    "gs": ["gcsfs"],
    "hdfs": ["pyarrow >= 1"],
    "arrow": ["pyarrow >= 1"],
    "http": ["aiohttp !=4.0.0a0, !=4.0.0a1"],
    "sftp": ["paramiko"],
    "s3": ["s3fs"],
    "oci": ["ocifs"],
    "smb": ["smbprotocol"],
    "ssh": ["paramiko"],
    "fuse": ["fusepy"],
    "libarchive": ["libarchive-c"],
    "gui": ["panel"],
    "tqdm": ["tqdm"],
}
# To simplify full installation
extras_require["full"] = sorted(set(sum(extras_require.values(), [])))

extras_require["devel"] = [
    "pytest",
    "pytest-cov",
    # might want to add other optional depends which are used exclusively
    # in the tests or not listed/very optional for other extra depends, e.g.
    # 'pyftpdlib',
    # 'fastparquet',
]

setup(
    name="fsspec",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    description="File-system specification",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fsspec/filesystem_spec",
    project_urls={
        "Changelog": "https://filesystem-spec.readthedocs.io/en/latest/changelog.html",
        "Documentation": "https://filesystem-spec.readthedocs.io/en/latest/",
    },
    maintainer="Martin Durant",
    maintainer_email="mdurant@anaconda.com",
    license="BSD",
    keywords="file",
    packages=["fsspec", "fsspec.implementations", "fsspec.tests.abstract"],
    python_requires=">=3.8",
    extras_require=extras_require,
    zip_safe=False,
)
