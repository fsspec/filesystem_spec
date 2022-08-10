#!/usr/bin/env python
import os

from setuptools import setup

import versioneer

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="fsspec",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description="File-system specification",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/fsspec/filesystem_spec",
    project_urls={
        "Changelog": "https://filesystem-spec.readthedocs.io/en/latest/changelog.html",
        "Documentation": "https://filesystem-spec.readthedocs.io/en/latest/",
    },
    maintainer="Martin Durant",
    maintainer_email="mdurant@anaconda.com",
    license="BSD",
    keywords="file",
    packages=["fsspec", "fsspec.implementations"],
    python_requires=">=3.7",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={
        "entrypoints": ["importlib_metadata ; python_version < '3.8' "],
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
        "http": ["requests", "aiohttp !=4.0.0a0, !=4.0.0a1"],
        "sftp": ["paramiko"],
        "s3": ["s3fs"],
        "oci": ["ocifs"],
        "smb": ["smbprotocol"],
        "ssh": ["paramiko"],
        "fuse": ["fusepy"],
        "libarchive": ["libarchive-c"],
        "gui": ["panel"],
        "tqdm": ["tqdm"],
    },
    zip_safe=False,
)
