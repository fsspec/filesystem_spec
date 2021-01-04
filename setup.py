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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="File-system specification",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/intake/filesystem_spec",
    maintainer="Martin Durant",
    maintainer_email="mdurant@anaconda.com",
    license="BSD",
    keywords="file",
    packages=["fsspec", "fsspec.implementations"],
    python_requires=">3.6",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={
        ":python_version < '3.8'": ['importlib_metadata'],
        "abfs": ["adlfs"],
        "adl": ["adlfs"],
        "dask": ["dask", "distributed"],
        "dropbox": ["dropboxdrivefs", "requests", "dropbox"],
        "gcs": ["gcsfs"],
        "git": ["pygit2"],
        "github": ["requests"],
        "gs": ["gcsfs"],
        "hdfs": ["pyarrow"],
        "http": ["requests", "aiohttp"],
        "sftp": ["paramiko"],
        "s3": ["s3fs"],
        "smb": ["smbprotocol"],
        "ssh": ["paramiko"],
    },
    zip_safe=False,
)
