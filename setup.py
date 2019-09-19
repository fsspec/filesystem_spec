#!/usr/bin/env python
import os

from setuptools import setup
import versioneer

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='fsspec',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
      ],
      description='File-system specification',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='http://github.com/intake/filesystem_spec',
      maintainer='Martin Durant',
      maintainer_email='mdurant@anaconda.com',
      license='BSD',
      keywords='file',
      packages=['fsspec', 'fsspec.implementations'],
      python_requires='>=3.5',
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      zip_safe=False)
