#!/usr/bin/env python

from setuptools import setup
import versioneer

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
      url='http://github.com/intake/filesystem_spec',
      maintainer='Martin Durant',
      maintainer_email='mdurant@anaconda.com',
      license='BSD',
      keywords='file',
      packages=['fsspec', 'fsspec.implementations'],
      python_requires='>=3.5',
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      zip_safe=False)
