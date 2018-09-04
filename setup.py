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
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
      ],
      description='File-system specification',
      url='http://github.com/martindurant/filesystem_spec',
      maintainer='Martin Durant',
      maintainer_email='mdurant@anaconda.com',
      license='BSD',
      keywords='file',
      packages=['fsspec'],
      python_requires='>= 2.7, != 3.0.*, != 3.1.*, != 3.2.*, != 3.3.*, != 3.4.*',
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      zip_safe=False)
