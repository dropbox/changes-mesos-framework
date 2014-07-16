#!/usr/bin/env python

import sys

from setuptools import setup, find_packages


requires = [
  'click>=2.4.0,<2.5.0',
  'Flask>=0.10.0,<0.11.0',
  'requests>=2.3.0,<2.4.0',
]

setup_options = dict(
  name='mesos-http-proxy',
  version='0.1.0',
  description='Mesos HTTP Proxy.',
  author='Brett Hoerner',
  author_email='brett@bretthoerner.com',
  scripts=['scheduler.py'],
  packages=find_packages(),
  install_requires=requires,
  license="Apache License 2.0",
  classifiers=(
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Topic :: Software Development',
  ),
)

setup(**setup_options)
