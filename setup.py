#!/usr/bin/env python

import sys

from setuptools import setup, find_packages


setup_options = dict(
  name='mesos-http-proxy',
  version='0.1.0',
  description='Mesos HTTP Proxy.',
  author='Brett Hoerner',
  author_email='brett@bretthoerner.com',
  scripts=['mesos-http-proxy'],
  packages=find_packages(),
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
