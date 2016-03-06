#!/bin/bash -eux

export DEBIAN_FRONTEND=noninteractive

# Install git
sudo apt-get install -y git

# Install fpm
sudo apt-get install -y ruby-dev gcc
fpm -h > /dev/null || sudo gem install fpm --no-ri --no-rdoc

# Install easy_install for fpm to use
sudo apt-get install -y python-setuptools

# Required for dynamically linked _mesos.so from python egg.
sudo apt-get install -y libcurl3 libsvn1 libsasl2-modules
