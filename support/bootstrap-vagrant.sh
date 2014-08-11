#!/bin/bash -eux

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update -y

# Install git
sudo apt-get install -y git

# Install fpm
sudo apt-get install -y ruby-dev gcc
sudo gem install fpm --no-ri --no-rdoc
