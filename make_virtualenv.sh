#! /usr/bin/env bash
set -xe

# Based on: https://github.com/brutasse/graphite-api/blob/master/fpm/build-deb.sh
# but adapted to use easy_install

export PROJECT=$1

sudo apt-get -y install build-essential python-dev python-virtualenv 

rm -rf build

mkdir -p build/usr/share/python
virtualenv build/usr/share/python/$PROJECT

build/usr/share/python/$PROJECT/bin/easy_install virtualenv-tools
# Actually install our project
build/usr/share/python/$PROJECT/bin/easy_install .

# can't seem to do this with easy_install
# ideally we wouldn't install test requirements for the deb we install
build/usr/share/python/$PROJECT/bin/pip install "file://`pwd`#egg=$PROJECT[tests]"

find build ! -perm -a+r -exec chmod a+r {} \;

cd build/usr/share/python/$PROJECT
# Not sure if this is necessary
sed -i "s/'\/bin\/python'/\('\/bin\/python','\/bin\/python2'\)/g" lib/python2.7/site-packages/virtualenv_tools-*-py2.7.egg/virtualenv_tools.py
./bin/virtualenv-tools --update-path /usr/share/python/$PROJECT
cd -

find build -iname *.pyc -exec rm {} \;
find build -iname *.pyo -exec rm {} \;
