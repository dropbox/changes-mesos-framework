PKG_NAME = changes-mesos-scheduler
VERSION = 0.0.2
# Revision shows date of latest commit and abbreviated commit SHA
# E.g., 1438708515-753e183
REV=`git show -s --format=%ct-%h HEAD`

deb:
	mkdir -p build/usr/local/bin
	cp changes-mesos-scheduler build/usr/local/bin/
	fpm -s dir -t deb -n $(PKG_NAME) -v "$(VERSION)-$(REV)" -a all -C ./build .

.PHONY: deb
