PKG_NAME = changes-mesos-scheduler
VERSION = 0.0.2
REV=`git rev-list HEAD --count`

deb:
	mkdir -p build/usr/local/bin
	cp changes-mesos-scheduler build/usr/local/bin/
	fpm -s dir -t deb -n $(PKG_NAME) -v "$(VERSION)-$(REV)" -a all -C ./build .

.PHONY: deb
