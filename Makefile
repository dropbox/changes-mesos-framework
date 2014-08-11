PKG_NAME = changes-mesos-scheduler
VERSION = 0.0.1
REV=`git rev-list HEAD --count`

deb:
	mkdir -p build/usr/local/bin
	cp changes-mesos-scheduler build/usr/local/bin/
	fpm -s dir -t deb -n $(PKG_NAME) -v "$(VERSION)-$(REV)" -a all -d "python-raven" -C ./build .

.PHONY: deb
