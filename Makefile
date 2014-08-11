PKG_NAME = mesos-http-proxy
VERSION = 0.0.1
REV=`git rev-list HEAD --count`

deb:
	mkdir -p build/usr/local/bin
	cp mesos-http-proxy build/usr/local/bin/
	fpm -s dir -t deb -n $(PKG_NAME) -v "$(VERSION)-$(REV)" -a all -C ./build .

.PHONY: deb
