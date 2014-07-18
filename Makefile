help:
	@echo "Please use \`$(MAKE) <target>' where <target> is one of the following:"
	@echo "pkg: build .deb package"

pkg:
	rm -f mesos-http-proxy_*.deb
	mkdir -p pkg_root/usr/bin/
	cp mesos-http-proxy pkg_root/usr/bin/
	fpm -s dir -t deb -n mesos-http-proxy -v 0.1.0 -C pkg_root/ .
	rm -rf pkg_root

.PHONY: pkg

