PKG_NAME = changes-mesos-scheduler
VERSION = 0.0.2
# Revision shows date of latest commit and abbreviated commit SHA
# E.g., 1438708515-753e183
REV=`git show -s --format=%ct-%h HEAD`

DEB_VERSION = "$(VERSION)-$(REV)"

test:
	py.test changes_mesos_scheduler/tests/

install-test-requirements:
	pip install "file://`pwd`#egg=changes-mesos-scheduler[tests]"

coverage:
	coverage run -m py.test --junitxml=python.junit.xml changes_mesos_scheduler/tests/
	coverage xml

virtualenv:
	./make_virtualenv.sh $(PKG_NAME)

deb: virtualenv
	fpm -f -t deb -s dir -C build -n $(PKG_NAME) -v $(DEB_VERSION) .

install_deb: deb
	sudo dpkg -i "$(PKG_NAME)_$(DEB_VERSION)_amd64.deb"

virtualenv_coverage: install_deb
	. /usr/share/python/$(PKG_NAME)/bin/activate; \
	make coverage

virtualenv_test: install_deb
	. /usr/share/python/$(PKG_NAME)/bin/activate; \
	make test

.PHONY: deb
