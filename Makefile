PKG_NAME = changes-mesos-scheduler
VERSION = 0.0.2
# Revision shows date of latest commit and abbreviated commit SHA
# E.g., 1438708515-753e183
REV=`git show -s --format=%ct-%h HEAD`

test:
	PYTHONPATH=changes_mesos_scheduler py.test changes_mesos_scheduler/tests/

install-test-requirements:
		pip install "file://`pwd`#egg=changes-mesos-scheduler[tests]"

coverage:
		PYTHONPATH=changes_mesos_scheduler coverage run -m py.test --junitxml=python.junit.xml changes_mesos_scheduler/tests/
		PYTHONPATH=changes_mesos_scheduler coverage xml

deb:
	fpm --no-python-fix-name -n $(PKG_NAME) -v "$(VERSION)-$(REV)" -s python -t deb setup.py

.PHONY: deb
