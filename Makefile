PKG_NAME = changes-mesos-scheduler
VERSION = 0.0.2
# Revision shows date of latest commit and abbreviated commit SHA
# E.g., 1438708515-753e183
REV=`git show -s --format=%ct-%h HEAD`

test:
	PYTHONPATH=changes_mesos_scheduler py.test changes_mesos_scheduler/tests/

deb:
	fpm --no-python-fix-name -n $(PKG_NAME) -v "$(VERSION)-$(REV)" -s python -t deb setup.py

.PHONY: deb
