***NOTICE: THIS REPO IS NO LONGER UPDATED***

Changes Mesos Scheduler
=======================
Setting up the vagrant VM:

```shell
vagrant up
vagrant ssh
```

Building a deb:

```shell
cd /vagrant
make deb
```

`make install_deb` will also install the deb on your machine.

Running tests:
```shell
cd /vagrant
make test
```

You can also run tests locally (on your host machine). You need to install
mesos (`brew install mesos` on Mac), and may need to `sudo pip install mesos`
too. After that `make test` should work (mileage may vary, this is only really
tested on Mac).


Running the scheduler requires having mesos set up and running but this vagrant VM is not set up to do that yet. You can instead use a different one:

```shell
git clone git@github.com:mesosphere/playa-mesos.git
cp your-changes-mesos-scheduler.deb playa-mesos/
cd playa-mesos

vagrant up
vagrant ssh

make install_deb
mkdir /etc/changes-mesos-scheduler
sudo touch /etc/changes-mesos-scheduler/blacklist

/usr/share/python/changes-mesos-scheduler/bin/changes-mesos-scheduler --help
/usr/share/python/changes-mesos-scheduler/bin/changes-mesos-scheduler --api-url your-changes-endpoint
```
