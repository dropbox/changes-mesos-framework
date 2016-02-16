from distutils.core import setup

MESOS_VERSION = '0.20.0'
UBUNTU_VERSION = '14.04'

tests_require = ['pytest>=2.5.0,<2.6.0', 'pytest-cov>=1.6,<1.7',
                 'pytest-xdist>=1.9,<1.10', 'unittest2>=0.5.1,<0.6.0',
                 'mock>=1.0.1,<1.1.0', 'flask>=0.10.1,<0.11.0']

setup(name='changes-mesos-scheduler',
      scripts=['scripts/changes-mesos-scheduler'],
      packages=['changes_mesos_scheduler'],
      extras_require={'tests': tests_require},
      dependency_links = ['http://downloads.mesosphere.io/master/ubuntu/%s/mesos-%s-py2.7-linux-x86_64.egg#egg=mesos'
                          % (UBUNTU_VERSION, MESOS_VERSION)],
      install_requires=['statsd', 'mesos'],
      package_dir={'changes_mesos_scheduler': 'changes_mesos_scheduler'})

