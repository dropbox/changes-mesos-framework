from distutils.core import setup

tests_require = ['pytest>=2.5.0,<2.6.0', 'pytest-cov>=1.6,<1.7',
                 'pytest-xdist>=1.9,<1.10', 'unittest2>=0.5.1,<0.6.0',
                 'mock>=1.0.1,<1.1.0', 'flask>=0.10.1,<0.11.0']

setup(name='changes-mesos-scheduler',
      scripts=['scripts/changes-mesos-scheduler'],
      packages=['changes_mesos_scheduler'],
      extras_require={'tests': tests_require},
      install_requires=['statsd'],
      package_dir={'changes_mesos_scheduler': 'changes_mesos_scheduler'})
