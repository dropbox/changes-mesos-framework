#!/usr/bin/env python

from __future__ import absolute_import, print_function

import argparse
import logging
import os
import signal
import sys
import threading

from time import sleep

try:
    from mesos.native import MesosSchedulerDriver
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import MesosSchedulerDriver
    import mesos_pb2

from .changes_scheduler import ChangesScheduler, ChangesAPI, FileBlacklist
from .statsreporter import StatsReporter

# Configuration should contain the file 'blacklist' which
# is a line-separated lists of hosts to blacklist.
#
# NOTE: inside ec2, hostnames look like
# ip-*-*-*-*.region.compute.internal
DEFAULT_CONFIG_DIR = '/etc/changes-mesos-scheduler'


def install_sentry_logger():
    try:
        import raven
    except ImportError:
        logging.warning('Unable to find raven library. Sentry integration disabled.')
        return

    from raven.conf import setup_logging
    from raven.handlers.logging import SentryHandler

    client = raven.Client()
    handler = SentryHandler(client, level=logging.WARN)
    setup_logging(handler)


def run(api_url, mesos_master, user, config_dir, state_file, changes_request_limit, stats=None):
    scheduler = ChangesScheduler(state_file, api=ChangesAPI(api_url), stats=stats,
                                 blacklist=FileBlacklist(os.path.join(config_dir, 'blacklist')),
                                 changes_request_limit=changes_request_limit)

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = os.path.abspath("./executor.py")
    executor.name = "Changes Executor"
    executor.source = "changes"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = user
    framework.name = "Changes Scheduler"
    framework.principal = "changes"
    # Give the scheduler 1 week to restart before mesos cancels the tasks.
    # this is the setting recommended by the docs.
    framework.failover_timeout = 3600 * 24 * 7

    if scheduler.framework_id:
        framework.id.value = scheduler.framework_id
        executor.framework_id.value = scheduler.framework_id

    driver = MesosSchedulerDriver(
        scheduler,
        framework,
        mesos_master)

    stopped = threading.Event()

    def handle_interrupt(signal, frame):
        stopped.set()
        logging.info("Received interrupt, shutting down")
        logging.warning("Not saving state. Will wait for running tasks to finish.")
        scheduler.shuttingDown.set()
        while scheduler.activeTasks > 0:
            logging.info("Waiting for %d tasks to finish running", scheduler.activeTasks)
            sleep(5)
        driver.stop()

    def handle_sigterm(signal, frame):
        stopped.set()
        logging.info("Received sigterm, shutting down")
        scheduler.shuttingDown.set()
        if scheduler.state_file:
            try:
                scheduler.save_state()
                logging.info("Successfully saved state to %s.", state_file)
            except Exception:
                logging.exception("Failed to save state")
                driver.stop()
                return
            # With `failover` set to true, we do not tell Mesos to stop the existing tasks
            # started by this framework. Instead, the tasks will run for
            # `fail_timeout` more seconds set above or we start a scheduler with
            # the same framework id.
            driver.stop(True)
        else:
            logging.warning("State file location not set. Not saving state. Existing builds will be cancelled.")
            driver.stop()

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_sigterm)

    driver.start()
    logging.info("Driver started")

    scheduler.poll_changes_until_shutdown(driver, 5, None)
    scheduler.wait_for_shutdown(driver)
    status = 0 if driver.join() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    if status == 1:
        driver.stop()

    sys.exit(status)


def main():
    parser = argparse.ArgumentParser(description='Mesos HTTP Proxy')

    parser.add_argument('--api-url', required=True,
                        help='URL root of Changes API, including scheme. (e.g. http://localhost:5000/api/0/)')
    parser.add_argument('--mesos-master', default='127.0.1.1:5050',
                        help='Location of Mesos master server. (e.g. 127.0.1.1:5050)')
    parser.add_argument('--user', default='root', help="User to run tasks as")
    parser.add_argument('--log-level', default='info', help="Level to log at. (e.g. info)")
    parser.add_argument('--config-dir', default=DEFAULT_CONFIG_DIR, help='Configuration directory')
    parser.add_argument('--state-file', default=None, help='File path preserve state across restarts')
    parser.add_argument('--statsd-host', default=None, help='Host to report stats to')
    parser.add_argument('--statsd-port', default=8125, type=int, help='Port for on statsd host to send to')
    parser.add_argument('--statsd-prefix', default='changes_scheduler', help='Prefix for stats keys')
    parser.add_argument('--changes-request-limit', default=200, type=int,
                        help='Maximum number of JobSteps to ask Changes for per-request')

    args = parser.parse_args(sys.argv[1:])
    logging.basicConfig(level=getattr(logging, args.log_level.upper()),
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    install_sentry_logger()

    stats = None
    if args.statsd_host:
        stats = StatsReporter({
            'STATSD_HOST':   args.statsd_host,
            'STATSD_PORT':   args.statsd_port,
            'STATSD_PREFIX': args.statsd_prefix,
        }).stats()

    try:
        run(args.api_url, args.mesos_master, args.user, args.config_dir, args.state_file, args.changes_request_limit, stats)
    except Exception as e:
        logging.exception(unicode(e))
        raise

if __name__ == "__main__":
    main()
