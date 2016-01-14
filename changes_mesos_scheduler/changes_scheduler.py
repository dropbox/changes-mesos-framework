from __future__ import absolute_import, print_function

import json
import logging
import os
import time
import urllib2

from changes_mesos_scheduler import statsreporter

from threading import Event
from uuid import uuid4

from google.protobuf import text_format as _text_format

try:
    from mesos.interface import Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Scheduler
    import mesos_pb2


class FileBlacklist(object):
    """ File-backed blacklist for slave hostnames.
    Hosts are expected to be named in the file, one per line.
    Whitespace and lines beginning with '#' are ignored.
    """
    def __init__(self, path):
        self._path = path
        self._mtime = 0
        self._blacklist = set()

    def refresh(self):
        """Refresh the blacklist if the file changed."""
        if os.path.getmtime(self._path) > self._mtime:
            self._refresh()

    def _refresh(self):
        """Unconditionally refresh the blacklist from the file."""
        logging.info('Refreshing blacklist')
        self._mtime = os.path.getmtime(self._path)
        with open(self._path) as file:
            self._blacklist = set([s.strip() for s in file.readlines() if not s.startswith('#')])

    def contains(self, hostname):
        """Returns whether the provided hostname is present in the blacklist as of last reading."""
        return hostname in self._blacklist


class APIError(Exception):
    """An Exception originating from ChangesAPI.
    This mostly exists so that our uncertainty of the possible Exceptions
    originating from API requests doesn't muddy the error handling in the Scheduler.
    """
    def __init__(self, msg, cause=None):
        super(APIError, self).__init__(msg)
        self.cause = cause


class ChangesAPI(object):
    """Client for the Changes API, intended for Scheduler use.
    Any exceptions resulting from runtime failures should be APIErrors.
    """

    def __init__(self, api_url):
        self._api_url = api_url

    def _api_request(self, path, body):
        full_url = self._api_url + path
        try:
            req = urllib2.Request(
                full_url, json.dumps(body),
                {'Content-Type': 'application/json'})
            # Any connectivity issues will raise an exception, as will some error statuses.
            content = urllib2.urlopen(req).read()
            return json.loads(content)
        except Exception as exc:
            # Always log exceptions so callers don't have to.
            logging.exception("Error POSTing to Changes at %s", full_url)
            raise APIError("Error POSTing to Changes at %s" % full_url, exc)

    def allocate_jobsteps(self, info):
        """ Given resource constraints for a slave, return a list of JobSteps to run on it.

        Args:
            info (dict): Dictionary of offer info.

        Returns:
            list: List of JobSteps to allocate.
        """
        return self._api_request("/jobsteps/allocate/", info)

    def update_jobstep(self, jobstep_id, status, result=None):
        """ Update the recorded status and possibly result of a JobStep in Changes.

        Args:
            jobstep_id (str): JobStep ID.
            status (str): Status (one of "finished", "queued", "in_progress").
            result (str): Optionally one of 'failed', 'passed', 'aborted', 'skipped', or 'infra_failed'.
        """
        data = {"status": status}
        if result:
            data["result"] = result
        self._api_request("/jobsteps/{}/".format(jobstep_id), data)

    def jobstep_console_append(self, jobstep_id, text):
        """ Append to the JobStep's console log.
        Args:
            jobstep_id (str): JobStep ID.
            text (str): Text to append.
        """
        url = '/jobsteps/%s/logappend/' % jobstep_id
        self._api_request(url, {'source': 'console', 'text': text})


class ChangesScheduler(Scheduler):
    def __init__(self, state_file, api, blacklist, stats=None):
        """
        Args:
            state_file (str): Path where serialized internal state will be stored.
            api (ChangesAPI): API to use for interacting with Changes.
            blacklist (FileBlacklist): Blacklist to use.
            stats (statsreporter.Stats): Optional Stats instance to use.
        """
        self.framework_id = None
        self._changes_api = api
        self.taskJobStepMapping = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.shuttingDown = Event()
        # Use the provided Stats or create a no-op one.
        self._stats = stats or statsreporter.Stats(None)
        self._blacklist = blacklist
        # Refresh now so that if it fails, it fails at startup.
        self._blacklist.refresh()
        self.state_file = state_file

        # Restore state from a previous run
        if not self.state_file:
            logging.warning("State file location not set. Not restoring old state.")
        elif not os.path.exists(self.state_file):
            logging.warning("State file not found. Not restoring old state.")
        else:
            try:
                self.restore_state()
            except Exception:
                logging.exception("Failed to restore state. Continuing as a new scheduler.")
            else:
                # Delete the old file to prevent it from being used again on a restart
                # as it will likely be stale.
                os.remove(self.state_file)

    def registered(self, driver, frameworkId, masterInfo):
        """
          Invoked when the scheduler successfully registers with a Mesos master.
          It is called with the frameworkId, a unique ID generated by the
          master, and the masterInfo which is information about the master
          itself.
        """
        logging.info("Registered with framework ID %s", frameworkId.value)
        self.framework_id = frameworkId.value

    def reregistered(self, driver, masterInfo):
        """
          Invoked when the scheduler re-registers with a newly elected Mesos
          master.  This is only called when the scheduler has previously been
          registered.  masterInfo contains information about the newly elected
          master.
        """
        logging.info("Re-Registered with new master")

    def disconnected(self, driver):
        """
          Invoked when the scheduler becomes disconnected from the master, e.g.
          the master fails and another is taking over.
        """
        logging.info("Disconnected from master")

    @staticmethod
    def _decode_typed_field(pb):
        field_type = pb.type
        if field_type == mesos_pb2.Value.SCALAR:
            return pb.scalar.value
        elif field_type == mesos_pb2.Value.RANGES:
            return [{"begin": ra.begin, "end": ra.end} for ra in pb.ranges.range]
        elif field_type == mesos_pb2.Value.SET:
            return pb.set.item
        elif field_type == mesos_pb2.Value.TEXT:
            return pb.text.value
        else:
            raise Exception("Unknown field type: %s", field_type)

    @staticmethod
    def _decode_attribute(attr_pb):
        return (attr_pb.name, ChangesScheduler._decode_typed_field(attr_pb))

    @staticmethod
    def _decode_resource(resource_pb):
        return (resource_pb.name, ChangesScheduler._decode_typed_field(resource_pb))

    @property
    def activeTasks(self):
        return self.tasksFinished - self.tasksLaunched

    def resourceOffers(self, driver, offers):
        """
          Invoked when resources have been offered to this framework. A single
          offer will only contain resources from a single slave.  Resources
          associated with an offer will not be re-offered to _this_ framework
          until either (a) this framework has rejected those resources (see
          SchedulerDriver.launchTasks) or (b) those resources have been
          rescinded (see Scheduler.offerRescinded).  Note that resources may be
          concurrently offered to more than one framework at a time (depending
          on the allocator being used).  In that case, the first framework to
          launch tasks using those resources will be able to use them while the
          other frameworks will have those resources rescinded (or if a
          framework has already launched tasks with those resources then those
          tasks will fail with a TASK_LOST status and a message saying as much).
        """
        logging.info("Got %d resource offers", len(offers))
        self._stats.incr('offers', len(offers))

        self._blacklist.refresh()

        for offer in offers:
            if self.shuttingDown.is_set():
                logging.info("Shutting down, declining offer: %s", offer.id)
                driver.declineOffer(offer.id)
                continue

            if self._blacklist.contains(offer.hostname):
                logging.info("Declining offer from blacklisted hostname: %s", offer.hostname)
                driver.declineOffer(offer.id)
                continue

            # protobuf -> dict
            info = {
                "resources": {name: value
                              for (name, value)
                              in [ChangesScheduler._decode_resource(r) for r in offer.resources]},
            }
            attributes = dict([ChangesScheduler._decode_attribute(a) for a in offer.attributes])
            if 'labels' in attributes:
                info['cluster'] = attributes['labels']
            logging.debug("Offer: %s", json.dumps(info, sort_keys=True, indent=2, separators=(',', ': ')))

            # hit service with our offer
            try:
                tasks_to_run = self._changes_api.allocate_jobsteps(info)
            except APIError:
                driver.declineOffer(offer.id)
                continue

            if len(tasks_to_run) == 0:
                logging.info("No tasks to run, declining offer: %s", offer.id)
                driver.declineOffer(offer.id)
                continue

            tasks = []
            for task_to_run in tasks_to_run:
                tid = uuid4().hex
                self.tasksLaunched += 1

                logging.info("Accepting offer on %s to start task %s", offer.hostname, tid)

                task = mesos_pb2.TaskInfo()
                task.name = "{} {}".format(
                    task_to_run['project']['slug'],
                    task_to_run['id'],
                )
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value

                cmd = task_to_run["cmd"]

                task.command.value = cmd
                logging.debug("Scheduling cmd: %s", cmd)

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = task_to_run["resources"]["cpus"]

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = task_to_run["resources"]["mem"]

                tasks.append(task)

                self.taskJobStepMapping[task.task_id.value] = task_to_run['id']
            driver.launchTasks(offer.id, tasks)

    def offerRescinded(self, driver, offerId):
        """
          Invoked when an offer is no longer valid (e.g., the slave was lost or
          another framework used resources in the offer.) If for whatever reason
          an offer is never rescinded (e.g., dropped message, failing over
          framework, etc.), a framwork that attempts to launch tasks using an
          invalid offer will receive TASK_LOST status updats for those tasks
          (see Scheduler.resourceOffers).
        """
        logging.info("Offer rescinded: %s", offerId.value)

    def statusUpdate(self, driver, status):
        """
          Invoked when the status of a task has changed (e.g., a slave is lost
          and so the task is lost, a task finishes and an executor sends a
          status update saying so, etc.) Note that returning from this callback
          acknowledges receipt of this status update.  If for whatever reason
          the scheduler aborts during this callback (or the process exits)
          another status update will be delivered.  Note, however, that this is
          currently not true if the slave sending the status update is lost or
          fails during that time.
        """

        states = {
            0: "starting",
            1: "running",
            2: "finished",  # terminal
            3: "failed",  # terminal
            4: "killed",  # terminal
            5: "lost",  # terminal
            6: "staging",
        }

        state = states[status.state]
        logging.info("Task %s is in state %d", status.task_id.value, status.state)

        jobstep_id = self.taskJobStepMapping.get(status.task_id.value)

        if status.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            self.taskJobStepMapping.pop(status.task_id.value, None)

        if not jobstep_id:
            # TODO(dcramer): how does this happen?
            logging.error("Task %s is missing JobStep ID (state %s, message %s)", status.task_id.value, state, 
                          _text_format.MessageToString(status))
            return

        if state == 'finished':
            try:
                self._changes_api.update_jobstep(jobstep_id, status="finished")
            except APIError:
                pass
        elif state in ('killed', 'lost', 'failed'):
            # Jobsteps are only intended to be executed once and should only exit non-zero or be
            # lost/killed by infrastructural issues, so we don't attempt to reschedule, and we mark
            # this down as an infrastructural failure. Note that this state may not mean that the
            # Jobstep will necessarily stop executing, but it means that the results will be
            # considered immediately invalid.
            logging.warn('Task %s %s: %s', jobstep_id, state, status.message)
            msg = '==> Scheduler marked task as %s (will NOT be retried):\n\n%s' % (state, status.message)
            try:
                self._changes_api.jobstep_console_append(jobstep_id, text=msg)
            except APIError:
                pass
            try:
                self._changes_api.update_jobstep(jobstep_id, status="finished", result="infra_failed")
            except APIError:
                pass

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
          Invoked when an executor sends a message. These messages are best
          effort; do not expect a framework message to be retransmitted in any
          reliable fashion.
        """
        logging.info("Received message: %s", repr(str(message)))

    def slaveLost(self, driver, slaveId):
        """
          Invoked when a slave has been determined unreachable (e.g., machine
          failure, network partition.) Most frameworks will need to reschedule
          any tasks launched on this slave on a new slave.
        """
        logging.warn("Slave lost: %s", slaveId.value)

    def executorLost(self, driver, executorId, slaveId, status):
        """
          Invoked when an executor has exited/terminated. Note that any tasks
          running will have TASK_LOST status updates automatically generated.
        """
        logging.warn("Executor %s lost on slave %s", executorId.value, slaveId.value)

    def error(self, driver, message):
        """
          Invoked when there is an unrecoverable error in the scheduler or
          scheduler driver.  The driver will be aborted BEFORE invoking this
          callback.
        """
        logging.error("Error from Mesos: %s", message)
        self._stats.incr('errors')

    def save_state(self):
        """
          Save current state to a file so that a restart of the scheduler can
          restore the state.
        """
        state = {}
        state['framework_id'] = self.framework_id
        state['taskJobStepMapping'] = self.taskJobStepMapping
        state['tasksLaunched'] = self.tasksLaunched
        state['tasksFinished'] = self.tasksFinished
        logging.info('Attempting to save state for framework %s with %d running tasks to %s',
                     self.framework_id, len(self.taskJobStepMapping), self.state_file)
        with open(self.state_file, 'w') as f:
            f.write(json.dumps(state))

    def restore_state(self):
        """
          Restores state from the previous run of the scheduler.
        """
        with open(self.state_file) as f:
            json_state = f.read()
        state = json.loads(json_state)

        self.framework_id = state['framework_id']
        self.taskJobStepMapping = state['taskJobStepMapping']
        self.tasksLaunched = state['tasksLaunched']
        self.tasksFinished = state['tasksFinished']

        logging.info('Restored state for framework %s with %d running tasks from %s',
                     self.framework_id, len(self.taskJobStepMapping), self.state_file)
