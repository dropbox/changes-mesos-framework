from __future__ import absolute_import, print_function

import bisect
import json
import logging
import os
import time
import urllib2

from changes_mesos_scheduler import statsreporter

from collections import defaultdict
from threading import Event
from urllib import urlencode
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

    @staticmethod
    def make_url(base_url, path, get_params=None):
        # Changes insists that paths end with a slash
        path = path if path.endswith('/') else path + '/'
        # Make sure there's exactly one slash between path and the API url
        path = path if path.startswith('/') else '/' + path
        base_url = base_url.rstrip('/')
        full_url = base_url + path
        if get_params:
            query_string = '?' + urlencode(get_params)
            full_url += query_string
        return full_url

    def _api_request(self, path, body=None, get_params=None):
        full_url = ChangesAPI.make_url(self._api_url, path, get_params)
        try:
            data = json.dumps(body) if body else None
            req = urllib2.Request(
                full_url, data,
                {'Content-Type': 'application/json'})
            # Any connectivity issues will raise an exception, as will some error statuses.
            content = urllib2.urlopen(req).read()
            return json.loads(content)
        except Exception as exc:
            # Always log exceptions so callers don't have to.
            logging.exception("Error POSTing to Changes at %s", full_url)
            raise APIError("Error POSTing to Changes at %s" % full_url, exc)

    def get_allocate_jobsteps(self, limit=None, cluster=None):
        """ Returns a list of up to `limit` pending allocation jobsteps in `cluster`.
            The scheduler may then allocate these as it sees fit.

        Args:
            limit (Optional[int]): maximum jobsteps to return
            cluster (Optional[str]): cluster to look in. The "default" cluster
                returns jobsteps with no cluster specified.

        Returns:
            list: List of JobSteps (in priority order) that are pending allocation
        """
        data = {'limit': limit} if limit else {}
        if cluster:
            data['cluster'] = cluster
        return self._api_request("/jobsteps/allocate/", get_params=data)['jobsteps']

    def post_allocate_jobsteps(self, jobstep_ids, cluster=None):
        """ Attempt to allocate the given list of JobStep ids.

        Args:
            jobstep_ids (list): list of JobStep ID hexs to allocate.
            cluster (Optional[str]): cluster to allocate in.
        """
        data = {'jobstep_ids': jobstep_ids}
        if cluster:
            data['cluster'] = cluster
        return self._api_request("/jobsteps/allocate/", data)['allocated']

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
    def __init__(self, state_file, api, blacklist, stats=None, changes_request_limit=200):
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
        self.changes_request_limit = changes_request_limit
        self._snapshot_slave_map = defaultdict(lambda: defaultdict(float))

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

    @staticmethod
    def get_cluster(offer):
        attributes = dict([ChangesScheduler._decode_attribute(a) for a in offer.attributes])
        return attributes.get('labels')

    @staticmethod
    def get_resources(offer):
        return {name: value for (name, value) in
                [ChangesScheduler._decode_resource(r) for r in offer.resources]}

    class OfferWrapper(object):
        """ Wrapper around a protobuf Offer object. Provides numerous
        conveniences including comparison (we currently use a least loaded
        approach), and being able to assign jobsteps to the offer.
        """
        def __init__(self, offer):
            self.offer = offer
            self.hostname = offer.hostname
            self.reset_state()

        def reset_state(self):
            resources = ChangesScheduler.get_resources(self.offer)
            self.cpus = resources.get('cpus', 0)
            self.memory = resources.get('mem', 0)
            self.jobsteps = []

        def __cmp__(self, other):
            # we prioritize first by cpu then memory.
            # (values are negated so more resources sorts as "least loaded")
            us = (-self.cpus, -self.memory)
            them = (-other.cpus, -other.memory)
            if us < them:
                return -1
            return 0 if us == them else 1

        def cluster(self):
            return ChangesScheduler.get_cluster(self.offer)

        def has_resources_for(self, jobstep):
            return self.cpus >= jobstep['resources']['cpus'] and self.memory >= jobstep['resources']['mem']

        def commit_resources_for(self, jobstep):
            assert self.has_resources_for(jobstep)
            self.cpus -= jobstep['resources']['cpus']
            self.memory -= jobstep['resources']['mem']

        def has_resources(self):
            return self.cpus > 0 and self.memory > 0

        def add_jobstep(self, jobstep):
            self.commit_resources_for(jobstep)
            self.jobsteps.append(jobstep)

        def remove_all_jobsteps(self):
            self.reset_state()

    def _get_slaves_for_snapshot(self, snapshot_id, recency_threshold_hours=12):
        """ Returns list of hostnames which have run tasks with a given
        snapshot_id recently.
        """
        latest_snapshot_use = time.time() - recency_threshold_hours * 3600
        return [k for k, v in self._snapshot_slave_map[snapshot_id].iteritems()
                if v >= latest_snapshot_use]

    def _associate_snapshot_with_slave(self, snapshot_id, slave):
        self._snapshot_slave_map[snapshot_id][slave] = time.time()

    def _jobstep_to_task(self, offer, jobstep):
        """ Given a jobstep and an offer to assign it to, returns the TaskInfo
        protobuf for the jobstep and updates scheduler state accordingly.
        """
        tid = uuid4().hex
        self.tasksLaunched += 1

        logging.info("Accepting offer on %s to start task %s", offer.hostname, tid)

        task = mesos_pb2.TaskInfo()
        task.name = "{} {}".format(
            jobstep['project']['slug'],
            jobstep['id'],
        )
        task.task_id.value = str(tid)
        task.slave_id.value = offer.slave_id.value

        cmd = jobstep["cmd"]

        task.command.value = cmd
        logging.debug("Scheduling cmd: %s", cmd)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = jobstep["resources"]["cpus"]

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = jobstep["resources"]["mem"]

        self.taskJobStepMapping[task.task_id.value] = jobstep['id']

        return task

    @staticmethod
    def _jobstep_snapshot(jobstep):
        """ Given a jobstep, return its snapshot id if set, None otherwise.
        """
        if 'image' in jobstep and jobstep['image']:
            if 'snapshot' in jobstep['image'] and jobstep['image']['snapshot']:
                return jobstep['image']['snapshot']['id']

        return None

    def _assign_jobsteps(self, cluster, offers):
        """ Attempts to schedule as many jobsteps as possible (using a least
        loaded approach) on the given set of offers.

         Args:
            cluster (string): The cluster of the offers in question (could be
                the None cluster)
            offers (list[OfferWrapper]): list of currently available offers
        """
        # TODO(nate): may want to do this POST in a loop (til there are no
        # jobsteps or we've filled our offers), so there's no 5s delay in scheduling many jobsteps
        try:
            possible_jobsteps = self._changes_api.get_allocate_jobsteps(limit=self.changes_request_limit, cluster=cluster)
        except APIError:
            logging.warning('/jobstep/allocate/ GET failed for cluster: %s', cluster, exc_info=True)
            possible_jobsteps = []
        if not possible_jobsteps:
            return

        to_allocate = []
        # Changes returns JobSteps in priority order, so for each one
        # we attempt to put it on the machine with the least current load that
        # still has sufficient resources for it. This is not necessarily an
        # optimal algorithm--it might allocate fewer jobsteps than is possible,
        # and it currently prioritizes cpu over memory. We don't believe this
        # to be an issue currently, but it may be worth improving in the future
        sorted_offers = sorted(offers)
        for jobstep in possible_jobsteps:
            if len(sorted_offers) == 0:
                break
            offer_to_use = None

            snapshot_id = self._jobstep_snapshot(jobstep)
            # Disable proximity check if not using a snapshot or scheduling in an explicit cluster.
            # Clusters are expected to pre-populate snapshots out of band and will not benefit
            # from proximity checks.
            if snapshot_id and not cluster:
                logging.info('Scanning for slaves containing snapshot: %s', snapshot_id)

                slaves_with_snapshot = self._get_slaves_for_snapshot(snapshot_id)
                logging.info('Checking if any slaves known to have snapshot were offered: %s',
                             slaves_with_snapshot)

                if len(slaves_with_snapshot) > 0:
                    for offer in sorted_offers:
                        if offer.hostname in slaves_with_snapshot:
                            if offer.has_resources_for(jobstep):
                                offer_to_use = offer
                                logging.info('Scheduling jobstep %s on slave %s which might have snapshot %s',
                                             jobstep, offer.hostname, snapshot_id)
                                break

            # If we couldn't find a slave which is likely to have the snapshot already,
            # this gives us the least-loaded offer that we could actually use for this jobstep
            if not offer_to_use:
                for offer in sorted_offers:
                    if offer.has_resources_for(jobstep):
                        offer_to_use = offer
                        break

            # couldn't find any offers that would support this jobstep, move on
            if not offer_to_use:
                continue

            sorted_offers.remove(offer_to_use)
            if snapshot_id:
                self._associate_snapshot_with_slave(snapshot_id, offer_to_use.hostname)

            offer_to_use.add_jobstep(jobstep)
            to_allocate.append(jobstep['id'])
            if offer_to_use.has_resources():
                bisect.insort(sorted_offers, offer_to_use)

        if not to_allocate:
            return

        try:
            allocated_ids = self._changes_api.post_allocate_jobsteps(to_allocate, cluster=cluster)
        except APIError:
            allocated_ids = []
        if sorted(allocated_ids) != sorted(to_allocate):
            # NB: cluster could be None here
            logging.warning("Could not successfully allocate for cluster: %s", cluster)
            # for now we just give up on this cluster entirely
            for offer in offers:
                offer.remove_all_jobsteps()

    @staticmethod
    def _is_maintenanced(pb_offer, now_nanos):
        if not pb_offer.HasField('unavailability'):
            return False

        start_time = pb_offer.unavailability.start.nanoseconds

        # If "duration" is not present use a default value of anything greater
        # than Now, to represent an unbounded maintenance time. Override this
        # with an actual end time if the "duration" field is present in the
        # protobuf.
        end_time = now_nanos + 1
        if (pb_offer.unavailability.HasField('duration')):
            end_time = start_time + pb_offer.unavailability.duration.nanoseconds

        return now_nanos > start_time and now_nanos < end_time

    def resourceOffers(self, driver, pb_offers):
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
        logging.info("Got %d resource offers", len(pb_offers))
        self._stats.incr('offers', len(pb_offers))

        self._blacklist.refresh()

        def decline(to_decline, reason_func):
            for pb_offer in to_decline:
                if reason_func:
                    logging.info(reason_func(pb_offer))
                driver.declineOffer(pb_offer.id)

        if self.shuttingDown.is_set():
            decline(pb_offers, lambda pb_offer: "Shutting down, declining offer: %s" % pb_offer.id)
            return

        now_nanos = int(time.time() * 1000000000)
        maintenanced, blacklisted, usable = [], [], []
        for pb_offer in pb_offers:
            if ChangesScheduler._is_maintenanced(pb_offer, now_nanos):
                maintenanced.append(pb_offer)
            elif self._blacklist.contains(pb_offer.hostname):
                blacklisted.append(pb_offer)
            else:
                usable.append(pb_offer)

        decline(maintenanced, lambda pb_offer: "Declining offer from maintenanced hostname: %s" % pb_offer.hostname)
        decline(blacklisted, lambda pb_offer: "Declining offer from blacklisted hostname: %s" % pb_offer.hostname)

        offers = [ChangesScheduler.OfferWrapper(pb_offer) for pb_offer in usable]
        offers_for_cluster = defaultdict(list)
        for offer in offers:
            offers_for_cluster[offer.cluster()].append(offer)

        for cluster, cluster_offers in offers_for_cluster.iteritems():
            self._assign_jobsteps(cluster, cluster_offers)

        # we've allocated all the jobsteps we can, now we launch them
        for offer in offers:
            if len(offer.jobsteps) == 0:
                logging.info("No tasks to run, declining offer: %s", offer.offer.id)
                driver.declineOffer(offer.offer.id)
                continue

            tasks = [self._jobstep_to_task(offer.offer, jobstep) for jobstep in offer.jobsteps]
            driver.launchTasks(offer.offer.id, tasks)

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
        state['snapshot_slave_map'] = self._snapshot_slave_map
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
        snapshot_slave_map = state['snapshot_slave_map']
        self._snapshot_slave_map = defaultdict(lambda: defaultdict(float))
        for snapshot, slave_map in snapshot_slave_map.iteritems():
            for slave, timestamp in slave_map.iteritems():
                self._snapshot_slave_map[snapshot][slave] = timestamp

        logging.info('Restored state for framework %s with %d running tasks from %s',
                     self.framework_id, len(self.taskJobStepMapping), self.state_file)
