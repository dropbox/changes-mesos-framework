from __future__ import absolute_import, print_function

import bisect
import concurrent.futures
import json
import logging
import os
import threading
import time
import urllib2 # type: ignore

from changes_mesos_scheduler import statsreporter

from typing import Any, Callable, Dict, NamedTuple, Optional, Set, Tuple

from collections import defaultdict
from threading import Event
from urllib import urlencode
from uuid import uuid4

from google.protobuf import text_format as _text_format # type: ignore

from mesos.interface import Scheduler, SchedulerDriver
from mesos.interface import mesos_pb2


class FileBlacklist(object):
    """ File-backed blacklist for slave hostnames.
    Hosts are expected to be named in the file, one per line.
    Whitespace and lines beginning with '#' are ignored.
    """
    def __init__(self, path):
        # type: (str) -> None
        self._path = path # type: str
        self._mtime = 0.0
        self._blacklist = set() # type: Set[str]

    def refresh(self):
        # type: () -> None
        """Refresh the blacklist if the file changed."""
        if os.path.getmtime(self._path) > self._mtime:
            self._refresh()

    def _refresh(self):
        # type: () -> None
        """Unconditionally refresh the blacklist from the file."""
        logging.info('Refreshing blacklist')
        self._mtime = os.path.getmtime(self._path)
        with open(self._path) as file:
            self._blacklist = set([s.strip() for s in file.readlines() if not s.startswith('#')])

    def contains(self, hostname):
        # type: (str) -> bool
        """Returns whether the provided hostname is present in the blacklist as of last reading."""
        return hostname in self._blacklist


class APIError(Exception):
    """An Exception originating from ChangesAPI.
    This mostly exists so that our uncertainty of the possible Exceptions
    originating from API requests doesn't muddy the error handling in the Scheduler.
    """
    def __init__(self, msg, cause=None):
        # type: (str, Any) -> None
        super(APIError, self).__init__(msg)
        self.cause = cause


class ChangesAPI(object):
    """Client for the Changes API, intended for Scheduler use.
    Any exceptions resulting from runtime failures should be APIErrors.
    """

    def __init__(self, api_url):
        # type: (str) -> None
        self._api_url = api_url

    @staticmethod
    def make_url(base_url, path, get_params=None):
        # type: (str, str, Optional[Dict[str,str]]) -> str
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
        # type: (str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]) -> Dict[str, Any]
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
        # type: (Optional[int], Optional[str]) -> List[Dict[str, Any]]
        """ Returns a list of up to `limit` pending allocation jobsteps in `cluster`.
            The scheduler may then allocate these as it sees fit.

        Args:
            limit: maximum jobsteps to return
            cluster: cluster to look in. The "default" cluster
                returns jobsteps with no cluster specified.

        Returns:
            list: List of JobSteps (in priority order) that are pending allocation
        """
        data = {'limit': limit} if limit else {} # type: Dict[str, Any]
        if cluster:
            data['cluster'] = cluster
        return self._api_request("/jobsteps/allocate/", get_params=data)['jobsteps']

    def post_allocate_jobsteps(self, jobstep_ids, cluster=None):
        # type: (List[str], Optional[str]) -> List[str]
        """ Attempt to allocate the given list of JobStep ids.

        Args:
            jobstep_ids: list of JobStep ID hexs to allocate.
            cluster: cluster to allocate in.

        Returns:
            list: list of jobstep ID hexs that were actually allocated.
        """
        data = {'jobstep_ids': jobstep_ids} # type: Dict[str, Any]
        if cluster:
            data['cluster'] = cluster
        return self._api_request("/jobsteps/allocate/", data)['allocated']

    def update_jobstep(self, jobstep_id, status, result=None, hostname=None):
        # type: (str, str, Optional[str], Optional[str]) -> None
        """ Update the recorded status and possibly result of a JobStep in Changes.

        Args:
            jobstep_id: JobStep ID.
            status: Status (one of "finished", "queued", "in_progress").
            result: Optionally one of 'failed', 'passed', 'aborted', 'skipped', or 'infra_failed'.
            hostname: Optional hostname of slave we are running this jobstep on
        """
        data = {"status": status}
        if result:
            data["result"] = result
        if hostname:
            data["node"] = hostname
        self._api_request("/jobsteps/{}/".format(jobstep_id), data)

    def jobstep_console_append(self, jobstep_id, text):
        # type: (str, str) -> None
        """ Append to the JobStep's console log.
        Args:
            jobstep_id: JobStep ID.
            text: Text to append.
        """
        url = '/jobsteps/%s/logappend/' % jobstep_id
        self._api_request(url, {'source': 'console', 'text': text})


class SlaveInfo(object):
    def __init__(self, hostname):
        # type: (str) -> None
        self.hostname = hostname

class ChangesScheduler(Scheduler):
    def __init__(self, state_file, api, blacklist, stats=None,
                 changes_request_limit=200):
        # type: (str, ChangesAPI, FileBlacklist, Optional[Any], int) -> None
        """
        Args:
            state_file (str): Path where serialized internal state will be
                stored.
            api (ChangesAPI): API to use for interacting with Changes.
            blacklist (FileBlacklist): Blacklist to use.
            stats (statsreporter.Stats): Optional Stats instance to use.
        """
        self.framework_id = None # type: Optional[str]
        self._changes_api = api
        self.taskJobStepMapping = {} # type: Dict[str, str]
        # maps from a slave_id to general info about that slave (currently only its hostname)
        self.slaveIdInfo = {} # type: Dict[str, SlaveInfo]
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
        self._snapshot_slave_map = defaultdict(lambda: defaultdict(float)) # type: Dict[str, Dict[str, float]]

        # Variables to help with polling Changes for pending jobsteps in a
        # separate thread. _cached_slaves_lock protects _cached_slaves.
        self._cached_slaves_lock = threading.Lock()
        self._cached_slaves = {} # type: Dict[str, ChangesScheduler.Slave]

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

    def poll_changes_until_shutdown(self, driver, interval):
        # type: (SchedulerDriver, int) -> None
        """In a separate thread, periodically poll Changes for jobsteps that
        need to be scheduled. This method will block, waiting indefinitely
        until shuttingDown() is set. Then the thread will terminate (finishing
        any current polling activity if necessary) and this method will return.
        Args:
            driver: the MesosSchedulerDriver object
            interval: number of seconds in each poll loop.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._polling_loop, driver, interval)
            logging.info("Started thread at %s. Now waiting...", time.ctime())
            while not future.done():
                time.sleep(.01)
            try:
                future.result()
            except Exception:
                logging.exception("Polling thread failed. Exiting.")
            self.decline_open_offers(driver)

    def _polling_loop(self, driver, interval):
        # type: (SchedulerDriver, int) -> None
        """Poll Changes for new jobsteps forever, until shuttingDown is set.
        Args:
            driver: the MesosSchedulerDriver object
            interval: number of seconds in each poll loop.
        """
        try:
            next_wait_duration = 0.0
            while not self.shuttingDown.wait(next_wait_duration):
                start_time = time.time()
                # Loop as long as Changes continues providing tasks to schedule.
                while self.poll_and_launch_once(driver):
                    pass

                # Schedule the delay for the next iteration of the loop,
                # attempting to compensate for scheduling skew caused by
                # polling/computation time.
                last_poll_duration = time.time() - start_time
                next_wait_duration = max(0, interval - last_poll_duration)
        finally:
            # In the event of an exception in the polling thread, shut
            # everything down clean(ish)ly.
            self.shuttingDown.set()

    def poll_and_launch_once(self, driver):
        # type: (SchedulerDriver) -> bool
        """Poll Changes once for all jobsteps matching all clusters for which
        we have offers. Then assign these jobsteps to offers. Then execute the
        assignments by launching tasks on Mesos and informing Changes about
        the assignments.
        This is also the entry point for most testing, since it skips the
        annoying threading and while-loop behavior that make synchronization
        difficult.
        Args:
            driver: the MesosSchedulerDriver object
        Returns:
            bool: True if there are more jobsteps to fetch from Changes, False
                otherwise.
        """
        # TODO: There's presently a window between post_allocate_jobsteps() and
        # launchTasks() where Changes thinks tasks are scheduled on Mesos, but
        # the tasks haven't actually been scheduled yet. If there's a shutdown
        # or failure in this window, it can be a long time before Changes will
        # figure it out and re-submit the tasks to the scheduler.
        #
        # Also note that until post_allocate_jobsteps() is called, Changes will
        # just keep returning the same set of jobsteps to
        # get_allocate_jobsteps(). Thus we call get- and post- in a 1:1
        # ratio, otherwise we could have an infinite poll loop on Changes.
        #
        # To that end, consider implementing something like the following:
        #  1) Query Changes for jobsteps
        #  2) Internally assign jobsteps to offers
        #  3) Store assignments in scheduler's state.pending_assignments
        #  4) Write the state file each time the state changes, rather than
        #     only on shutdown, such that we'd have everything in order in the
        #     event of a problem.
        #  5) post_allocate_jobsteps() the assignments
        #  6) Goto 1 until no more jobsteps
        #  7) Launch jobsteps on mesos
        #  8) Clear state.pending_assignments and write state file.
        #
        #  9) On startup, jobstep_deallocate any state.pending_assignments
        with self._cached_slaves_lock:
            # Get all slaves (composites of individual offers on the same host)
            all_slaves = self._cached_slaves.values()
            filtered_slaves = self._filter_slaves(all_slaves)
            logging.info("Do scheduling cycle with %d available slaves. (%d " +
                         "after filtering)",
                         len(all_slaves), len(filtered_slaves))
            slaves_by_cluster = self._slaves_by_cluster(filtered_slaves)

            # Get all jobsteps, organized by cluster.
            jobsteps_by_cluster = self._query_changes_for_jobsteps(
                    driver, slaves_by_cluster.keys())

            # For each cluster, assign jobsteps to slaves, then launch the
            # jobsteps on those slaves, using multiple offers if necessary.
            for cluster, jobsteps in jobsteps_by_cluster.iteritems():
                self._assign_jobsteps(cluster,
                                      slaves_by_cluster[cluster],
                                      jobsteps_by_cluster[cluster])
                self._launch_jobsteps(driver,
                                      cluster,
                                      slaves_by_cluster[cluster])

        # Guess whether or not there are more jobsteps waiting on Changes by
        # comparing the number of jobsteps received vs. the number of jobsteps
        # requested.
        return len(jobsteps_by_cluster) == self.changes_request_limit

    def decline_open_offers(self, driver):
        # type: (SchedulerDriver) -> None
        """Decline all cached Mesos pb_offers.
        """
        with self._cached_slaves_lock:
            slaves = self._cached_slaves.values()
            for slave in slaves:
                self._stat_and_log_list(slave.offers(), 'decline_for_shutdown',
                                        lambda offer: "Shutting down, declining offer: %s" % offer.offer.id)
                self._decline_list(driver, slave.offers())
            self._cached_slaves = {}

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
        # type: (SchedulerDriver) -> None
        """
          Invoked when the scheduler becomes disconnected from the master, e.g.
          the master fails and another is taking over.
          Abandon all open offers and slaves. We don't decline, since there's
          no master to report to. The new master should provide a new batch of
          offers soon enough.
        """
        logging.info("Disconnected from master. Abandoning all cached offer and slave info without declining.")
        with self._cached_slaves_lock:
            self._cached_slaves = {}

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
        """Precompute some commonly-used fields from a Mesos Offer proto.
        """
        def __init__(self, pb_offer):
            # type: (Any) -> None
            self.offer = pb_offer
            self.cluster = ChangesScheduler.get_cluster(pb_offer)

            resources = ChangesScheduler.get_resources(pb_offer)
            self.cpu = resources.get('cpus', 0.0)
            self.mem = resources.get('mem', 0)

        def __cmp__(self, other):
            # type: (ChangesScheduler.OfferWrapper) -> int
            """Comparator for sorting offers by "least loaded".
            """
            # we prioritize first by cpu then memory.
            # (values are negated so more resources sorts as "least loaded")
            us = (-self.cpu, -self.mem)
            them = (-other.cpu, -other.mem)
            if us < them:
                return -1
            return 0 if us == them else 1

        def __str__(self, pb_offer):
            cpu = "?"
            mem = "?"
            for r in pb_offer.resources:
                if r.name == 'cpus':
                    cpu = str(r.scalar).strip()
                if r.name == 'memory':
                    cpu = str(r.scalar).strip()
            return "Offer({} {} {} cpu: {}  mem: {})".format(
                    pb_offer.id.value, pb_offer.slave_id.value,
                    pb_offer.hostname, cpu, mem)

    class Slave(object):
        """ Wrapper around a protobuf Offer object. Provides numerous
        conveniences including comparison (we currently use a least loaded
        approach), and being able to assign jobsteps to the offer.
        """
        def __init__(self, slave_id, hostname, cluster):
            # type: (str, str, str) -> None
            self.slave_id = slave_id
            self.hostname = hostname
            self.cluster = cluster

            self._offers = {} # type: Dict[str, ChangesScheduler.OfferWrapper]
            self.jobsteps_assigned = []  # type: List[Dict[str, Any]]

            # Sum of all Offer resources for this slave.
            self.total_cpu = 0.0
            self.total_mem = 0

            # Sum of all resources for jobsteps assigned to this slave.
            self.allocated_cpu = 0.0
            self.allocated_mem = 0

        def offers(self):
            # type: () -> List[ChangesScheduler.OfferWrapper]
            """Returns a list of available offers on the slave.
            """
            return self._offers.values()

        def has_offers(self):
            # type: () -> bool
            """Returns True if the slave has any available offers, False
            otherwise.
            """
            return len(self._offers) > 0

        def is_maintenanced(self, now_nanos):
            # type: (int) -> bool
            """Determine if a Mesos offer indicates that a maintenance window is
            in progress for the slave. Treat the slave as maintenanced if ANY
            offer has an active maintenance window.
            Args:
                now_nanos: Timestamp of right now in nanoseconds, for comparing
                    to the offer's (optional) maintenance time window.
            Returns:
                True if the offer is in the maintenance window, False otherwise.
            """
            is_maintenanced = False
            for offer in self._offers.itervalues():
                if not offer.offer.HasField('unavailability'):
                    continue
                start_time = offer.offer.unavailability.start.nanoseconds

                # If "duration" is not present use a default value of anything
                # greater than Now, to represent an unbounded maintenance time.
                # Override this with an actual end time if the "duration" field
                # is present in the protobuf.
                end_time = now_nanos + 1
                if (offer.offer.unavailability.HasField('duration')):
                    end_time = start_time + offer.offer.unavailability.duration.nanoseconds

                is_maintenanced = now_nanos > start_time and now_nanos < end_time
                if is_maintenanced:
                    break
            return is_maintenanced

        def add_offer(self, offer):
            # type: (ChangesScheduler.OfferWrapper) -> None
            """Add an offer to this slave, and add its resources to the slave's
            total resources.
            """
            if (offer.offer.slave_id.value != self.slave_id or
                offer.offer.hostname != self.hostname or
                offer.cluster != self.cluster):
                logging.error("A mismatched offer got mixed in with the wrong " +
                              "slave. Skipping. (\n  Slave: %s\n  Offer: %s)",
                              self, offer)
                return

            self.total_cpu += offer.cpu
            self.total_mem += offer.mem
            logging.info("Slave %s: Add new offer +%f cpu,  +%d mem (-> %f %d)",
                         self.hostname, offer.cpu, offer.mem, self.total_cpu,
                         self.total_mem)
            self._offers[offer.offer.id.value] = offer

        def remove_offer(self, offer_id):
            # type: (Any) -> None
            """Remove an offer and its resources from this slave.
            Args:
                offer_id: mesos_pb2.OfferId
            """
            offer = self._offers.get(offer_id.value)
            if offer:
                del(self._offers[offer_id.value])
                self.total_cpu -= offer.cpu
                self.total_mem -= offer.mem

        def offers_to_launch(self):
            # type: () -> List[ChangesScheduler.OfferWrapper]
            """Based on the jobsteps previously assigned, select the offers on
            which to allocate the jobsteps.
            Also, remove from the Slave all offers which are about to be
            launched, and decrement total resources appropriately.
            Returns:
                A list of OfferWrappers representing the set of offers on which
                the tasks should be scheduled. All returned OfferWrappers
                should have the same slave ID and hostname.
            """
            current_offers = sorted(self._offers.values())

            offers_to_launch = []
            for offer in current_offers:
                # Decrement the "remaining" resources fields as we choose
                # offers to allocate to the jobsteps.
                if (self.allocated_cpu > 0 and offer.cpu > 0 or
                    self.allocated_mem > 0 and offer.mem > 0):
                    offers_to_launch.append(offer.offer.id)
                    self.allocated_cpu -= offer.cpu
                    self.allocated_mem -= offer.mem
                    self.remove_offer(offer.offer.id)
            return offers_to_launch

        def tasks_to_launch(self):
            # type: () -> Tuple[List[Any], List[str]]
            """Generate list of mesos_pb2.Task to launch, and a second list of
            jobstep IDs corresponding to each task.
            Also, reset/clear jobsteps_assigned on the Slave.
            Returns:
                (list of tasks, list of jobstep IDs)
            """
            tasks = []
            jobstep_ids = []
            for jobstep in self.jobsteps_assigned:
                tasks.append(self._jobstep_to_task(jobstep))
                jobstep_ids.append(jobstep['id'])

            self.unassign_jobsteps()
            return tasks, jobstep_ids

        def unassign_jobsteps(self):
            # type: () -> None
            """Clear all assigned jobsteps from the Slave and reset required
            resources.
            """
            self.jobsteps_assigned = []
            self.allocated_cpu = 0.0
            self.allocated_mem = 0

        def __cmp__(self, other):
            # type: (ChangesScheduler.Slave) -> int
            # we prioritize first by cpu then memory.
            # (values are negated so more resources sorts as "least loaded")
            us = (-(self.total_cpu - self.allocated_cpu),
                  -(self.total_mem - self.allocated_mem))
            them = (-(other.total_cpu - other.allocated_cpu),
                    -(other.total_mem - other.allocated_mem))
            if us < them:
                return -1
            return 0 if us == them else 1

        def __str__(self, slave):
            return "Slave({}: {} offers, {} acpu, {} amem)".format(
                    slave.hostname, len(slave.offers()), slave.total_cpu,
                    slave.total_mem)

        def has_resources_for(self, jobstep):
            # type: (Dict[str, Any]) -> bool
            """Returns true if the slave has sufficient available resources to
            execute a jobstep, false otherwise.
            Args:
                jobstep: The jobstep to execute.
            Returns:
                True if the slave can host the jobstep.
            """
            return ((self.total_cpu - self.allocated_cpu) >= jobstep['resources']['cpus'] and
                    (self.total_mem - self.allocated_mem) >= jobstep['resources']['mem'])

        def assign_jobstep(self, jobstep):
            # type: (Dict[str, Any]) -> None
            """Tentatively assign a jobstep to run on this slave. The actual
            launching occurs elsewhere.
            """
            assert self.has_resources_for(jobstep)
            self.allocated_cpu += jobstep['resources']['cpus']
            self.allocated_mem += jobstep['resources']['mem']
            self.jobsteps_assigned.append(jobstep)

        def _jobstep_to_task(self, jobstep):
            # type: (Dict[str, Any]) -> Any
            """ Given a jobstep and an offer to assign it to, returns the TaskInfo
            protobuf for the jobstep and updates scheduler state accordingly.
            Args:
                jobstep: The jobstep to convert to a task.
            Returns:
                mesos_pb2.Task
            """
            tid = uuid4().hex
            logging.info("Accepting offer on %s to start task %s", self.hostname, tid)

            task = mesos_pb2.TaskInfo()
            task.name = "{} {}".format(
                jobstep['project']['slug'],
                jobstep['id'],
            )
            task.task_id.value = str(tid)
            task.slave_id.value = self.slave_id

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

            return task

    def _get_slaves_for_snapshot(self, snapshot_id, recency_threshold_hours=12):
        # type: (str, int) -> List[str]
        """ Returns list of hostnames which have run tasks with a given
        snapshot_id recently.
        """
        latest_snapshot_use = time.time() - recency_threshold_hours * 3600
        return [k for k, v in self._snapshot_slave_map[snapshot_id].iteritems()
                if v >= latest_snapshot_use]

    def _associate_snapshot_with_slave(self, snapshot_id, slave):
        self._snapshot_slave_map[snapshot_id][slave] = time.time()

    @staticmethod
    def _jobstep_snapshot(jobstep):
        """ Given a jobstep, return its snapshot id if set, None otherwise.
        """
        if 'image' in jobstep and jobstep['image']:
            if 'snapshot' in jobstep['image'] and jobstep['image']['snapshot']:
                return jobstep['image']['snapshot']['id']

        return None

    def _fetch_jobsteps(self, cluster):
        # type: (str) -> List[Dict[str, Any]]
        """Query Changes for all allocatable jobsteps for the specified cluster.
        """
        try:
            with self._stats.timer('poll_changes'):
                possible_jobsteps = self._changes_api.get_allocate_jobsteps(limit=self.changes_request_limit,
                                                                            cluster=cluster)
        except APIError:
            logging.warning('/jobstep/allocate/ GET failed for cluster: %s', cluster, exc_info=True)
            possible_jobsteps = []
        return possible_jobsteps

    def _assign_jobsteps(self, cluster, slaves_for_cluster, jobsteps_for_cluster):
        # type: (str, List[ChangesScheduler.Slave], List[Dict[str, Any]]) -> None
        """Make assignments for jobsteps for a cluster to offers for a cluster.
        Assignments are stored in the OfferWrapper, to be launched later.
        Args:
            cluster: The cluster to make assignments for.
            slaves_for_cluster: A list of offers for the cluster.
            jobsteps_for_cluster: A list of jobsteps for the cluster.
        """
        # Changes returns JobSteps in priority order, so for each one
        # we attempt to put it on the machine with the least current load that
        # still has sufficient resources for it. This is not necessarily an
        # optimal algorithm--it might allocate fewer jobsteps than is possible,
        # and it currently prioritizes cpu over memory. We don't believe this
        # to be an issue currently, but it may be worth improving in the future
        if len(slaves_for_cluster) == 0 or len(jobsteps_for_cluster) == 0:
            return

        logging.info("Assign %s jobsteps on cluster %s", len(jobsteps_for_cluster), cluster)
        sorted_slaves = sorted(slaves_for_cluster)

        for jobstep in jobsteps_for_cluster:
            slave_to_use = None
            snapshot_id = self._jobstep_snapshot(jobstep)
            # Disable proximity check if not using a snapshot or scheduling in an explicit cluster.
            # Clusters are expected to pre-populate snapshots out of band and will not benefit
            # from proximity checks.
            if snapshot_id and not cluster:
                slaves_with_snapshot = self._get_slaves_for_snapshot(snapshot_id)
                logging.info('Found slaves with snapshot id %s: %s',
                             snapshot_id, slaves_with_snapshot)

                if len(slaves_with_snapshot) > 0:
                    for slave in sorted_slaves:
                        if (slave.hostname in slaves_with_snapshot and
                            slave.has_resources_for(jobstep)):
                            slave_to_use = slave
                            logging.info('Scheduling jobstep %s on slave %s which might have snapshot %s',
                                         jobstep, slave.hostname, snapshot_id)
                            break

            # If we couldn't find a slave which is likely to have the snapshot already,
            # this gives us the least-loaded slave that we could actually use for this jobstep
            if not slave_to_use:
                for slave in sorted_slaves:
                    if slave.has_resources_for(jobstep):
                        slave_to_use = slave
                        break

            # couldn't find any slaves that would support this jobstep, move on
            if not slave_to_use:
                logging.warning("No slave found to run jobstep %s.", jobstep)
                continue

            sorted_slaves.remove(slave_to_use)
            if snapshot_id:
                self._associate_snapshot_with_slave(snapshot_id, slave_to_use.hostname)

            slave_to_use.assign_jobstep(jobstep)
            bisect.insort(sorted_slaves, slave_to_use)

    def _stat_and_log_list(self, to_decline, stats_counter_name, reason_func):
        # type: (List[Any], str, Callable[[Any], str]) -> None
        """Inform the Mesos master that we're declining a list of offers.
        Args:
            to_decline: The list of offers to decline
            stats_counter_name: A counter name to increment, to track stats for
                different decline reasons.
            reason_func (function(Mesos Offer protobuf)): A function to generate
                a logging string, to explain why this offer was declined.
        """
        self._stats.incr(stats_counter_name, len(to_decline))
        for offer in to_decline:
            if reason_func:
                logging.info(reason_func(offer))

    def _decline_list(self, driver, to_decline):
        # type: (SchedulerDriver, List[Any]) -> None
        """Inform the Mesos master that we're declining a list of offers.
        Args:
            driver: the MesosSchedulerDriver object
            to_decline: The list of offers to decline
        """
        for offer in to_decline:
            driver.declineOffer(offer.offer.id)

    def _filter_slaves(self, slaves):
        # type: (List[Any]) -> List[Any]
        """Given a list of offer protos, decline blacklisted or unusable
        offers. Return a list of usable offers.
        Args:
            pb_offers (list of Mesos Offer protobufs): A list of offers, some
                of which are usable and some of which might not be usable.
        Returns:
            list of usable Mesos Offer protobufs
        """
        self._blacklist.refresh()
        now_nanos = int(time.time() * 1000000000)
        maintenanced, blacklisted, usable = [], [], []
        for slave in slaves:
            if slave.is_maintenanced(now_nanos):
                maintenanced.append(slave)
            elif self._blacklist.contains(slave.hostname):
                blacklisted.append(slave)
            else:
                usable.append(slave)

        self._stat_and_log_list(maintenanced, 'ignore_for_maintenance',
                                lambda slave: "Ignoring slave from maintenanced hostname: %s" % slave.hostname)
        self._stat_and_log_list(blacklisted, 'ignore_for_blacklist',
                                lambda slave: "Ignoring slave from blacklisted hostname: %s" % slave.hostname)
        return usable

    def _launch_jobsteps(self, driver, cluster, slaves_for_cluster):
        # type: (SchedulerDriver, str, List[ChangesScheduler.Slave]) -> None
        """Given a list of offers, launch all jobsteps assigned on each offer.
        Remove from the Offers cache any used offers.
        Args:
            driver: the MesosSchedulerDriver object
            slaves_for_cluster: A list of offers with assigned jobsteps already
                embedded. Launch the jobsteps on the offer.
        """
        if len(slaves_for_cluster) == 0:
            return

        # Inform Changes of where the jobsteps are going.
        jobsteps_to_allocate = []
        for slave in slaves_for_cluster:
            jobstep_ids = [jobstep['id'] for jobstep in slave.jobsteps_assigned]
            jobsteps_to_allocate.extend(jobstep_ids)

        if len(jobsteps_to_allocate) == 0:
            return

        try:
            jobsteps_to_allocate.sort()  # Make testing deterministic.
            allocated_jobstep_ids = self._changes_api.post_allocate_jobsteps(
                    jobsteps_to_allocate, cluster=cluster)
        except APIError:
            allocated_jobstep_ids = []
        if sorted(allocated_jobstep_ids) != sorted(jobsteps_to_allocate):
            # NB: cluster could be None here
            logging.warning("Could not successfully allocate for cluster: %s", cluster)
            # for now we just give up on this cluster entirely
            for slave in slaves_for_cluster:
                slave.unassign_jobsteps()

        # we've allocated all the jobsteps we can, now we launch them
        for slave in slaves_for_cluster:
            if len(slave.jobsteps_assigned) == 0:
                continue
            filters = mesos_pb2.Filters()
            filters.refuse_seconds = 1.0

            # Note: offers_to_launch() and tasks_to_launch() remove offers and
            # tasks from the slave.
            offers_to_launch = slave.offers_to_launch()
            tasks_to_launch, jobstep_ids = slave.tasks_to_launch()

            for task, jobstep_id in zip(tasks_to_launch, jobstep_ids):
                self.taskJobStepMapping[task.task_id.value] = jobstep_id

            self.tasksLaunched += len(tasks_to_launch)
            logging.info("Launch tasks: {} offers,  {} tasks".format(len(offers_to_launch), len(tasks_to_launch)))
            driver.launchTasks(offers_to_launch, tasks_to_launch, filters)

    def resourceOffers(self, driver, pb_offers):
        # type: (SchedulerDriver, List[Any]) -> None
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

        # Simply add the offers to our local cache of available offers.
        # Jobsteps are allocated asynchronously, driven by
        # poll_changes_until_shutdown().
        with self._cached_slaves_lock:
            for pb_offer in pb_offers:
                offer = ChangesScheduler.OfferWrapper(pb_offer)
                if pb_offer.slave_id.value not in self._cached_slaves:
                    slave = ChangesScheduler.Slave(pb_offer.slave_id.value,
                                                   pb_offer.hostname,
                                                   offer.cluster)
                    self._cached_slaves[pb_offer.slave_id.value] = slave
                self._cached_slaves[pb_offer.slave_id.value].add_offer(offer)
                self.slaveIdInfo[pb_offer.slave_id.value] = SlaveInfo(hostname=pb_offer.hostname)

    def _slaves_by_cluster(self, slaves):
        slaves_by_cluster = defaultdict(list)
        for slave in slaves:
            if slave.has_offers():
                slaves_by_cluster[slave.cluster].append(slave)
        return slaves_by_cluster

    def _query_changes_for_jobsteps(self, driver, clusters):
        # type: (SchedulerDriver, List[str]) -> Dict[str, List[Dict[str, Any]]]
        """Query Changes for the pending jobsteps for each cluster for which we
        have offers available.
        """
        jobsteps_by_cluster = defaultdict(list)  # type: Dict[str, List[Dict[str, Any]]]
        for cluster in clusters:
            jobsteps = self._fetch_jobsteps(cluster)
            jobsteps_by_cluster[cluster] = jobsteps
        return jobsteps_by_cluster

    def offerRescinded(self, driver, offerId):
        # type: (SchedulerDriver, Any) -> None
        """
          Invoked when an offer is no longer valid (e.g., the slave was lost or
          another framework used resources in the offer.) If for whatever reason
          an offer is never rescinded (e.g., dropped message, failing over
          framework, etc.), a framwork that attempts to launch tasks using an
          invalid offer will receive TASK_LOST status updats for those tasks
          (see Scheduler.resourceOffers).
          Args:
            driver: the MesosSchedulerDriver object
            offerId: a Mesos OfferId protobuf
        """
        logging.info("Offer rescinded: %s", offerId.value)
        with self._cached_slaves_lock:
            for slave in self._cached_slaves.itervalues():
                slave.remove_offer(offerId)

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

        hostname = None
        if self.slaveIdInfo.get(status.slave_id.value):
            hostname = self.slaveIdInfo[status.slave_id.value].hostname
        if hostname is None:
            logging.warning('No hostname associated with task: %s (slave_id %s)', status.task_id.value, status.slave_id.value)

        if jobstep_id is None:
            # TODO(nate): how does this happen?
            logging.error("Task %s missing JobStep ID (state %s, message %s)",
                          status.task_id.value, state,
                          _text_format.MessageToString(status))
            self._stats.incr('missing_jobstep_id_' + state)
            return

        if state == 'finished':
            try:
                self._changes_api.update_jobstep(jobstep_id, status="finished", hostname=hostname)
            except APIError:
                pass
        elif state in ('killed', 'lost', 'failed'):
            self._stats.incr('task_' + state)
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
                self._changes_api.update_jobstep(jobstep_id, status="finished", result="infra_failed", hostname=hostname)
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
        self._stats.incr('slave_lost')
        with self._cached_slaves_lock:
            slave = self._cached_slaves.pop(slaveId.value, None)
            if slave:
                self._stat_and_log_list(slave.offers(), 'decline_for_slave_lost',
                                        lambda offer: "Slave lost, declining offer: %s" % offer.offer.id)
                self._decline_list(driver, slave.offers())

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
        state['slaveIdInfo'] = {}
        for slave, info in self.slaveIdInfo.iteritems():
            state['slaveIdInfo'][slave] = {'hostname': info.hostname}
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
        self.slaveIdInfo = {}
        for slave, info in state.get('slaveIdInfo', {}).iteritems():
            self.slaveIdInfo[slave] = SlaveInfo(hostname=info.get('hostname'))
        self.tasksLaunched = state['tasksLaunched']
        self.tasksFinished = state['tasksFinished']
        snapshot_slave_map = state['snapshot_slave_map']
        self._snapshot_slave_map = defaultdict(lambda: defaultdict(float))
        for snapshot, slave_map in snapshot_slave_map.iteritems():
            for slave, timestamp in slave_map.iteritems():
                self._snapshot_slave_map[snapshot][slave] = timestamp

        logging.info('Restored state for framework %s with %d running tasks from %s',
                     self.framework_id, len(self.taskJobStepMapping), self.state_file)

    def state_json(self):
        # type: () -> str
        """Produce a JSON dump of the scheduler's internal state.
        Returns:
            A JSON-encoded dict representing the scheduler's state.
        """
        def convert_attrs(attrs):
            # type: (List[Any]) -> List[Dict[str, Any]]
            """Convert Attribute and Resource protobuf fields to dictionaries.
            Args:
                attrs: List of mesos_pb2.Attribute or mesos_pb2.Resource
            Returns:
                {'name': str, 'type': int, 'value': any simple Python type}
            """
            accum = []
            for attr in attrs:
                if attr.type == mesos_pb2.Value.SCALAR:
                    value = attr.scalar.value
                elif attr.type == mesos_pb2.Value.RANGES:
                    value = ', '.join(map(lambda x: '(%d, %d)' % (x.begin, x.end), attr.ranges.range))
                elif attr.type == mesos_pb2.Value.SET:
                    value = ', '.join(attr.set.item)
                elif attr.type == mesos_pb2.Value.TEXT:
                    value = attr.text.value
                else:
                    value = 'Unknown Mesos value type {} on slave {} offer {}'.format(
                            attr.type, slave.hostname, offer.offer.id.value)

                attr_output = {
                    'name': attr.name,
                    'type': attr.type,
                    'value': value,
                }
                accum.append(attr_output)
            return accum

        start_time = time.time()
        with self._cached_slaves_lock:
            # Build JSON output for the blacklist.
            blacklist_output = {
                'path': self._blacklist._path,
                'entries': sorted(list(self._blacklist._blacklist)),
            }

            # Build JSON output for all slaves.
            slaves = self._cached_slaves.values()
            slaves.sort(key=lambda x: x.hostname)
            slaves_output = []
            for slave in slaves:
                # Build JSON output for all offers on the slave.
                offers = slave._offers.values()
                offers.sort(key=lambda x: x.offer.id.value)
                offers_output = []
                for offer in offers:
                    if offer.offer.url.address.hostname:
                        base = offer.offer.url.address.hostname
                    else:
                        base = offer.offer.url.address.ip
                    url = (offer.offer.url.scheme +
                           base +
                           offer.offer.url.path +
                           '&'.join(offer.offer.url.query) +
                           offer.offer.url.fragment)

                    offer_output = {
                        'offer_id': offer.offer.id.value,
                        'framework_id': offer.offer.framework_id.value,
                        'url': url,
                        'cpu': offer.cpu,
                        'mem': offer.mem,
                        'attributes': convert_attrs(offer.offer.attributes),
                        'resources': convert_attrs(offer.offer.resources),
                    }
                    json.dumps(offer_output)
                    offers_output.append(offer_output)
                slave_output = {
                        'slave_id': slave.slave_id,
                        'hostname': slave.hostname,
                        'cluster': slave.cluster,
                        'offers': offers_output,
                        'total_cpu': slave.total_cpu,
                        'total_mem': slave.total_mem,
                        'is_maintenanced': slave.is_maintenanced(
                            int(start_time * 1000000000)),
                }
                json.dumps(slave_output)
                slaves_output.append(slave_output)

        # Put it all together.
        state = {
            'framework_id': self.framework_id,
            'taskJobStepMapping': self.taskJobStepMapping,
            'tasksLaunched': self.tasksLaunched,
            'tasksFinished': self.tasksFinished,
            'shuttingDown': self.shuttingDown.is_set(),
            'blacklist': blacklist_output,
            'snapshot_slave_map': self._snapshot_slave_map,
            'changes_request_limit': self.changes_request_limit,
            'cached_slaves': slaves_output,
            'build_state_json_secs': time.time() - start_time,
        }

        return json.dumps(state)
