import logging
import os
import shutil
import tempfile
import threading
import time

from collections import defaultdict

import mock
from typing import Any
from unittest import TestCase

# Capture debug logging output on test failure
logger = logging.getLogger()
logger.level = logging.DEBUG

try:
    from mesos.interface import mesos_pb2
    from mesos.interface import Scheduler
except ImportError:
    import mesos_pb2
    from mesos import Scheduler

from changes_mesos_scheduler.changes_scheduler import ChangesScheduler, APIError, FileBlacklist, ChangesAPI
from changes_mesos_scheduler import statsreporter

def _noop_blacklist():
    """Returns a blacklist instance that behaves like an empty blacklist."""
    m = mock.Mock(spec=FileBlacklist)
    m.contains.return_value = False
    return m


def _sync_resource_offers(cs, driver, new_offers):
    # type: (ChangesScheduler, Scheduler, List[Any]) -> None
    """Receive offers from the Mesos master and poll Changes for new jobsteps
    in a synchronous manner to facilitate simpler, more straightforward
    testing. Normally these two tasks run in separate threads.
    Args:
        driver: the MesosSchedulerDriver object
        new_offers: A list of Mesos Offer protobufs that should be offered to
            the scheduler.
    """
    cs.shuttingDown.clear()
    poll_notifier = threading.Condition()
    def poll_loop_done_cb():
        poll_notifier.acquire()
        poll_notifier.notifyAll()
        poll_notifier.wait()

    cs.resourceOffers(driver, new_offers)
    poll_notifier.acquire()

    # In practice, this should poll-loop just one time for tests.
    cs.poll_changes_until_shutdown(driver, 5, poll_loop_done_cb)

    # Wait for the poll loop to execute one time, then shut down.
    # We don't call wait_for_shutdown() here in order to distinguish between
    # ignored and declined offers. The _sync_resource_offers() caller should
    # call wait_for_shutdown() manually when ignored and declined checks
    # are complete.
    poll_notifier.wait()
    cs.shuttingDown.set()

    # Release the wait() in the polling loop. Since shuttingDown is now set,
    # the thread should finish immediately (more or less). wait_for_shutdown()
    # calls join() to cleanly finalize thread termination.
    poll_notifier.notifyAll()
    poll_notifier.release()


class ChangesAPITest(TestCase):
    url = 'https://changes.com/api/0'

    def test_make_url_paths(self):
        desired = 'https://changes.com/api/0/jobsteps/allocate/'
        assert ChangesAPI.make_url(self.url, '/jobsteps/allocate/') == desired
        assert ChangesAPI.make_url(self.url, 'jobsteps/allocate') == desired
        assert ChangesAPI.make_url(self.url + '/', 'jobsteps/allocate') == desired
        assert ChangesAPI.make_url(self.url + '/', '/jobsteps/allocate') == desired
        assert ChangesAPI.make_url(self.url + '//', '/jobsteps/allocate') == desired

    def test_make_url_query(self):
        desired = ['https://changes.com/api/0/jobsteps/allocate/?foo=bar&baz=xyzz',
                   'https://changes.com/api/0/jobsteps/allocate/?baz=xyzz&foo=bar']
        full_url = ChangesAPI.make_url(self.url, '/jobsteps/allocate/', {'foo': 'bar', 'baz': 'xyzz'})
        assert full_url in desired


class ChangesSchedulerTest(TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        super(ChangesSchedulerTest, self).setUp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(ChangesSchedulerTest, self).tearDown()

    def _make_offer(self,
                    hostname='hostname',
                    cpus=4,
                    mem=8192,
                    cluster=None,
                    id='offerid',
                    unavailability_start_secs=None,
                    unavailability_duration_secs=None):
        offer = mesos_pb2.Offer(
            id=mesos_pb2.OfferID(value=id),
            framework_id=mesos_pb2.FrameworkID(value="frameworkid"),
            slave_id=mesos_pb2.SlaveID(value="slaveid"),
            hostname=hostname,
        )

        if unavailability_start_secs is not None:
            offer.unavailability.start.nanoseconds = int(unavailability_start_secs * 1000000000)
        if unavailability_duration_secs is not None:
            offer.unavailability.duration.nanoseconds = int(unavailability_duration_secs * 1000000000)

        offer.resources.add(name="cpus",
                            type=mesos_pb2.Value.SCALAR,
                            scalar=mesos_pb2.Value.Scalar(value=cpus))
        offer.resources.add(name="mem",
                            type=mesos_pb2.Value.SCALAR,
                            scalar=mesos_pb2.Value.Scalar(value=mem))
        if cluster:
            offer.attributes.add(name="labels",
                                 type=mesos_pb2.Value.TEXT,
                                 text=mesos_pb2.Value.Text(value=cluster))
        return offer

    def _make_changes_task(self, id, cpus=2, mem=4096, slug='foo', cmd='ls', snapshot=None):
        image = None
        if snapshot:
            image = {'snapshot': {'id': snapshot}}

        return {'project': {'slug': slug}, 'id': id,
                'cmd': cmd, 'resources': {'cpus': cpus, 'mem': mem},
                'image': image}

    def test_save_restore_state(self):
        state_file = self.test_dir + '/test.json'

        cs = ChangesScheduler(state_file, api=mock.Mock(),
                              blacklist=_noop_blacklist())
        cs.tasksLaunched = 5
        cs.tasksFinished = 3
        cs.taskJobStepMapping['task x'] = 'jobstep x'
        cs._snapshot_slave_map = defaultdict(lambda: defaultdict(float))
        cs._snapshot_slave_map['snapid']['host1'] = 1234567.0
        cs._snapshot_slave_map['snapid']['host2'] = 1234569.0
        cs.save_state()

        cs2 = ChangesScheduler(state_file, api=mock.Mock(),
                               blacklist=_noop_blacklist())
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': 'jobstep x'} == cs2.taskJobStepMapping
        assert not os.path.exists(state_file)
        assert {'snapid': {'host1': 1234567.0, 'host2': 1234569.0}} == cs2._snapshot_slave_map

    def test_blacklist(self):
        blpath = self.test_dir + '/blacklist'
        # Ensure we have an empty blacklist file.
        open(blpath, 'w+').close()

        api = mock.MagicMock()
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=api, stats=stats,
                              blacklist=FileBlacklist(blpath))
        offer = self._make_offer(hostname = 'some_hostname.com')

        blacklist = open(blpath, 'w+')
        blacklist.write('some_hostname.com\n')
        blacklist.close()

        driver = mock.Mock()
        # We have to fake the mtime despite the file legitimately having been modified
        # later because some filesystems (HFS+, for example) don't have enough precision
        # for this to pass reliably.
        with mock.patch('os.path.getmtime', return_value=time.time()+1) as getmtime:
            _sync_resource_offers(cs, driver, [offer])
            getmtime.assert_called_with(blpath)
        assert api.declineOffer.call_count == 0
        assert api.allocate_jobsteps.call_count == 0

        assert stats.incr.call_count == 3
        stats.incr.assert_any_call('ignore_for_blacklist', 1)
        stats.incr.assert_any_call('ignore_for_maintenance', 0)

        # Decline any unused offers. Expect the blacklisted offer.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_blacklist_maintenance(self):
        api = mock.Mock(spec=ChangesAPI)
        now = time.time()
        memlimit = 8192

        # Test no unavailability scheduled - ACCEPT
        offer1 = self._make_offer(hostname='hostname_1.com',
                                  id="offer_1",
                                  mem=memlimit)

        # Test unavailability scheduled right now - DECLINE
        offer2 = self._make_offer(hostname='hostname_2.com',
                                  id="offer_2",
                                  mem=memlimit,
                                  unavailability_start_secs=now,
                                  unavailability_duration_secs=10)

        # Test unavailability scheduled in a few seconds - ACCEPT
        offer3 = self._make_offer(hostname='hostname_3.com',
                                  id="offer_3",
                                  mem=memlimit,
                                  unavailability_start_secs=now + 5,
                                  unavailability_duration_secs=10)

        # Test unavailability scheduled in the past, ending in the past - ACCEPT
        offer4 = self._make_offer(hostname='hostname_5.com',
                                  id="offer_4",
                                  mem=memlimit,
                                  unavailability_start_secs=now - 20,
                                  unavailability_duration_secs=10)

        # Test unavailability in progress - DECLINE
        offer5 = self._make_offer(hostname='hostname_5.com',
                                  id="offer_5",
                                  mem=memlimit,
                                  unavailability_start_secs=now - 5,
                                  unavailability_duration_secs=10)

        # Test past unavailability with no duration - DECLINE
        offer6 = self._make_offer(hostname='hostname_6.com',
                                  id="offer_6",
                                  mem=memlimit,
                                  unavailability_start_secs=now - 5,
                                  unavailability_duration_secs=None)

        # Test future unavailability with no duration - ACCEPT
        offer7 = self._make_offer(hostname='hostname_7.com',
                                  id="offer_7",
                                  mem=memlimit,
                                  unavailability_start_secs=now + 5,
                                  unavailability_duration_secs=None)

        # Test unavailability with zero duration - ACCEPT
        offer8 = self._make_offer(hostname='hostname_8.com',
                                  id="offer_8",
                                  mem=memlimit,
                                  unavailability_start_secs=now - 5,
                                  unavailability_duration_secs=0)

        all_offers = [offer1, offer2, offer3, offer4, offer5, offer6, offer7, offer8]
        expected_launches = [offer1, offer3, offer4, offer7, offer8]
        expected_ignores = [offer2, offer5, offer6]

        # To ensure that offers aren't accidentally declined due to a shortage
        # of tasks, ensure tasks > offers so there's at least one tasks per
        # machine, plus an extra. (each offer has memory for one task)
        num_tasks = len(all_offers) + 1
        tasks = []
        for i in xrange(num_tasks):
            tasks.append(self._make_changes_task(str(i), mem=memlimit))
        api.get_allocate_jobsteps.return_value = tasks

        # In practice, we end up with two tasks allocated per offer.
        post_allocate_jobsteps_return = []
        for i in xrange(len(expected_launches)):
            post_allocate_jobsteps_return.append(str(i))
        api.post_allocate_jobsteps.return_value = post_allocate_jobsteps_return

        # Actually run the test logic.
        stats = mock.MagicMock(spec=statsreporter.Stats)
        cs = ChangesScheduler(state_file=None, api=api, stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()
        _sync_resource_offers(cs, driver, all_offers)

        # Check that the maintenanced offers are declined.
        assert driver.declineOffer.call_count == 0

        # Check the stats reporting.
        assert stats.incr.call_count == 3
        stats.incr.assert_any_call('offers', len(all_offers))
        stats.incr.assert_any_call('ignore_for_blacklist', 0)
        stats.incr.assert_any_call('ignore_for_maintenance', len(expected_ignores))

        # Check that the non-maintenanced tasks are launched.
        assert driver.launchTasks.call_count == len(expected_launches)
        actual_launch_set = set()
        expected_launch_set = set()
        for launch_offer, args in zip(expected_launches,
                                      driver.launchTasks.call_args_list):
            expected_launch_set.add(launch_offer.id.value)
            actual_launch_set.add(args[0][0].value)
        assert actual_launch_set == expected_launch_set

        # Decline any unused offers. Expect all maintenanced offers.
        cs.wait_for_shutdown(driver)
        for offer in expected_ignores:
            driver.declineOffer.assert_any_call(offer.id)
        assert driver.declineOffer.call_count == len(expected_ignores)

    def test_error_stats(self):
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=mock.Mock(), stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()
        cs.error(driver, 'message')
        stats.incr.assert_called_once_with('errors')

    def test_slaveLost_stats(self):
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=mock.Mock(), stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()
        cs.slaveLost(driver, mesos_pb2.SlaveID(value="slaveid"))
        stats.incr.assert_called_once_with('slave_lost')

    def test_api_error(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.side_effect = APIError("Failure")
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer()

        _sync_resource_offers(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster=None)
        assert driver.declineOffer.call_count == 0

        # Decline any unused offers. Expect the errored offer.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_api_no_tasks(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = []
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster")

        _sync_resource_offers(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        assert driver.declineOffer.call_count == 0

        # Decline any unused offers. Expect the only existing offer.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_api_one_task(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1')]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(hostname='aHostname', cluster="foo_cluster")

        def check_tasks(offer_id, tasks, filters):
            assert offer_id == offer.id
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer.slave_id.value
            assert tasks[0].labels.labels[0].key == 'hostname'
            assert tasks[0].labels.labels[0].value == 'aHostname'
            assert tasks[0].command.value == 'ls'
            assert tasks[0].resources[0].name == "cpus"
            assert tasks[0].resources[0].scalar.value == 2
            assert tasks[0].resources[1].name == "mem"
            assert tasks[0].resources[1].scalar.value == 4096
            assert filters.refuse_seconds == 1.0
        driver.launchTasks.side_effect = check_tasks

        _sync_resource_offers(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        api.post_allocate_jobsteps.assert_called_once_with(['1'], cluster="foo_cluster")
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

        # Decline any unused offers (should be none)
        cs.wait_for_shutdown(driver)
        assert driver.declineOffer.call_count == 0

    def test_not_enough_resources(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1', cpus=8)]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster", cpus=4)

        _sync_resource_offers(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        assert api.post_allocate_jobsteps.call_count == 0
        assert driver.launchTasks.call_count == 0
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 0

        # Decline any unused offers. Expect the offer with insufficient
        # resources to schedule the jobstep.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_tries_all_offers(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1', cpus=8)]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(cluster="foo_cluster", cpus=4)
        offer2 = self._make_offer(cluster="foo_cluster", cpus=8)

        def check_tasks(offer_id, tasks, filters):
            assert offer_id == offer2.id
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer2.slave_id.value
            assert filters.refuse_seconds == 1.0
        driver.launchTasks.side_effect = check_tasks

        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        api.post_allocate_jobsteps.assert_called_once_with(['1'], cluster="foo_cluster")
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

        # Decline any unused offers (should be none)
        cs.wait_for_shutdown(driver)
        assert driver.declineOffer.call_count == 0

    def test_least_loaded(self):
        api = mock.Mock(spec=ChangesAPI)
        # task 4 won't be allocated if we schedule tasks in the order they're returned
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1'), self._make_changes_task('2'),
                                                  self._make_changes_task('3'), self._make_changes_task('4', cpus=3)]
        api.post_allocate_jobsteps.return_value = ['2', '1', '3']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id='offer1', cpus=4, mem=8192)
        # should get loaded first
        offer2 = self._make_offer(id='offer2', cpus=4, mem=8193)

        def check_tasks(offer_id, tasks, filters):
            assert offer_id in (offer1.id, offer2.id)
            if offer_id == offer1.id:
                assert len(tasks) == 1
                # after task 1 is allocated, this slave is least loaded, so
                # second task should go to it.
                assert tasks[0].name == 'foo 2'
                assert tasks[0].slave_id.value == offer1.slave_id.value
            elif offer_id == offer2.id:
                assert len(tasks) == 2
                assert tasks[0].name == 'foo 1'
                assert tasks[0].slave_id.value == offer2.slave_id.value
                # for task 3 this slave is least loaded again
                assert tasks[1].name == 'foo 3'
                assert tasks[1].slave_id.value == offer2.slave_id.value
            assert filters.refuse_seconds == 1.0

        driver.launchTasks.side_effect = check_tasks

        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster=None)
        api.post_allocate_jobsteps.assert_called_once_with(['2', '1', '3'], cluster=None)
        assert driver.launchTasks.call_count == 2
        assert cs.tasksLaunched == 3

        # Decline any unused offers (should be none)
        cs.wait_for_shutdown(driver)
        assert driver.declineOffer.call_count == 0

    def test_alloc_failed(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.side_effect = lambda limit, cluster: [self._make_changes_task(id=cluster)]
        def post_allocate_jobsteps(ids, cluster):
            if cluster == '1':
                return ['1']
            else:
                raise APIError('Failure')
        api.post_allocate_jobsteps.side_effect = post_allocate_jobsteps
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id="offer1", cluster="1")
        offer2 = self._make_offer(id="offer2", cluster="2")

        def check_tasks(offer_id, tasks, filters):
            assert offer_id == offer1.id
            # other task should still get launched if second one failed.
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer1.slave_id.value
            assert filters.refuse_seconds == 1.0
        driver.launchTasks.side_effect = check_tasks

        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_has_calls([mock.call(limit=200, cluster='1'), mock.call(limit=200, cluster='2')],
                                                   any_order=True)
        assert api.get_allocate_jobsteps.call_count == 2
        api.post_allocate_jobsteps.assert_has_calls([mock.call(['1'], cluster='1'), mock.call(['2'], cluster='2')],
                                                    any_order=True)
        assert api.post_allocate_jobsteps.call_count == 2
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1
        assert driver.declineOffer.call_count == 0

        # Decline any unused offers (offer2 should be open, since it failed
        # to schedule.)
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer2.id)

    def test_group_snapshots_on_same_machine(self):
        # Create 2 tasks with same snapshot and assert they both go to the
        # same slave.
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1', cpus=2, snapshot='snapfoo'),
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['1', '2']

        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        def launchTasks(offer, tasks, filters=None):
            # Assert all launched tasks go to offer1 (host1)
            # Due to stable sorting of offers based on remaining resources,
            # host1 is assured to be picked first.
            assert offer == mesos_pb2.OfferID(value="offer1")

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)

        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers. Expect offer2 remains, since both tasks
        # were schedule on offer1.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer2.id)

    def test_fall_back_to_least_loaded(self):
        # Fall back to least-loaded assignment if the snapshot for a task is
        # not found on any slave.
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1', cpus=2, snapshot='snapfoo'),
            self._make_changes_task('2', cpus=2, snapshot='snapbar')
        ]
        api.post_allocate_jobsteps.return_value = ['1', '2']

        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)

        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 2  # Jobs are sent to separate slaves
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers (should be none)
        cs.wait_for_shutdown(driver)
        assert driver.declineOffer.call_count == 0

    def test_prefer_loaded_slave_with_snapshot(self):
        # Fall back to least-loaded assignment if the snapshot for a task is
        # not found on any slave.
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['1']

        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        _sync_resource_offers(cs, driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        cs.wait_for_shutdown(driver)
        api.reset_mock()
        driver.reset_mock()

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offer, tasks, filters=None):
            # Assert launched task goes to offer1 (host1)
            # although it has lesser resources than host2
            assert offer == mesos_pb2.OfferID(value="offer1")

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=2)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers. Expect offer2 remains since offer1 was
        # prefered.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer2.id)

    def test_slave_with_snapshot_unavailable(self):
        # Fall back to least-loaded assignment if the snapshot for a task is
        # not found on any slave.
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['1']

        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        _sync_resource_offers(cs, driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        cs.wait_for_shutdown(driver)
        api.reset_mock()
        driver.reset_mock()

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        # Use this slightly roundabout way of verify launchTasks in order to
        # avoid hanging the changes-polling thread. Otherwise the test will
        # hang when it fails.
        expected_launched_tasks = [mesos_pb2.OfferID(value="offer2").value]
        launched_tasks = []
        def launchTasks(offer, tasks, filters=None):
            # Assert offer is accepted although slave doesn't have snapshot.
            launched_tasks.append(offer.value)

        driver.launchTasks.side_effect = launchTasks

        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        _sync_resource_offers(cs, driver, [offer2])
        assert launched_tasks == expected_launched_tasks

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers (should be none)
        cs.wait_for_shutdown(driver)
        assert driver.declineOffer.call_count == 0

    @mock.patch('time.time')
    def test_slave_with_stale_snapshot(self, time_mock):
        # Fall back to least-loaded assignment if the snapshot for a task is
        # not found on any slave.
        api = mock.Mock(spec=ChangesAPI)
        time_mock.return_value = 1
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['1']

        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        _sync_resource_offers(cs, driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1

        cs.wait_for_shutdown(driver)
        api.reset_mock()
        driver.reset_mock()
        time_mock.return_value = 1000000

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offer, tasks, filters=None):
            # Ignore stale snapshot association and select least-loaded slave.
            assert offer == mesos_pb2.OfferID(value="offer2")

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=2)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        _sync_resource_offers(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers. Expect offer1 remains, since offer2 was
        # least-loaded.
        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_called_once_with(offer1.id)

    def test_cached_offer_is_used(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        # Scheduler has an offer, but no tasks.
        api.get_allocate_jobsteps.return_value = []
        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        _sync_resource_offers(cs, driver, [offer1])
        assert api.get_allocate_jobsteps.call_count == 1
        assert api.post_allocate_jobsteps.call_count == 0

        cs.wait_for_shutdown(driver)
        api.reset_mock()
        driver.reset_mock()

        # When an offer arrives, the scheduler uses on the cached offer.
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']
        _sync_resource_offers(cs, driver, [])
        assert api.get_allocate_jobsteps.call_count == 1
        assert api.post_allocate_jobsteps.call_count == 1

        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_not_called()

    def test_offer_rescinded(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        # Scheduler has an offer, but no tasks are available.
        api.get_allocate_jobsteps.return_value = []
        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        _sync_resource_offers(cs, driver, [offer1])
        assert api.get_allocate_jobsteps.call_count == 1
        assert api.post_allocate_jobsteps.call_count == 0

        cs.wait_for_shutdown(driver)
        api.reset_mock()
        driver.reset_mock()

        # Offer gets rescinded by Mesos master.
        cs.offerRescinded(driver, offer1.id)
        api.get_allocate_jobsteps.assert_not_called()
        api.reset_mock()
        driver.reset_mock()
        
        # Now changes has no offers, so the task can't be scheduled.
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        _sync_resource_offers(cs, driver, [])
        # No offers -> no clusters to query for -> no get_allocate_jobsteps calls.
        assert api.get_allocate_jobsteps.call_count == 0  
        assert api.post_allocate_jobsteps.call_count == 0

        cs.wait_for_shutdown(driver)
        driver.declineOffer.assert_not_called()
