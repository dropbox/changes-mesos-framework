import logging
import json
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

from mesos.interface import mesos_pb2
from mesos.interface import Scheduler

from changes_mesos_scheduler.changes_scheduler import ChangesScheduler, APIError, FileBlacklist, ChangesAPI, SlaveInfo
from changes_mesos_scheduler import statsreporter

def _noop_blacklist():
    """Returns a blacklist instance that behaves like an empty blacklist."""
    m = mock.Mock(spec=FileBlacklist)
    m.contains.return_value = False
    return m


def help_resource_offers_and_poll_changes(cs, driver, new_offers):
    # type: (ChangesScheduler, Scheduler, List[Any]) -> None
    """Receive offers from the Mesos master and poll Changes for new jobsteps
    in a synchronous manner to facilitate simpler, more straightforward
    testing. Normally these two tasks run in separate threads.
    Args:
        driver: the MesosSchedulerDriver object
        new_offers: A list of Mesos Offer protobufs that should be offered to
            the scheduler.
    """
    cs.shuttingDown.clear()                     # reset shuttingDown if necessary.
    cs.resourceOffers(driver, new_offers)       # Get offers from Mesos master.
    assert not cs.poll_and_launch_once(driver)  # Get jobsteps and launch them.


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

    def _make_task_status(self, id='taskid', state=mesos_pb2.TASK_FINISHED,
                          message="foo", slave_id='slaveid', jobstep_id='1'):
        status = mesos_pb2.TaskStatus(
            task_id=mesos_pb2.TaskID(value=id),
            slave_id=mesos_pb2.SlaveID(value=slave_id),
            state=state,
            message=message,
        )
        return status

    def _make_offer(self,
                    hostname=None,
                    cpus=4,
                    mem=8192,
                    cluster=None,
                    id='offerid',
                    unavailability_start_secs=None,
                    unavailability_duration_secs=None):
        # Offers with different IDs will have different hostnames, unless
        # otherwise explicitly specified.
        if hostname is None:
            hostname = id
        offer = mesos_pb2.Offer(
            id=mesos_pb2.OfferID(value=id),
            framework_id=mesos_pb2.FrameworkID(value="frameworkid"),
            slave_id=mesos_pb2.SlaveID(value="slave_id_" + hostname),
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
        cs.slaveIdInfo['slaveid'] = SlaveInfo(hostname='aHostname')
        cs._snapshot_slave_map = defaultdict(lambda: defaultdict(float))
        cs._snapshot_slave_map['snapid']['host1'] = 1234567.0
        cs._snapshot_slave_map['snapid']['host2'] = 1234569.0
        cs.save_state()
        cs = None

        cs2 = ChangesScheduler(state_file, api=mock.Mock(),
                               blacklist=_noop_blacklist())
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': 'jobstep x'} == cs2.taskJobStepMapping
        assert cs2.slaveIdInfo['slaveid'].hostname == 'aHostname'
        assert not os.path.exists(state_file)
        assert {'snapid': {'host1': 1234567.0, 'host2': 1234569.0}} == cs2._snapshot_slave_map

    def test_save_restore_state_missing(self):
        state_file = self.test_dir + '/test.json'

        state = {'framework_id': 1,
                 'tasksLaunched': 5,
                 'tasksFinished': 3,
                 'taskJobStepMapping': {'task x': 'jobstep x'},
                 'snapshot_slave_map': {}
                 }

        with open(state_file, 'w') as f:
            f.write(json.dumps(state))

        cs2 = ChangesScheduler(state_file, api=mock.Mock(),
                               blacklist=_noop_blacklist())
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': 'jobstep x'} == cs2.taskJobStepMapping
        # when the scheduler is first updated to have slaveIdInfo, state.json
        # won't have anything about slaveIdInfo; test that it works anyway
        assert cs2.slaveIdInfo == {}
        assert not os.path.exists(state_file)
        assert {} == cs2._snapshot_slave_map

    def test_task_finished(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        cs.taskJobStepMapping = {'taskid': '1'}
        cs.slaveIdInfo = {'slaveid': SlaveInfo(hostname='aHostname')}
        driver = mock.Mock()

        status = self._make_task_status(id='taskid', jobstep_id='1')

        cs.statusUpdate(driver, status)

        assert cs.tasksFinished == 1
        assert len(cs.taskJobStepMapping) == 0

        api.update_jobstep.assert_called_once_with('1', status='finished', hostname='aHostname')

    def test_task_failed(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        cs.taskJobStepMapping = {'taskid': '1'}
        cs.slaveIdInfo = {'slaveid': SlaveInfo(hostname='aHostname')}
        driver = mock.Mock()

        status = self._make_task_status(id='taskid', jobstep_id='1', state=mesos_pb2.TASK_FAILED)

        cs.statusUpdate(driver, status)

        assert cs.tasksFinished == 0
        assert len(cs.taskJobStepMapping) == 1

        assert api.jobstep_console_append.call_count == 1
        api.update_jobstep.assert_called_once_with('1', status='finished', result='infra_failed', hostname='aHostname')

    def test_missing_jobstep_mapping(self):
        api = mock.Mock(spec=ChangesAPI)
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=api, stats=stats,
                              blacklist=_noop_blacklist())
        cs.taskJobStepMapping = {}
        driver = mock.Mock()

        status = self._make_task_status(id='taskid', jobstep_id='1', state=mesos_pb2.TASK_FINISHED)

        cs.statusUpdate(driver, status)

        assert cs.tasksFinished == 1

        stats.incr.assert_called_once_with('missing_jobstep_id_finished')

    def test_missing_hostname_mapping(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        cs.taskJobStepMapping = {'taskid': '1'}
        driver = mock.Mock()

        status = self._make_task_status(id='taskid', jobstep_id='1')

        cs.statusUpdate(driver, status)

        assert cs.tasksFinished == 1
        assert len(cs.taskJobStepMapping) == 0

        api.update_jobstep.assert_called_once_with('1', status='finished', hostname=None)

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
            help_resource_offers_and_poll_changes(cs, driver, [offer])
            getmtime.assert_called_with(blpath)
        assert api.declineOffer.call_count == 0
        assert api.allocate_jobsteps.call_count == 0

        assert stats.incr.call_count == 3
        stats.incr.assert_any_call('ignore_for_blacklist', 1)
        stats.incr.assert_any_call('ignore_for_maintenance', 0)

        # Decline any unused offers. Expect the blacklisted offer.
        cs.decline_open_offers(driver)
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
        offer4 = self._make_offer(hostname='hostname_4.com',
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
        help_resource_offers_and_poll_changes(cs, driver, all_offers)

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
            assert len(args[0][0]) == 1  # only one OfferId in the launch args.
            actual_launch_set.add(args[0][0][0].value)
        assert actual_launch_set == expected_launch_set

        # Decline any unused offers. Expect all maintenanced offers.
        cs.decline_open_offers(driver)
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

    def test_slaveLost(self):
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=mock.Mock(), stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        pb_offer = self._make_offer(hostname="hostname")

        # Check removing an unrecognized slave.
        assert len(cs._cached_slaves) == 0
        cs.slaveLost(driver, pb_offer.slave_id)
        stats.incr.assert_called_once_with('slave_lost')

        # Check removing a recognized slave.
        cs.resourceOffers(driver, [pb_offer])
        stats.reset_mock()
        assert len(cs._cached_slaves) == 1

        cs.slaveLost(driver, pb_offer.slave_id)
        assert stats.incr.call_count == 2
        stats.incr.assert_any_call('decline_for_slave_lost', 1)
        stats.incr.assert_any_call('slave_lost')
        assert len(cs._cached_slaves) == 0

    def test_disconnected(self):
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=mock.Mock(), stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        pb_offer = self._make_offer(hostname="hostname")
        cs.resourceOffers(driver, [pb_offer])
        assert len(cs._cached_slaves) == 1

        cs.disconnected(driver)
        assert len(cs._cached_slaves) == 0

    def test_api_error(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.side_effect = APIError("Failure")
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer()

        help_resource_offers_and_poll_changes(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster=None)
        assert driver.declineOffer.call_count == 0

        # Decline any unused offers. Expect the errored offer.
        cs.decline_open_offers(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_api_no_tasks(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = []
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster")

        help_resource_offers_and_poll_changes(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        assert driver.declineOffer.call_count == 0

        # Decline any unused offers. Expect the only existing offer.
        cs.decline_open_offers(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_api_one_task(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1')]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(hostname='aHostname', cluster="foo_cluster")

        def check_tasks(offer_ids, tasks, filters):
            assert len(offer_ids) == 1
            assert offer_ids[0] == offer.id
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer.slave_id.value
            assert tasks[0].command.value == 'ls'
            assert tasks[0].resources[0].name == "cpus"
            assert tasks[0].resources[0].scalar.value == 2
            assert tasks[0].resources[1].name == "mem"
            assert tasks[0].resources[1].scalar.value == 4096
            assert filters.refuse_seconds == 1.0
        driver.launchTasks.side_effect = check_tasks

        help_resource_offers_and_poll_changes(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        api.post_allocate_jobsteps.assert_called_once_with(['1'], cluster="foo_cluster")
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

        # Decline any unused offers (should be none)
        cs.decline_open_offers(driver)
        assert driver.declineOffer.call_count == 0

    def test_not_enough_resources(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1', cpus=8)]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster", cpus=4)

        help_resource_offers_and_poll_changes(cs, driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        assert api.post_allocate_jobsteps.call_count == 0
        assert driver.launchTasks.call_count == 0
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 0

        # Decline any unused offers. Expect the offer with insufficient
        # resources to schedule the jobstep.
        cs.decline_open_offers(driver)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_tries_all_offers(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1', cpus=8)]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(hostname="host1", cluster="foo_cluster", cpus=4)
        offer2 = self._make_offer(hostname="host2", cluster="foo_cluster", cpus=8)

        def check_tasks(offer_ids, tasks, filters):
            assert offer_ids == [offer2.id]
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer2.slave_id.value
            assert filters.refuse_seconds == 1.0
        driver.launchTasks.side_effect = check_tasks

        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        api.post_allocate_jobsteps.assert_called_once_with(['1'], cluster="foo_cluster")
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

        # Decline any unused offers (should be one)
        cs.decline_open_offers(driver)
        assert driver.declineOffer.call_count == 1

    def test_least_loaded(self):
        api = mock.Mock(spec=ChangesAPI)
        # task 4 won't be allocated if we schedule tasks in the order they're
        # returned
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1'), self._make_changes_task('2'),
            self._make_changes_task('3'), self._make_changes_task('4', cpus=3),
        ]
        api.post_allocate_jobsteps.return_value = ['1', '2', '3']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(id='offer1', cpus=4, mem=8192)
        # should get loaded first
        offer2 = self._make_offer(id='offer2', cpus=4, mem=8193)

        def check_tasks(offer_ids, tasks, filters):
            assert len(offer_ids) == 1
            offer_id = offer_ids[0]
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

        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster=None)
        api.post_allocate_jobsteps.assert_called_once_with(['1', '2', '3'], cluster=None)
        assert driver.launchTasks.call_count == 2
        assert cs.tasksLaunched == 3

        # Decline any unused offers (should be none)
        cs.decline_open_offers(driver)
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

        def check_tasks(offer_ids, tasks, filters):
            assert len(offer_ids) == 1
            assert offer_ids[0] == offer1.id
            # other task should still get launched if second one failed.
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer1.slave_id.value
            assert filters.refuse_seconds == 1.0
        driver.launchTasks.side_effect = check_tasks

        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

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
        cs.decline_open_offers(driver)
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

        launched_offer_id = None
        def launchTasks(offers, tasks, filters=None):
            # Assert all launched tasks go to offer1 (host1)
            # host1 is assured to be picked first because it has slightly more
            # resources at first.
            assert len(offers) == 1
            assert offers == [mesos_pb2.OfferID(value="offer1")]

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=5)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)

        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers. Expect offer2 remains, since both tasks
        # were schedule on offer1.
        cs.decline_open_offers(driver)
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

        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 2  # Jobs are sent to separate slaves
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers (should be none)
        cs.decline_open_offers(driver)
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
        help_resource_offers_and_poll_changes(cs, driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        cs.decline_open_offers(driver)
        api.reset_mock()
        driver.reset_mock()

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offers, tasks, filters=None):
            # Assert launched task goes to offer1 (host1)
            # although it has lesser resources than host2
            assert offers == [mesos_pb2.OfferID(value="offer1")]

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=2)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers. Expect offer2 remains since offer1 was
        # prefered.
        cs.decline_open_offers(driver)
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
        help_resource_offers_and_poll_changes(cs, driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        cs.decline_open_offers(driver)
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
        def launchTasks(offers, tasks, filters=None):
            # Assert offer is accepted although slave doesn't have snapshot.
            assert len(offers) == 1
            launched_tasks.append(offers[0].value)

        driver.launchTasks.side_effect = launchTasks

        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        help_resource_offers_and_poll_changes(cs, driver, [offer2])
        assert launched_tasks == expected_launched_tasks

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers (should be none)
        cs.decline_open_offers(driver)
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
        help_resource_offers_and_poll_changes(cs, driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1

        cs.decline_open_offers(driver)
        api.reset_mock()
        driver.reset_mock()
        time_mock.return_value = 1000000

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offers, tasks, filters=None):
            # Ignore stale snapshot association and select least-loaded slave.
            assert offers == [mesos_pb2.OfferID(value="offer2")]

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=2)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        help_resource_offers_and_poll_changes(cs, driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

        # Decline any unused offers. Expect offer1 remains, since offer2 was
        # least-loaded.
        cs.decline_open_offers(driver)
        driver.declineOffer.assert_called_once_with(offer1.id)

    def test_cached_offer_is_used(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        # Scheduler has an offer, but no tasks.
        api.get_allocate_jobsteps.return_value = []
        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        help_resource_offers_and_poll_changes(cs, driver, [offer1])
        assert api.get_allocate_jobsteps.call_count == 1
        assert api.post_allocate_jobsteps.call_count == 0

        # Don't decline offers here like we decline offers when resetting
        # elsewhere. We need to leave the offer cache intact.
        api.reset_mock()
        driver.reset_mock()

        # When an offer arrives, the scheduler uses on the cached offer.
        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']
        help_resource_offers_and_poll_changes(cs, driver, [])
        assert api.get_allocate_jobsteps.call_count == 1
        assert api.post_allocate_jobsteps.call_count == 1

        cs.decline_open_offers(driver)
        driver.declineOffer.assert_not_called()

    def test_offer_rescinded(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        # Scheduler has an offer, but no tasks are available.
        api.get_allocate_jobsteps.return_value = []
        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        help_resource_offers_and_poll_changes(cs, driver, [offer1])
        assert api.get_allocate_jobsteps.call_count == 1
        assert api.post_allocate_jobsteps.call_count == 0

        cs.decline_open_offers(driver)
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
        help_resource_offers_and_poll_changes(cs, driver, [])
        # No offers -> no clusters to query for -> no get_allocate_jobsteps calls.
        assert api.get_allocate_jobsteps.call_count == 0
        assert api.post_allocate_jobsteps.call_count == 0

        cs.decline_open_offers(driver)
        driver.declineOffer.assert_not_called()

    def test_combine_offer_fragments(self):
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('1', cpus=2, mem=2048),
        ]
        api.post_allocate_jobsteps.return_value = ['1']

        # Add an offer for a different host, to make sure offers for different
        # hosts aren't being merged/defragmented.
        host2_offer = self._make_offer(id='host2_offer', hostname='host2',
                                       cpus=1, mem=1024)
        cs.resourceOffers(driver, [host2_offer])

        # Add a set of small, fragmented offers one at a time. The task can
        # only be scheduled once all offers have arrived. By our powers
        # combined!
        offers = ((1, 0), (1, 0), (0, 1024), (0, 1024))
        expected_offer_ids = []
        for i, (cpu, mem) in enumerate(offers):
            offer_id = 'host1_offer{}'.format(i)
            new_offer = self._make_offer(id=offer_id, hostname='host1',
                                         cpus=cpu, mem=mem)
            expected_offer_ids.append(new_offer.id)

            if i < len(offers) - 1:  # Not the last iteration
                help_resource_offers_and_poll_changes(cs, driver, [new_offer])
                assert api.get_allocate_jobsteps.call_count == 1
                assert api.post_allocate_jobsteps.call_count == 0
            else:  # on the final iteration only
                def check_tasks(offer_ids, tasks, filters):
                    assert offer_ids == expected_offer_ids
                    assert len(tasks) == 1
                driver.launchTasks.side_effect = check_tasks
                help_resource_offers_and_poll_changes(cs, driver, [new_offer])
                assert api.get_allocate_jobsteps.call_count == 1
                assert api.post_allocate_jobsteps.call_count == 1
            api.reset_mock()
            driver.reset_mock()
        cs.decline_open_offers(driver)
        driver.declineOffer.assert_not_called()

    def test_full_thread_polling(self):
        """This test simply runs through the startup/teardown machinery for the
        Changes polling thread. We run the polling a couple of times to ensure
        the looping works properly.
        """
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        # Add an offer to ensure get_allocate_jobsteps() is polled.
        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        cs.resourceOffers(driver, [offer1])

        # After get_allocate_jobsteps() is called a couple of times, shut down.
        count = [0]
        def get_allocate_jobsteps(limit, cluster):
            if count[0] > 3:
                cs.shuttingDown.set()
            count[0] += 1
            return []
        api.get_allocate_jobsteps.side_effect = get_allocate_jobsteps

        # Just makes sure this executes with no exceptions.
        cs.poll_changes_until_shutdown(driver, 0)

    def test_full_thread_polling_with_exception(self):
        """Test that exceptions in the polling thread are reported back to the
        main thread correctly.
        """
        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        # Add an offer to ensure get_allocate_jobsteps() is polled.
        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        cs.resourceOffers(driver, [offer1])

        # Force an exception in get_allocate_jobsteps()
        def get_allocate_jobsteps(limit, cluster):
            assert False
        api.get_allocate_jobsteps.side_effect = get_allocate_jobsteps

        class Filter(logging.Filter):
            def __init__(self):
                super(Filter, self).__init__()
                self.found_error = False

            def filter(self, record):
                if record.getMessage() == "Polling thread failed. Exiting.":
                    self.found_error = True

        f = Filter()
        try:
            logger.addFilter(f)
            cs.poll_changes_until_shutdown(driver, 0)
            assert f.found_error
        finally:
            logger.removeFilter(f)

    def test_state_json(self):
        framework_id = 'frameworkid'
        changes_request_limit = 53

        blpath = self.test_dir + '/blacklist'
        blacklist = open(blpath, 'w+')
        blacklist.write('hostname1\nhostname2\n')
        blacklist.close()

        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None,
                              api=api,
                              blacklist=FileBlacklist(blpath),
                              changes_request_limit=changes_request_limit)
        cs.framework_id = framework_id
        driver = mock.Mock()
        now = time.time()

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=1, mem=1024)
        offer2 = self._make_offer(id='offer2', hostname='host1', cpus=2, mem=2048)
        offer3 = self._make_offer(id='offer3', hostname='host3', cpus=4, mem=4096,
                                  cluster='some_cluster')
        offer4 = self._make_offer(hostname='host4',
                                  id='offer4',
                                  cpus=5,
                                  mem=5000,
                                  unavailability_start_secs=now - 5,
                                  unavailability_duration_secs=100)
        cs.resourceOffers(driver, [offer1, offer2, offer3, offer4])

        expected_state = {
            'framework_id': framework_id,
            'taskJobStepMapping': {},
            'tasksLaunched': 0,
            'tasksFinished': 0,
            'shuttingDown': False,
            'blacklist': {
                'path': blpath,
                'entries': [
                    'hostname1',
                    'hostname2',
                ],
            },
            'snapshot_slave_map': {},
            'changes_request_limit': changes_request_limit,
            'cached_slaves': [
                {
                    'slave_id': 'slave_id_host1',
                    'hostname': 'host1',
                    'cluster': None,
                    'offers': [
                        {
                            'offer_id': 'offer1',
                            'framework_id': framework_id,
                            'url': '',
                            'cpu': 1.0,
                            'mem': 1024,
                            'attributes': [],
                            'resources': [
                                {'name': 'cpus', 'type': mesos_pb2.Value.SCALAR, 'value': 1},
                                {'name': 'mem', 'type': mesos_pb2.Value.SCALAR, 'value': 1024},
                            ],
                        },
                        {
                            'offer_id': 'offer2',
                            'framework_id': framework_id,
                            'url': '',
                            'cpu': 2.0,
                            'mem': 2048,
                            'attributes': [],
                            'resources': [
                                {'name': 'cpus', 'type': mesos_pb2.Value.SCALAR, 'value': 2},
                                {'name': 'mem', 'type': mesos_pb2.Value.SCALAR, 'value': 2048},
                            ],
                        },
                    ],
                    'total_cpu': 3.0,
                    'total_mem': 3072,
                    'is_maintenanced': False,
                },
                {
                    'slave_id': 'slave_id_host3',
                    'hostname': 'host3',
                    'cluster': 'some_cluster',
                    'offers': [
                        {
                            'offer_id': 'offer3',
                            'framework_id': framework_id,
                            'url': '',
                            'cpu': 4.0,
                            'mem': 4096,
                            'attributes': [
                                {'name': 'labels', 'type': mesos_pb2.Value.TEXT, 'value': 'some_cluster'},
                            ],
                            'resources': [
                                {'name': 'cpus', 'type': mesos_pb2.Value.SCALAR, 'value': 4},
                                {'name': 'mem', 'type': mesos_pb2.Value.SCALAR, 'value': 4096},
                            ],
                        },
                    ],
                    'total_cpu': 4.0,
                    'total_mem': 4096,
                    'is_maintenanced': False,
                },
                {
                    'slave_id': 'slave_id_host4',
                    'hostname': 'host4',
                    'cluster': None,
                    'offers': [
                        {
                            'offer_id': 'offer4',
                            'framework_id': framework_id,
                            'url': '',
                            'cpu': 5.0,
                            'mem': 5000,
                            'attributes': [],
                            'resources': [
                                {'name': 'cpus', 'type': mesos_pb2.Value.SCALAR, 'value': 5},
                                {'name': 'mem', 'type': mesos_pb2.Value.SCALAR, 'value': 5000},
                            ],
                        },
                    ],
                    'total_cpu': 5.0,
                    'total_mem': 5000,
                    'is_maintenanced': True,
                },
            ],
            'build_state_json_secs': .5,
        }

        state_json = cs.state_json()
        state = json.loads(state_json)

        # Verify that we got a time-to-build with approximately the right order
        # of magnitude, then replace the value with something predictable.
        assert state['build_state_json_secs'] > 0
        assert state['build_state_json_secs'] < 10
        state['build_state_json_secs'] = expected_state['build_state_json_secs']

        # Verify a bunch of state fields individually to make diffing easier
        # when we find a problem
        for slave, expected_slave in (zip(state['cached_slaves'],
                                          expected_state['cached_slaves'])):
            for offer, expected_offer in zip(slave['offers'], expected_slave['offers']):
                assert offer == expected_offer

        # Compare all state keys individually.
        for key in expected_state:
            print 'Compare key {}: [{}] vs expected [{}]'.format(
                key, state[key], expected_state[key])
            assert state[key] == expected_state[key]

        # Ensure both dicts have the same number of keys, which means the
        # previous loop hit everything.
        assert sorted(expected_state.keys()) == sorted(state.keys())

        # Add some tasks to the scheduler and reschedule, to trigger some
        # snapshot-slave mappings.
        tasks = [
            self._make_changes_task('1', mem=3072, snapshot='snap1'),
            self._make_changes_task('2', mem=4096, snapshot='snap2'),
        ]
        api.get_allocate_jobsteps.return_value = tasks
        api.post_allocate_jobsteps.return_value = ['1', '2']

        assert api.get_allocate_jobsteps.reset()
        assert api.post_allocate_jobsteps.reset()
        assert not cs.poll_and_launch_once(driver)  # Get jobsteps and launch them.
        assert api.get_allocate_jobsteps.call_count == 2
        assert api.post_allocate_jobsteps.call_count == 2

        state_json = cs.state_json()
        state = json.loads(state_json)
        assert len(state['snapshot_slave_map']) == 2

    def test_state_json_performance(self):
        """Verify that the /state_json handler can build its JSON payload in
        less than .05 seconds, on average.
        """
        framework_id = 'frameworkid'
        changes_request_limit = 53

        blpath = self.test_dir + '/blacklist'
        blacklist = open(blpath, 'w+')
        blacklist.write('hostname1\nhostname2\n')
        blacklist.close()

        api = mock.Mock(spec=ChangesAPI)
        cs = ChangesScheduler(state_file=None,
                              api=api,
                              blacklist=FileBlacklist(blpath),
                              changes_request_limit=changes_request_limit)
        cs.framework_id = framework_id
        driver = mock.Mock()
        now = time.time()

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=1, mem=1024)
        offer2 = self._make_offer(id='offer2', hostname='host1', cpus=2, mem=2048)
        offer3 = self._make_offer(id='offer3', hostname='host3', cpus=4, mem=4096,
                                  cluster='some_cluster')
        offer4 = self._make_offer(hostname='host4',
                                  id='offer4',
                                  cpus=5,
                                  mem=5000,
                                  unavailability_start_secs=now - 5,
                                  unavailability_duration_secs=100)
        cs.resourceOffers(driver, [offer1, offer2, offer3, offer4])

        tasks = [
            self._make_changes_task('1', mem=3072, snapshot='snap1'),
            self._make_changes_task('2', mem=4096, snapshot='snap2'),
        ]
        api.get_allocate_jobsteps.return_value = tasks
        api.post_allocate_jobsteps.return_value = ['1', '2']
        assert not cs.poll_and_launch_once(driver)

        start_time = time.time()
        loops = 1000
        for i in xrange(loops):
            state_json = cs.state_json()
        total_time = time.time() - start_time

        max_avg_time_per_loop = .05
        assert total_time < max_avg_time_per_loop * loops
