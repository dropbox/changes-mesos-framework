import logging
import os
import shutil
import tempfile
import time

from collections import defaultdict

import mock

from unittest import TestCase

# Capture debug logging output on test failure
logger = logging.getLogger()
logger.level = logging.DEBUG

try:
    from mesos.interface import mesos_pb2
except ImportError:
    import mesos_pb2

from changes_mesos_scheduler.changes_scheduler import ChangesScheduler, APIError, FileBlacklist, ChangesAPI

def _noop_blacklist():
    """Returns a blacklist instance that behaves like an empty blacklist."""
    m = mock.Mock(spec=FileBlacklist)
    m.contains.return_value = False
    return m


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
        cs = ChangesScheduler(state_file=None, api=api,
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
            cs.resourceOffers(driver, [offer])
            getmtime.assert_called_with(blpath)
        driver.declineOffer.assert_called_once_with(offer.id)
        assert api.allocate_jobsteps.call_count == 0


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
        expected_declines = [offer2, offer5, offer6]

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
        cs = ChangesScheduler(state_file=None, api=api, blacklist=_noop_blacklist())
        driver = mock.Mock()
        cs.resourceOffers(driver, all_offers)

        # Check that the maintenanced offers are declined.
        for offer in expected_declines:
            driver.declineOffer.assert_any_call(offer.id)
        assert driver.declineOffer.call_count == len(expected_declines)

        # Check that the non-maintenanced tasks are launched.
        assert driver.launchTasks.call_count == len(expected_launches)
        for launch_offer, args in zip(expected_launches,
                                      driver.launchTasks.call_args_list):
            assert args[0][0] == launch_offer.id

    def test_error_stats(self):
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=mock.Mock(), stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()
        cs.error(driver, 'message')
        stats.incr.assert_called_once_with('errors')

    def test_api_error(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.side_effect = APIError("Failure")
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer()

        cs.resourceOffers(driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster=None)
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_api_no_tasks(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = []
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster")

        cs.resourceOffers(driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_api_one_task(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1')]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster")

        def check_tasks(offer_id, tasks):
            assert offer_id == offer.id
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer.slave_id.value
            assert tasks[0].command.value == 'ls'
            assert tasks[0].resources[0].name == "cpus"
            assert tasks[0].resources[0].scalar.value == 2
            assert tasks[0].resources[1].name == "mem"
            assert tasks[0].resources[1].scalar.value == 4096
        driver.launchTasks.side_effect = check_tasks

        cs.resourceOffers(driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        api.post_allocate_jobsteps.assert_called_once_with(['1'], cluster="foo_cluster")
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

    def test_not_enough_resources(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1', cpus=8)]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = self._make_offer(cluster="foo_cluster", cpus=4)

        cs.resourceOffers(driver, [offer])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        assert api.post_allocate_jobsteps.call_count == 0
        assert driver.launchTasks.call_count == 0
        driver.declineOffer.assert_called_once_with(offer.id)
        assert cs.tasksLaunched == 0

    def test_tries_all_offers(self):
        api = mock.Mock(spec=ChangesAPI)
        api.get_allocate_jobsteps.return_value = [self._make_changes_task('1', cpus=8)]
        api.post_allocate_jobsteps.return_value = ['1']
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer1 = self._make_offer(cluster="foo_cluster", cpus=4)
        offer2 = self._make_offer(cluster="foo_cluster", cpus=8)

        def check_tasks(offer_id, tasks):
            assert offer_id == offer2.id
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer2.slave_id.value
        driver.launchTasks.side_effect = check_tasks

        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster="foo_cluster")
        api.post_allocate_jobsteps.assert_called_once_with(['1'], cluster="foo_cluster")
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

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

        def check_tasks(offer_id, tasks):
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

        driver.launchTasks.side_effect = check_tasks

        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200, cluster=None)
        api.post_allocate_jobsteps.assert_called_once_with(['1', '2', '3'], cluster=None)
        assert driver.launchTasks.call_count == 2
        assert cs.tasksLaunched == 3

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

        def check_tasks(offer_id, tasks):
            assert offer_id == offer1.id
            # other task should still get launched if second one failed.
            assert len(tasks) == 1
            assert tasks[0].name == 'foo 1'
            assert tasks[0].slave_id.value == offer1.slave_id.value
        driver.launchTasks.side_effect = check_tasks

        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_has_calls([mock.call(limit=200, cluster='1'), mock.call(limit=200, cluster='2')],
                                                   any_order=True)
        assert api.get_allocate_jobsteps.call_count == 2
        api.post_allocate_jobsteps.assert_has_calls([mock.call(['1'], cluster='1'), mock.call(['2'], cluster='2')],
                                                    any_order=True)
        assert api.post_allocate_jobsteps.call_count == 2
        assert driver.launchTasks.call_count == 1
        assert cs.tasksLaunched == 1

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

        def launchTasks(offer, tasks):
            # Assert all launched tasks go to offer1 (host1)
            # Due to stable sorting of offers based on remaining resources,
            # host1 is assured to be picked first.
            assert offer == mesos_pb2.OfferID(value="offer1")

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=4)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)

        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 1
        assert cs.tasksLaunched == 2

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

        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 2  # Jobs are sent to separate slaves
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

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
        cs.resourceOffers(driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        api.reset_mock()
        driver.reset_mock()

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offer, tasks):
            # Assert launched task goes to offer1 (host1)
            # although it has lesser resources than host2
            assert offer == mesos_pb2.OfferID(value="offer1")

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=2)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 1
        assert cs.tasksLaunched == 2

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
        cs.resourceOffers(driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        api.reset_mock()
        driver.reset_mock()

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offer, tasks):
            # Assert offer is accepted although slave doesn't have snapshot.
            assert offer == mesos_pb2.OfferID(value="offer2")

        driver.launchTasks.side_effect = launchTasks

        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        cs.resourceOffers(driver, [offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 0
        assert cs.tasksLaunched == 2

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
        cs.resourceOffers(driver, [offer1])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)

        api.reset_mock()
        driver.reset_mock()
        time_mock.return_value = 1000000

        api.get_allocate_jobsteps.return_value = [
            self._make_changes_task('2', cpus=2, snapshot='snapfoo')
        ]
        api.post_allocate_jobsteps.return_value = ['2']

        def launchTasks(offer, tasks):
            # Ignore stale snapshot association and select least-loaded slave.
            assert offer == mesos_pb2.OfferID(value="offer2")

        driver.launchTasks.side_effect = launchTasks

        offer1 = self._make_offer(id='offer1', hostname='host1', cpus=2)
        offer2 = self._make_offer(id='offer2', hostname='host2', cpus=4)
        cs.resourceOffers(driver, [offer1, offer2])

        api.get_allocate_jobsteps.assert_called_once_with(limit=200,
                                                          cluster=None)
        assert api.post_allocate_jobsteps.call_count == 1
        assert driver.launchTasks.call_count == 1
        assert driver.declineOffer.call_count == 1
        assert cs.tasksLaunched == 2
