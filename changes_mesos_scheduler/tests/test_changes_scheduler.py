import os
import shutil
import tempfile
import time

import mock

from unittest import TestCase

try:
    from mesos.interface import mesos_pb2
except ImportError:
    import mesos_pb2

from changes_scheduler import ChangesScheduler, APIError, FileBlacklist


def _noop_blacklist():
    """Returns a blacklist instance that behaves like an empty blacklist."""
    m = mock.Mock(spec=FileBlacklist)
    m.contains.return_value = False
    return m


class ChangesSchedulerTest(TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        super(ChangesSchedulerTest, self).setUp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(ChangesSchedulerTest, self).tearDown()

    def test_save_restore_state(self):
        state_file = self.test_dir + '/test.json'

        cs = ChangesScheduler(state_file, api=mock.Mock(),
                              blacklist=_noop_blacklist())
        cs.tasksLaunched = 5
        cs.tasksFinished = 3
        cs.taskJobStepMapping['task x'] = 'jobstep x'
        cs.save_state()

        cs2 = ChangesScheduler(state_file, api=mock.Mock(),
                               blacklist=_noop_blacklist())
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': 'jobstep x'} == cs2.taskJobStepMapping
        assert not os.path.exists(state_file)

    def test_blacklist(self):
        blpath = self.test_dir + '/blacklist'
        # Ensure we have an empty blacklist file.
        open(blpath, 'w+').close()

        api = mock.MagicMock()
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=FileBlacklist(blpath))
        offer = mesos_pb2.Offer(
            id=mesos_pb2.OfferID(value="offerid"),
            framework_id=mesos_pb2.FrameworkID(value="frameworkid"),
            slave_id=mesos_pb2.SlaveID(value="slaveid"),
            hostname='some_hostname.com',
        )

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


    def test_error_stats(self):
        stats = mock.Mock()
        cs = ChangesScheduler(state_file=None, api=mock.Mock(), stats=stats,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()
        cs.error(driver, 'message')
        stats.incr.assert_called_once_with('errors')

    def test_api_error(self):
        api = mock.Mock(spec_set=["allocate_jobsteps"])
        api.allocate_jobsteps.side_effect = APIError("Failure")
        cs = ChangesScheduler(state_file=None, api=api,
                              blacklist=_noop_blacklist())
        driver = mock.Mock()

        offer = mesos_pb2.Offer(
            id=mesos_pb2.OfferID(value="offerid"),
            framework_id=mesos_pb2.FrameworkID(value="frameworkid"),
            slave_id=mesos_pb2.SlaveID(value="slaveid"),
            hostname='hostname',
        )
        offer.resources.add(name="cpus",
                            type=mesos_pb2.Value.SCALAR,
                            scalar=mesos_pb2.Value.Scalar(value=4))
        offer.resources.add(name="mem",
                            type=mesos_pb2.Value.SCALAR,
                            scalar=mesos_pb2.Value.Scalar(value=8192))
        cs.resourceOffers(driver, [offer])

        api.allocate_jobsteps.assert_called_once_with({
                "resources": {"cpus": 4, "mem": 8192},
        })
        driver.declineOffer.assert_called_once_with(offer.id)
