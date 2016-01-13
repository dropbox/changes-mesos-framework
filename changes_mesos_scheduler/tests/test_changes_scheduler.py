import os
import tempfile
import time

import mock

from unittest import TestCase

try:
    from mesos.interface import mesos_pb2
except ImportError:
    import mesos_pb2

from changes_scheduler import ChangesScheduler, APIError


class ChangesSchedulerTest(TestCase):
    def test_save_restore_state(self):
        test_dir = tempfile.mkdtemp()
        state_file = test_dir + '/test.json'
        open(test_dir + '/blacklist', 'w+').close()

        cs = ChangesScheduler(test_dir, state_file, api=mock.Mock())
        cs.tasksLaunched = 5
        cs.tasksFinished = 3
        cs.taskJobStepMapping['task x'] = 'jobstep x'
        cs.save_state()

        cs2 = ChangesScheduler(test_dir, state_file, api=mock.Mock())
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': 'jobstep x'} == cs2.taskJobStepMapping
        assert not os.path.exists(state_file)

    def test_blacklist(self):
        test_dir = tempfile.mkdtemp()
        state_file = test_dir + '/test.json'
        blpath = test_dir + '/blacklist'
        # Ensure we have an empty blacklist file.
        open(blpath, 'w+').close()

        api = mock.MagicMock()
        cs = ChangesScheduler(test_dir, state_file, api=api)
        offer = mesos_pb2.Offer(
            id=mesos_pb2.OfferID(value="offerid"),
            framework_id=mesos_pb2.FrameworkID(value="frameworkid"),
            slave_id=mesos_pb2.SlaveID(value="slaveid"),
            hostname='some_hostname.com',
        )

        blacklist = open(test_dir + '/blacklist', 'w+')
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
        test_dir = tempfile.mkdtemp()
        open(test_dir + '/blacklist', 'w+').close()

        stats = mock.Mock()
        cs = ChangesScheduler(test_dir, 'nostatefile', api=mock.Mock(), stats=stats)
        driver = mock.Mock()
        cs.error(driver, 'message')
        stats.incr.assert_called_once_with('errors')

    def test_api_error(self):
        test_dir = tempfile.mkdtemp()
        open(test_dir + '/blacklist', 'w+').close()
        api = mock.Mock(spec_set=["allocate_jobsteps"])
        api.allocate_jobsteps.side_effect = APIError("Failure")
        cs = ChangesScheduler(test_dir, 'nostatefile', api=api)
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
