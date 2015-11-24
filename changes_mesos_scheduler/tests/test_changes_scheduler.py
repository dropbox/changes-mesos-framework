import os
import tempfile

import mock

from unittest import TestCase

from changes_scheduler import ChangesScheduler


class ChangesSchedulerTest(TestCase):
    def test_save_restore_state(self):
        test_dir = tempfile.mkdtemp()
        state_file = test_dir + '/test.json'
        open(test_dir + '/blacklist', 'w+').close()

        cs = ChangesScheduler('changes url', test_dir, state_file)
        cs.tasksLaunched = 5
        cs.tasksFinished = 3
        cs.taskJobStepMapping['task x'] = 'jobstep x'
        cs.save_state()

        cs2 = ChangesScheduler('changes url', test_dir, state_file)
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': 'jobstep x'} == cs2.taskJobStepMapping
        assert not os.path.exists(state_file)

    def test_blacklist(self):
        test_dir = tempfile.mkdtemp()
        state_file = test_dir + '/test.json'
        blacklist = open(test_dir + '/blacklist', 'w+')

        cs = ChangesScheduler('changes url', test_dir, state_file)
        driver = mock.Mock()
        offer = mock.Mock()
        offer.hostname = 'some_hostname.com'
        offer.id = '999'
        blacklist.write('some_hostname.com\n')
        blacklist.close()
        cs.resourceOffers(driver, [offer])
        driver.declineOffer.assert_called_once_with(offer.id)

    def test_error_stats(self):
        test_dir = tempfile.mkdtemp()
        open(test_dir + '/blacklist', 'w+').close()

        stats = mock.Mock()
        cs = ChangesScheduler('changes url', test_dir, 'nostatefile', stats=stats)
        driver = mock.Mock()
        cs.error(driver, 'message')
        stats.incr.assert_called_once_with('errors')
