import os
import tempfile

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
        cs.taskJobStepMapping['task x'] = ['jobstep x']
        cs.save_state()

        cs2 = ChangesScheduler('changes url', test_dir, state_file)
        assert 5 == cs2.tasksLaunched
        assert 3 == cs2.tasksFinished
        assert {'task x': ['jobstep x']} == cs2.taskJobStepMapping
        assert not os.path.exists(state_file)
