#!/usr/bin/env python

import sys
import threading
import time

import mesos
import mesos_pb2

class ChangesExecutor(mesos.Executor):
  def launchTask(self, driver, task):
    # Create a thread to run the task. Tasks should always be run in new
    # threads or processes, rather than inside launchTask itself.
    def run_task():
      print "Running task %s" % task.task_id.value
      update = mesos_pb2.TaskStatus()
      update.task_id.value = task.task_id.value
      update.state = mesos_pb2.TASK_RUNNING
      update.data = 'data with a \0 byte'
      driver.sendStatusUpdate(update)

      # This is where one would perform the requested task.

      print "Sending status update..."
      update = mesos_pb2.TaskStatus()
      update.task_id.value = task.task_id.value
      update.state = mesos_pb2.TASK_FINISHED
      update.data = 'data with a \0 byte'
      driver.sendStatusUpdate(update)
      print "Sent status update"

    thread = threading.Thread(target=run_task)
    thread.start()

  def frameworkMessage(self, driver, message):
    # Send it back to the scheduler.
    driver.sendFrameworkMessage(message)

if __name__ == "__main__":
  print "Starting executor"
  driver = mesos.MesosExecutorDriver(ChangesExecutor())
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
