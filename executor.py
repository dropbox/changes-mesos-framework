#!/usr/bin/env python

import logging
import subprocess
import sys
import threading
import time

import mesos
import mesos_pb2

class HTTPProxyExecutor(mesos.Executor):
  def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
    """
      Invoked once the executor driver has been able to successfully connect
      with Mesos.  In particular, a scheduler can pass some data to its
      executors through the FrameworkInfo.ExecutorInfo's data field.
    """
    logging.warn("Registered with executor ID %s" % executorInfo.executor_id)

  def reregistered(self, driver, slaveInfo):
    """
      Invoked when the executor re-registers with a restarted slave.
    """
    logging.warn("Registered with new slave")

  def disconnected(self, driver):
    """
      Invoked when the executor becomes "disconnected" from the slave (e.g.,
      the slave is being restarted due to an upgrade).
    """
    logging.warn("Disconnected from slave")

  def launchTask(self, driver, task):
    """
      Invoked when a task has been launched on this executor (initiated via
      Scheduler.launchTasks).  Note that this task can be realized with a
      thread, a process, or some simple computation, however, no other
      callbacks will be invoked on this executor until this callback has
      returned.
    """
    # Create a thread to run the task. Tasks should always be run in new
    # threads or processes, rather than inside launchTask itself.
    def run_task():
      logging.info("Running task %s" % task.task_id.value)
      update = mesos_pb2.TaskStatus()
      update.task_id.value = task.task_id.value
      update.state = mesos_pb2.TASK_RUNNING
      update.data = ''
      driver.sendStatusUpdate(update)

      # This is where one would perform the requested task.
      status = subprocess.call(["/bin/sleep", "30"])

      logging.info("Task %s finished" % task.task_id.value)
      update = mesos_pb2.TaskStatus()
      update.task_id.value = task.task_id.value
      update.state = mesos_pb2.TASK_FINISHED
      update.data = ''
      driver.sendStatusUpdate(update)

    thread = threading.Thread(target=run_task)
    thread.start()

  def killTask(self, driver, taskId):
    """
      Invoked when a task running within this executor has been killed (via
      SchedulerDriver.killTask).  Note that no status update will be sent on
      behalf of the executor, the executor is responsible for creating a new
      TaskStatus (i.e., with TASK_KILLED) and invoking ExecutorDriver's
      sendStatusUpdate.
    """
    logging.warn("Kill task called on: %s" % taskId.value)

  def frameworkMessage(self, driver, message):
    """
      Invoked when a framework message has arrived for this executor.  These
      messages are best effort; do not expect a framework message to be
      retransmitted in any reliable fashion.
    """
    # Send it back to the scheduler.
    driver.sendFrameworkMessage(message)

  def shutdown(self, driver):
    """
      Invoked when the executor should terminate all of its currently
      running tasks.  Note that after Mesos has determined that an executor
      has terminated any tasks that the executor did not send terminal
      status updates for (e.g., TASK_KILLED, TASK_FINISHED, TASK_FAILED,
      etc) a TASK_LOST status update will be created.
    """
    logging.warn("Shutdown called")

  def error(self, driver, message):
    """
      Invoked when a fatal error has occured with the executor and/or
      executor driver.  The driver will be aborted BEFORE invoking this
      callback.
    """
    logging.error("Error from Mesos: %s" % message)


if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG)

  driver = mesos.MesosExecutorDriver(HTTPProxyExecutor())
  sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
