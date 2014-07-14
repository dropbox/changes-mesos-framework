#!/usr/bin/env python

import os
import sys
import time

import mesos
import mesos_pb2

TOTAL_TASKS = 5

TASK_CPUS = 1
TASK_MEM = 32

class ChangesScheduler(mesos.Scheduler):
  def __init__(self, executor):
    self.executor = executor
    self.taskData = {}
    self.tasksLaunched = 0
    self.tasksFinished = 0
    self.messagesSent = 0
    self.messagesReceived = 0

  def registered(self, driver, frameworkId, masterInfo):
    print "Registered with framework ID %s" % frameworkId.value

  def reregistered(self, driver, masterInfo):
    print "Re-Registered with new master"

  def disconnected(self, driver):
    print "Disconnected from master"

  def resourceOffers(self, driver, offers):
    print "Got %d resource offers" % len(offers)
    for offer in offers:
      tasks = []
      print "Got resource offer %s" % offer.id.value
      if self.tasksLaunched < TOTAL_TASKS:
        tid = self.tasksLaunched
        self.tasksLaunched += 1

        print "Accepting offer on %s to start task %d" \
            % (offer.hostname, tid)

        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % tid
        task.executor.MergeFrom(self.executor)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        tasks.append(task)
        self.taskData[task.task_id.value] = (
          offer.slave_id, task.executor.executor_id)
      driver.launchTasks(offer.id, tasks)

  def offerRescinded(self, driver, offerId):
    print "Offer rescinded: %s" % offerId.value

  def statusUpdate(self, driver, update):
    print "Task %s is in state %d" % (update.task_id.value, update.state)

    # Ensure the binary data came through.
    if update.data != "data with a \0 byte":
      print "The update data did not match!"
      print "  Expected: 'data with a \\x00 byte'"
      print "  Actual:  ", repr(str(update.data))
      sys.exit(1)

    if update.state == mesos_pb2.TASK_FINISHED:
      self.tasksFinished += 1
      if self.tasksFinished == TOTAL_TASKS:
        print "All tasks done, waiting for final framework message"

      slave_id, executor_id = self.taskData[update.task_id.value]

      self.messagesSent += 1
      driver.sendFrameworkMessage(
        executor_id,
        slave_id,
        'data with a \0 byte')

  def frameworkMessage(self, driver, executorId, slaveId, message):
    self.messagesReceived += 1

    # The message bounced back as expected.
    if message != "data with a \0 byte":
      print "The returned message data did not match!"
      print "  Expected: 'data with a \\x00 byte'"
      print "  Actual:  ", repr(str(message))
      sys.exit(1)
    print "Received message:", repr(str(message))

    if self.messagesReceived == TOTAL_TASKS:
      if self.messagesReceived != self.messagesSent:
        print "Sent", self.messagesSent,
        print "but received", self.messagesReceived
        sys.exit(1)
      print "All tasks done, and all messages received, exiting"
      driver.stop()

  def slaveLost(self, driver, slaveId):
    print "Slave lost: %s" % slaveId.value

  def executorLost(self, driver, executorId, slaveId, status):
    print "Executor %s lost on slave %s" % (exeuctorId.value, slaveId.value)

  def error(self, driver, message):
    print "Error: %s" % message


if __name__ == "__main__":
  if len(sys.argv) != 2:
    print "Usage: %s master" % sys.argv[0]
    sys.exit(1)

  executor = mesos_pb2.ExecutorInfo()
  executor.executor_id.value = "default"
  executor.command.value = os.path.abspath("./executor.py")
  executor.name = "Changes Executor (Python)"
  executor.source = "python_test"

  framework = mesos_pb2.FrameworkInfo()
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "Changes Framework (Python)"

  # TODO(vinod): Make checkpointing the default when it is default
  # on the slave.
  if os.getenv("MESOS_CHECKPOINT"):
    print "Enabling checkpoint for the framework"
    framework.checkpoint = True

  if os.getenv("MESOS_AUTHENTICATE"):
    print "Enabling authentication for the framework"

    if not os.getenv("DEFAULT_PRINCIPAL"):
      print "Expecting authentication principal in the environment"
      sys.exit(1);

    if not os.getenv("DEFAULT_SECRET"):
      print "Expecting authentication secret in the environment"
      sys.exit(1);

    credential = mesos_pb2.Credential()
    credential.principal = os.getenv("DEFAULT_PRINCIPAL")
    credential.secret = os.getenv("DEFAULT_SECRET")

    framework.principal = os.getenv("DEFAULT_PRINCIPAL")

    driver = mesos.MesosSchedulerDriver(
      ChangesScheduler(executor),
      framework,
      sys.argv[1],
      credential)
  else:
    framework.principal = "test-framework-python"

    driver = mesos.MesosSchedulerDriver(
      ChangesScheduler(executor),
      framework,
      sys.argv[1])

  status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

  # Ensure that the driver process terminates.
  driver.stop();

  sys.exit(status)
