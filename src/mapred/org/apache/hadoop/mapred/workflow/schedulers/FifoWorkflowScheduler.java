/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.mapred.workflow.schedulers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskScheduler;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;
import org.apache.hadoop.mapred.workflow.schedulers.WorkflowUtil.MachineTypeJobNamePair;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

public class FifoWorkflowScheduler extends TaskScheduler implements
    WorkflowScheduler {

  private static final Log LOG = LogFactory.getLog(FifoWorkflowScheduler.class);

  private FifoWorkflowListener fifoWorkflowListener;

  private WorkflowSchedulingProtocol workflowSchedulingProtocol;
  private SchedulingPlan schedulingPlan;

  public FifoWorkflowScheduler() {
    LOG.info("In constructor.");
    fifoWorkflowListener = new FifoWorkflowListener();
  }

  @Override
  public synchronized void start() throws IOException {
    super.start();
    LOG.info("In start() method.");
    taskTrackerManager.addJobInProgressListener(fifoWorkflowListener);
    taskTrackerManager.addWorkflowInProgressListener(fifoWorkflowListener);
  }

  @Override
  public synchronized void terminate() throws IOException {
    LOG.info("In terminate() method.");
    if (fifoWorkflowListener != null) {
      taskTrackerManager.removeJobInProgressListener(fifoWorkflowListener);
      taskTrackerManager.removeWorkflowInProgressListener(fifoWorkflowListener);
    }
    super.terminate();
  }

  public synchronized void setWorkflowSchedulingProtocol(
      WorkflowSchedulingProtocol workflowSchedulingProtocol) {
    this.workflowSchedulingProtocol = workflowSchedulingProtocol;
  }

  @Override
  // Called from JobTracker heartbeat function, which is called by a taskTracker
  public List<Task> assignTasks(TaskTracker taskTracker) throws IOException {
    LOG.info("In assignTasks method.");

    if (taskTrackerManager.isInSafeMode()) {
      LOG.info("JobTracker is in safe mode, not scheduling any tasks.");
      return null;
    }

    if (schedulingPlan == null) {
      schedulingPlan = workflowSchedulingProtocol.getWorkflowSchedulingPlan();
      if (schedulingPlan == null) { return null; }

      List<MachineTypeJobNamePair> taskMapping = schedulingPlan.getTaskMapping();
      Map<String, String> trackerMapping = schedulingPlan.getTrackerMapping();

      for (String type : trackerMapping.keySet()) {
        LOG.info("Mapped machinetype " + type + " to "
            + trackerMapping.get(type));
      }

      for (MachineTypeJobNamePair pair : taskMapping) {
        LOG.info("Have pair " + pair.jobName + " to " + pair.machineType);
      }

    }

    // Find out which tasktracker wants a task.
    // Match it with an available task.
    //LOG.info("Tracker " + taskTracker.getTrackerName() + " wants a task.");

    // Collection<Object> queue = fifoWorkflowListener.

    return null;
  }

  private synchronized int obtainNewMapTask() {
    return 0;
  }

  private synchronized int obtainNewReduceTask() {
    return 0;
  }

  @Override
  public Collection<JobInProgress> getJobs(String queueName) {
    LOG.info("In getJobs method.");
    return null;
  }

  // checkJobSubmission ?? (superclass method does nothing)
  // refresh ?? (superclass method does nothing)

}