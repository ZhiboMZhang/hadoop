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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.EagerTaskInitializationListener;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskScheduler;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

public class OptimalWorkflowScheduler extends TaskScheduler implements
    WorkflowScheduler {

  private static final Log LOG = LogFactory.getLog(OptimalWorkflowScheduler.class);

  private WorkflowListener workflowListener;
  private EagerTaskInitializationListener eagerTaskInitializationListener;

  private WorkflowSchedulingProtocol workflowSchedulingProtocol;
  private SchedulingPlan schedulingPlan;

  public OptimalWorkflowScheduler() {
    workflowListener = new WorkflowListener();
  }

  @Override
  public synchronized void start() throws IOException {
    super.start();
    taskTrackerManager.addJobInProgressListener(workflowListener);
    taskTrackerManager.addWorkflowInProgressListener(workflowListener);

    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(eagerTaskInitializationListener);
  }

  @Override
  public synchronized void terminate() throws IOException {
    if (workflowListener != null) {
      taskTrackerManager.removeJobInProgressListener(workflowListener);
      taskTrackerManager.removeWorkflowInProgressListener(workflowListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    eagerTaskInitializationListener = new EagerTaskInitializationListener(conf);
  }

  @Override
  public synchronized void setWorkflowSchedulingProtocol(
      WorkflowSchedulingProtocol workflowSchedulingProtocol) {
    this.workflowSchedulingProtocol = workflowSchedulingProtocol;
  }

  @Override
  public List<Task> assignTasks(TaskTracker taskTracker) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String ignored) {

    // Both JobInProgress and WorkflowInProgress objects exist in the default
    // queue. Filter the queue to a Collection of JobInProgress objects.
    Collection<Object> queue = workflowListener.getQueue();
    Collection<JobInProgress> jobQueue = new ArrayList<JobInProgress>();

    for (Object object : queue) {
      if (object instanceof JobInProgress) {
        jobQueue.add((JobInProgress) object);
      }
    }

    return jobQueue;
  }

}