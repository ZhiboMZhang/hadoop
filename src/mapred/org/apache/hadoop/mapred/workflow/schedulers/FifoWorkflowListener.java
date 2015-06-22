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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobChangeEvent;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobInProgressListener;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobStatusChangeEvent;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.workflow.WorkflowChangeEvent;
import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.WorkflowInProgressListener;
import org.apache.hadoop.mapred.workflow.WorkflowStatus;

public class FifoWorkflowListener extends JobInProgressListener implements
    WorkflowInProgressListener {

  private static final Log LOG = LogFactory.getLog(FifoWorkflowListener.class);

  private Map<JobID, JobInProgress> jobs;
  private Map<WorkflowID, WorkflowInProgress> workflows;

  // A queue to hold both jobs and workflows in progress.
  private Map<SchedulingInfo, Object> queue;

  public FifoWorkflowListener() {
    jobs = new HashMap<JobID, JobInProgress>();
    workflows = new LinkedHashMap<WorkflowID, WorkflowInProgress>();
    queue = new HashMap<SchedulingInfo, Object>();
  }

  /**
   * @return A collection of JobInProgress/WorkflowInProgress objects.
   */
  public Collection<Object> getQueue() {
    return queue.values();
  }

  @Override
  public void workflowAdded(WorkflowInProgress workflow) throws IOException {
    // Keep a list of workflows to be executed.
    WorkflowID workflowId = workflow.getProfile().getWorkflowId();
    workflows.put(workflowId, workflow);
    queue.put(new WorkflowSchedulingInfo(workflow.getStatus()), workflow);

    LOG.info("Added workflow " + workflowId + " to queue.");
  }

  @Override
  public void workflowRemoved(WorkflowInProgress workflow) {
    // Workflow will be removed once it completes.
  }

  @Override
  public void workflowUpdated(WorkflowChangeEvent event) {
    // TODO: Workflow updating currently not handled.
  }

  @Override
  public void jobAdded(JobInProgress job) throws IOException {

    LOG.info("Job added to listener queue.");

    // Add the job to the queue, and list of jobs.
    queue.put(new JobSchedulingInfo(job.getStatus()), job);
    jobs.put(job.getJobID(), job);

    // Update the workflow status.
    WorkflowID workflowId = job.getStatus().getWorkflowId();
    if (workflowId != null) {
      LOG.info("Added job is a workflow job.");
      WorkflowInProgress workflow = workflows.get(workflowId);
      workflow.getStatus().addSubmittedJob(job.getJobID().toString());
    }
  }

  @Override
  public void jobRemoved(JobInProgress job) {
    // Job will be removed once it completes.
  }

  @Override
  // Most code is taken from JobQueueJobInProgressListener.
  public void jobUpdated(JobChangeEvent event) {

    LOG.info("In jobUpdated function.");
    if (event instanceof JobStatusChangeEvent) {
      JobStatusChangeEvent statusEvent = (JobStatusChangeEvent) event;

      if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
        int runState = statusEvent.getNewStatus().getRunState();

        if (JobStatus.SUCCEEDED == runState || JobStatus.FAILED == runState
            || JobStatus.KILLED == runState) {

          JobStatus oldStatus = statusEvent.getOldStatus();
          JobSchedulingInfo info = new JobSchedulingInfo(oldStatus);
          JobInProgress job = event.getJobInProgress();
          WorkflowID workflowId = job.getStatus().getWorkflowId();
          LOG.info("WORKFLOW: jobUpdated: workflowId is: " + workflowId);

          // The job is finished, so either way remove it from the queue.
          queue.remove(info);
          jobs.remove(info.getJobId());

          // Job belongs to a workflow, remove it from the queue.
          if (workflowId != null) {

            // Update the workflow status.
            WorkflowInProgress workflow = workflows.get(workflowId);
            WorkflowStatus workflowStatus = workflow.getStatus();

            workflowStatus.addFinishedJob(info.getJobId().toString());

            // Check the workflow status.
            if (workflow.getStatus().isFinished()) {
              queue.remove(new WorkflowSchedulingInfo(workflowStatus));
              workflows.remove(workflowId);
            }
          }
        }
      }
    }
  }

}