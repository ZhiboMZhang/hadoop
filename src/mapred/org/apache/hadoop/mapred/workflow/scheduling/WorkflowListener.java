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
package org.apache.hadoop.mapred.workflow.scheduling;

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

public class WorkflowListener extends JobInProgressListener implements
    WorkflowInProgressListener {

  private static final Log LOG = LogFactory.getLog(WorkflowListener.class);

  private Map<JobID, JobInProgress> jobs;
  private Map<WorkflowID, WorkflowInProgress> workflows;

  // A queue to hold both jobs and workflows in progress.
  private Map<? super SchedulingInfo, Object> queue;

  public WorkflowListener() {
    jobs = new HashMap<JobID, JobInProgress>();
    workflows = new HashMap<WorkflowID, WorkflowInProgress>();
    queue = new LinkedHashMap<Object, Object>();
  }

  /**
   * @return A collection of JobInProgress/WorkflowInProgress objects.
   */
  public Collection<Object> getQueue() {
    return queue.values();
  }

  @Override
  public synchronized void workflowAdded(WorkflowInProgress workflow)
      throws IOException {
    // Keep a list of workflows to be executed.
    WorkflowID workflowId = workflow.getProfile().getWorkflowId();
    workflows.put(workflowId, workflow);
    queue.put(new WorkflowSchedulingInfo(workflow.getStatus()), workflow);

    LOG.info("Added the workflow " + workflowId + " to queue.");

    // Add all workflow jobs to WorkflowStatus in PREP state.
    for (JobConf conf : workflow.getConf().getJobs().values()) {
      workflow.getStatus().addPrepJob(conf.getJobName());
    }
  }

  @Override
  public synchronized void workflowRemoved(WorkflowInProgress workflow) {
    // Workflow will be removed once it completes (see jobUpdated).
  }

  @Override
  public synchronized void workflowUpdated(WorkflowChangeEvent event) {
    // Workflow updating is handled when jobs are updated.
  }

  @Override
  public synchronized void jobAdded(JobInProgress job) throws IOException {

    LOG.info("Job added to listener queue.");

    // Add the job to the queue, and list of jobs.
    queue.put(new JobSchedulingInfo(job.getStatus()), job);
    jobs.put(job.getJobID(), job);

    // Update the workflow status.
    WorkflowID workflowId = job.getStatus().getWorkflowId();
    if (workflowId != null) {
      LOG.info("The added job is a workflow job.");
      WorkflowInProgress workflow = workflows.get(workflowId);
      workflow.getStatus().addRunningJob(job.getJobConf().getJobName());
    }
  }

  @Override
  public synchronized void jobRemoved(JobInProgress job) {
    // Job will be removed once it completes.
  }

  @Override
  // Most code is taken from JobQueueJobInProgressListener.
  public synchronized void jobUpdated(JobChangeEvent event) {

    LOG.info("A job was updated.");

    if (event instanceof JobStatusChangeEvent) {
      JobStatusChangeEvent statusEvent = (JobStatusChangeEvent) event;

      if (statusEvent.getEventType() == EventType.RUN_STATE_CHANGED) {
        int runState = statusEvent.getNewStatus().getRunState();

        if (JobStatus.SUCCEEDED == runState || JobStatus.FAILED == runState
            || JobStatus.KILLED == runState) {

          LOG.info("The job is finished.");

          JobStatus oldStatus = statusEvent.getOldStatus();
          JobSchedulingInfo info = new JobSchedulingInfo(oldStatus);
          JobInProgress job = statusEvent.getJobInProgress();

          // The job is finished, so either way remove it from the queue.
          queue.remove(info);
          jobs.remove(info.getJobId());

          // Job belongs to a workflow, remove it from the queue.
          WorkflowID workflowId = job.getStatus().getWorkflowId();

          if (workflowId != null) {
            LOG.info("Updated job is part of a workflow. Its ID is: " + workflowId);

            // Update the workflow status.
            WorkflowInProgress workflow = workflows.get(workflowId);
            WorkflowStatus workflowStatus = workflow.getStatus();

            String jobName = job.getJobConf().getJobName();
            workflowStatus.addFinishedJob(jobName);
            LOG.info("Updated job is finised, updating workflow status for " + jobName);

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