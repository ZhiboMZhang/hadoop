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
package org.apache.hadoop.mapred.workflow;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.workflow.WorkflowStatus.RunState;

/**
 * WorkflowInProgress maintains all the info for keeping a Workflow valid. It
 * keeps a {@link WorkflowProfile} and a {@link WorkflowStatus}, in addition to
 * other data for bookkeeping of its Jobs.
 */
public class WorkflowInProgress {

  /**
   * Used when the kill signal is issued to a workflow which is initializing.
   */
  @SuppressWarnings("serial")
  static class KillInterruptedException extends InterruptedException {
    public KillInterruptedException(String msg) {
      super(msg);
    }
  }

  static final Log LOG = LogFactory.getLog(WorkflowInProgress.class);

  private WorkflowProfile profile;
  private WorkflowStatus status;
  private WorkflowID workflowId;
  private WorkflowConf workflowConf;

  Set<JobInProgress> runningJobs;
  Set<JobInProgress> completedJobs;

  public WorkflowInProgress(JobTracker jobTracker, WorkflowConf workflowConf,
      WorkflowInfo workflowInfo) {

    this.workflowConf = workflowConf;
    workflowId = workflowInfo.getWorkflowId();
    profile = new WorkflowProfile(workflowId, workflowConf.getWorkflowName());
    status = new WorkflowStatus(workflowId);
    status.setSubmissionTime(jobTracker.getClock().getTime());

    runningJobs = new HashSet<JobInProgress>();
    completedJobs = new HashSet<JobInProgress>();
  }

  public WorkflowStatus getStatus() {
    return status;
  }

  public WorkflowProfile getProfile() {
    return profile;
  }

}