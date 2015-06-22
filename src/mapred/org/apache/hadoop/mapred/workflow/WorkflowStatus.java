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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Describes the current status of a workflow.
 */
public class WorkflowStatus implements Writable {

  public enum RunState {
    PREP, SUBMITTED, RUNNING, SUCCEEDED, FAILED, KILLED
  };

  // Variables
  private WorkflowID workflowId;
  private RunState runState = RunState.PREP;
  private String failureInfo = "NA";
  private long submissionTime = -1;

  private Set<String> prepJobs;
  private Set<String> submittedJobs;
  private Set<String> runningJobs;
  private Set<String> finishedJobs;  // Equal to succeeded jobs.

  // Required for readFields()/reflection when building the object.
  public WorkflowStatus() {}

  public WorkflowStatus(WorkflowID workflowId) {
    this.workflowId = workflowId;

    this.prepJobs = new HashSet<String>();
    this.submittedJobs = new HashSet<String>();
    this.runningJobs = new HashSet<String>();
    this.finishedJobs = new HashSet<String>();
  }

  /**
   * Add a job to the list of jobs which have not yet been submitted.
   *
   * @param jobId A string representing the JobID of the job to add.
   */
  public void addPrepJob(String jobId) {
    prepJobs.add(jobId);
  }

  // TODO: job submitted for a workflow which actually doesn't contain it..
  // TODO: for all of them.. what if job is in the wrong state?
  // TODO: any need to be synchronized, and with what lock?
  public void addSubmittedJob(String jobId) {
    prepJobs.remove(jobId);
    submittedJobs.add(jobId);
    runState = RunState.SUBMITTED;
  }

  public void addRunningJob(String jobId) {
    submittedJobs.remove(jobId);
    runningJobs.add(jobId);
    runState = RunState.RUNNING;
  }

  public void addFinishedJob(String jobId) {
    runningJobs.remove(jobId);
    finishedJobs.add(jobId);

    if (isFinished()) {
      runState = RunState.SUCCEEDED;
    }
  }

  public boolean isFinished() {
    return prepJobs.isEmpty() && submittedJobs.isEmpty()
        && runningJobs.isEmpty();
  }

  /**
   * Get the run state of the workflow.
   *
   * @return The current run state of the workflow.
   */
  public RunState getRunState() {
    return runState;
  }

  /**
   * Get the submission time of the workflow.
   *
   * @return The submission time of the workflow.
   */
  public synchronized long getSubmissionTime() {
    return submissionTime;
  }

  /**
   * Set the submission time of the workflow.
   *
   * @param submissionTime The submission time to be set.
   */
  public synchronized void setSubmissionTime(long submissionTime) {
    if (runState == RunState.PREP) {
      this.submissionTime = submissionTime;
      runState = RunState.SUBMITTED;
    }
  }

  /**
   * Gets any available information regarding the reason of job failure.
   *
   * @return A string containing diagnostic information on job failure.
   */
  public synchronized String getFailureInfo() {
    // TODO: ? call to get failure info of current job.
    return failureInfo;
  }

  /**
   * Set the information regarding the reason of job failure.
   *
   * @param failureInfo A string containing diagnostic information on job
   *          failure.
   */
  public synchronized void setFailureInfo(String failureInfo) {
    this.failureInfo = failureInfo;
  }

  /**
   * Return the {@link WorkflowID} of the workflow.
   */
  public WorkflowID getWorkflowId() {
    return workflowId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    workflowId.write(out);
    Text.writeString(out, failureInfo);
    out.writeLong(submissionTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    workflowId = new WorkflowID();
    workflowId.readFields(in);
    failureInfo = Text.readString(in);
    submissionTime = in.readLong();
  }

}