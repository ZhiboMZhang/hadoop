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
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Clock;

/**
 * Describes the current status of a workflow.
 */
public class WorkflowStatus implements Writable {

  private static final Log LOG = LogFactory.getLog(WorkflowStatus.class);

  public enum RunState {
    PREP, SUBMITTED, RUNNING, SUCCEEDED, FAILED, KILLED
  };

  // Variables
  private WorkflowID workflowId;
  private RunState runState = RunState.PREP;
  private String failureInfo = "NA";

  private Clock clock;
  private long startTime = -1L;
  private long finishTime = -1L;

  private Collection<String> prepJobs;
  private Collection<String> submittedJobs;
  private Collection<String> runningJobs;
  private Collection<String> finishedJobs;  // Equal to succeeded jobs.

  // Required for readFields()/reflection when building the object.
  public WorkflowStatus() {
    prepJobs = new HashSet<String>();
    submittedJobs = new HashSet<String>();
    runningJobs = new HashSet<String>();
    finishedJobs = new HashSet<String>();
    clock = new Clock();
  }

  public WorkflowStatus(WorkflowID workflowId, Clock clock) {
    this();
    this.workflowId = workflowId;
    this.clock = clock;
    this.startTime = clock.getTime();
  }

  // TODO: pass in workflowConf on construction, use it to block adding of
  // jobs which aren't in the workflow configuration?

  /**
   * Add a job to the list of jobs which have not yet been submitted.
   *
   * @param jobName A string representing the name of the job to add.
   */
  public synchronized void addPrepJob(String jobName) {
    prepJobs.add(jobName);
    LOG.info("Added job to prepJobs. " + jobName);
  }

  /**
   * Add a job to the list of jobs which have been submitted but are not yet
   * running.
   *
   * @param jobName A string representing the name of the job to add.
   */
  public synchronized void addSubmittedJob(String jobName) {
    boolean success = prepJobs.remove(jobName);
    if (success) { submittedJobs.add(jobName); }
    LOG.info((success ? "Added" : "Did not add") + " job to submittedJobs. "
        + jobName);

    if (runState == RunState.PREP) {
      runState = RunState.SUBMITTED;
      LOG.info("Updating workflow status to submitted.");
    }
  }

  /** Add a job to the list of jobs which are running but are not yet finished.
   *
   * @param jobName A string representing the name of the job to add.
   */
  public synchronized void addRunningJob(String jobName) {
    boolean success = submittedJobs.remove(jobName);
    if (success) { runningJobs.add(jobName); }
    LOG.info((success ? "Added" : "Did not add") + " job to runningJobs. "
        + jobName);

    if (runState == RunState.SUBMITTED) {
      runState = RunState.RUNNING;
      LOG.info("Updating workflow status to running.");
    }
  }

  /**
   * Add a job to the list of jobs which have finished running (successfully).
   *
   * @param jobName A string representing the name of the job to add.
   */
  public synchronized void addFinishedJob(String jobName) {
    boolean success = runningJobs.remove(jobName);
    if (success) { finishedJobs.add(jobName); }
    LOG.info((success ? "Added" : "Did not add") + " job to finishedJobs. "
        + jobName);

    if (isFinished()) {
      runState = RunState.SUCCEEDED;
      finishTime = clock.getTime();
    }
  }

  /**
   * Return a collection of jobs which are in the preparation stage.
   *
   * @return A collection of jobs, identified by their job name.
   */
  public synchronized Collection<String> getPrepJobs() {
    return new HashSet<String>(prepJobs);
  }

  /**
   * Return a collection of jobs which have been submitted.
   *
   * @return A collection of jobs, identified by their job name.
   */
  public synchronized Collection<String> getSubmittedJobs() {
    return new HashSet<String>(submittedJobs);
  }

  /**
   * Return a collection of jobs which are currently running.
   *
   * @return A collection of jobs, identified by their job name.
   */
  public synchronized Collection<String> getRunningJobs() {
    return new HashSet<String>(runningJobs);
  }

  /**
   * Return a collection of finished jobs.
   *
   * @return A collection of finished jobs, identified by their job name.
   */
  public synchronized Collection<String> getFinishedJobs() {
    return new HashSet<String>(finishedJobs);
  }

  /**
   * Report whether the workflow is finished.
   *
   * @return True if the workflow is finished, false otherwise.
   */
  public synchronized boolean isFinished() {
    return prepJobs.isEmpty() && submittedJobs.isEmpty() && runningJobs.isEmpty();
  }

  /**
   * Get the run state of the workflow.
   *
   * @return The current run state of the workflow.
   */
  public synchronized RunState getRunState() {
    return runState;
  }

  /**
   * Get the submission time of the workflow.
   *
   * @return The submission time of the workflow.
   */
  public synchronized long getStartTime() {
    return startTime;
  }

  /**
   * Return the time when the workflow completed, as measured when the last job
   * finished.
   */
  public synchronized long getFinishTime() {
    return finishTime;
  }

  /**
   * Gets any available information regarding the reason of job failure.
   *
   * @return A string containing diagnostic information on job failure.
   */
  public synchronized String getFailureInfo() {
    // TODO: Add a call to get failure info of current jobs.
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
    Text.writeString(out, runState.name());
    Text.writeString(out, failureInfo);

    clock.write(out);
    out.writeLong(startTime);
    out.writeLong(finishTime);

    writeJobSet(out, prepJobs);
    writeJobSet(out, submittedJobs);
    writeJobSet(out, runningJobs);
    writeJobSet(out, finishedJobs);
  }

  private void writeJobSet(DataOutput out, Collection<String> jobs)
      throws IOException {
    out.writeInt(jobs.size());
    for (String job : jobs) {
      Text.writeString(out, job);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    workflowId = new WorkflowID();
    workflowId.readFields(in);
    runState = RunState.valueOf(Text.readString(in));
    failureInfo = Text.readString(in);

    clock.readFields(in);
    startTime = in.readLong();
    finishTime = in.readLong();

    readJobSet(in, prepJobs);
    readJobSet(in, submittedJobs);
    readJobSet(in, runningJobs);
    readJobSet(in, finishedJobs);
  }

  private void readJobSet(DataInput in, Collection<String> jobs)
      throws IOException {
    int numJobs = in.readInt();
    for (int i = 0; i < numJobs; i++) {
      jobs.add(Text.readString(in));
    }
  }

}