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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;

/**
 * WorkflowInProgress maintains all the info for keeping a Workflow valid. It
 * keeps a {@link WorkflowProfile} and a {@link WorkflowStatus}, in addition to
 * other data for bookkeeping of its Jobs.
 */
public class WorkflowInProgress {

  private static final Log LOG = LogFactory.getLog(WorkflowInProgress.class);

  /**
   * Used when the kill signal is issued to a workflow which is initializing.
   */
  @SuppressWarnings("serial")
  static class KillInterruptedException extends InterruptedException {
    public KillInterruptedException(String msg) {
      super(msg);
    }
  }

  private WorkflowProfile profile;
  private WorkflowStatus status;
  private WorkflowID workflowId;
  private WorkflowConf workflowConf;

  public WorkflowInProgress(JobTracker jobTracker, WorkflowConf workflowConf,
      WorkflowInfo workflowInfo) {

    this.workflowConf = workflowConf;
    workflowId = workflowInfo.getWorkflowId();
    profile = new WorkflowProfile(workflowId, workflowConf.getWorkflowName());
    status = new WorkflowStatus(workflowId, jobTracker.getClock());
  }

  /**
   * Clean up a completed workflow.
   *
   * This function removes all temporary directories associated with the
   * workflow.
   */
  // Called from jobTracker, which is called from running/networked workflow.
  public void cleanupWorkflow() {
    synchronized (this) {

      CleanupQueue queue = CleanupQueue.getInstance();

      String workflowOutput = workflowConf.get("mapred.output.dir");

      // Remove the temporary directories created in HDFS.
      // These are the input/output directories of jobs internal to the workflow.
      // Some may be repeated, so add all paths to a set before deletion.
      Set<String> ioDirs = new HashSet<String>();
      for (JobConf conf : workflowConf.getJobs().values()) {
        String output = conf.getOutputDir();
        if (!output.equals(workflowOutput)) { ioDirs.add(output); }

        // An entry job doesn't have to have the workflowInput as its input.
        if (workflowConf.getDependencies().get(conf.getJobName()) != null) {
          ioDirs.add(conf.getInputDir());
        }
      }
      for (String dir : ioDirs) {
        queue.addToQueue(new PathDeletionContext(new Path(dir), workflowConf));
        LOG.info("Removing temporary directory '" + dir + "'.");
      }

      // Remove the workflow submission directory.
      String submitJobDir = workflowConf.get("mapreduce.workflow.dir");
      if (submitJobDir != null) {
        LOG.info("Removing workflow submit directory '" + submitJobDir + "'.");
        queue.addToQueue(
          new PathDeletionContext(new Path(submitJobDir), workflowConf));
      }
    }
  }

  public WorkflowProfile getProfile() {
    return profile;
  }

  public WorkflowStatus getStatus() {
    return status;
  }

  public WorkflowID getWorkflowId() {
    return workflowId;
  }

  public WorkflowConf getConf() {
    return workflowConf;
  }

  public long getStartTime() {
    return status.getStartTime();
  }

  public long getFinishTime() {
    return status.getFinishTime();
  }

}