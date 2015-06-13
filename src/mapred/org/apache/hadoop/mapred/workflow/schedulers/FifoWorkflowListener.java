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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobChangeEvent;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobInProgressListener;
import org.apache.hadoop.mapred.workflow.WorkflowChangeEvent;
import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.WorkflowInProgressListener;

public class FifoWorkflowListener extends JobInProgressListener implements
    WorkflowInProgressListener {

  private static class SchedulingInfo {

  }

  private static final Log LOG = LogFactory.getLog(FifoWorkflowListener.class);

  private Map<JobID, JobInProgress> jobs;
  private Map<WorkflowID, WorkflowInProgress> workflows;

  // A queue to hold both jobs and workflows in progress.
  private Map<SchedulingInfo, Object> queue;

  public FifoWorkflowListener() {
    LOG.info("In constructor.");
    jobs = new HashMap<JobID, JobInProgress>();
    workflows = new HashMap<WorkflowID, WorkflowInProgress>();
    queue = new HashMap<SchedulingInfo, Object>();
  }

  @Override
  public void workflowAdded(WorkflowInProgress workflow) throws IOException {
    LOG.info("In workflowAdded function.");
  }

  @Override
  public void workflowRemoved(WorkflowInProgress workflow) {
    LOG.info("In workflowRemoved function.");
  }

  @Override
  public void workflowUpdated(WorkflowChangeEvent event) {
    LOG.info("In workflowUpdated function.");
  }

  @Override
  public void jobAdded(JobInProgress job) throws IOException {
    LOG.info("In jobAdded function.");
  }

  @Override
  public void jobRemoved(JobInProgress job) {
    LOG.info("In jobRemoved function.");
  }

  @Override
  public void jobUpdated(JobChangeEvent event) {
    LOG.info("In jobUpdated function.");
  }

}