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

import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobInProgressListener;
import org.apache.hadoop.mapred.workflow.WorkflowChangeEvent;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.WorkflowInProgressListener;
import org.apache.hadoop.mapred.JobChangeEvent;

public class FifoWorkflowListener extends JobInProgressListener implements
    WorkflowInProgressListener {

  @Override
  public void workflowAdded(WorkflowInProgress workflow) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void workflowRemoved(WorkflowInProgress workflow) {
    // TODO Auto-generated method stub

  }

  @Override
  public void workflowUpdated(WorkflowChangeEvent event) {
    // TODO Auto-generated method stub

  }

  @Override
  public void jobAdded(JobInProgress job) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void jobRemoved(JobInProgress job) {
    // TODO Auto-generated method stub

  }

  @Override
  public void jobUpdated(JobChangeEvent event) {
    // TODO Auto-generated method stub

  }

}