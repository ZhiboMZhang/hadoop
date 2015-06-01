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

import org.apache.hadoop.mapred.JobTracker;

/**
 * A listener for changes in a {@link WorkflowInProgress workflow}'s lifecycle
 * in the @ link JobTracker} .
 */
public interface WorkflowInProgressListener {

  /**
   * Invoked when a new workflow has been added to the {@link JobTracker}.
   * 
   * @param workflow The added workflow.
   * @throws IOException
   */
  public void workflowAdded(WorkflowInProgress workflow) throws IOException;

  /**
   * Invoked when a workflow has been removed from the {@link JobTracker}.
   * 
   * @param workflow The removed workflow.
   */
  public void workflowRemoved(WorkflowInProgress workflow);

  /**
   * Invoked when a workflow has been updated in the {@link JobTracker}. This
   * change in the workflow is tracked using a {@link WorkflowChangeEvent}.
   * 
   * @param event the event that tracks the change.
   */
  public void workflowUpdated(WorkflowChangeEvent event);
}