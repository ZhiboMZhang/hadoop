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

/**
 * <code>RunningWorkflow</code> is the user-interface to query for details on a
 * running Map-Reduce workflow.
 */
public interface RunningWorkflow {

  /**
   * Get the workflow identifier.
   * 
   * @return the workflow identifier.
   */
  public WorkflowID getID();

  /**
   * Get the name of the workflow.
   * 
   * @return the name of the workflow.
   */
  public String getWorkflowName();

  /**
   * Get failure information for the workflow.
   * 
   * @return the failure information for the workflow.
   * @throws IOException
   */
  public String getFailureInfo() throws IOException;

  // TODO: Maybe, hopefully.
  // public float setupProgress() throws IOException;
  // public float jobProgress() throws IOException;
  // public float mapProgress() throws IOException;
  // public float reduceProgress() throws IOException;
  // public float cleanupProgress() throws IOException;

  /**
   * Returns a snapshot of the current workflow's status.
   *
   * @return The {@link WorkflowStatus} of the current workflow.
   * @throws IOException
   */
  public WorkflowStatus getWorkflowStatus() throws IOException;
}