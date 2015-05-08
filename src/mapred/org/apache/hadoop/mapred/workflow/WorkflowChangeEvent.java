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

/**
 * {@link WorkflowChangeEvent} is used to capture state changes in a workflow. A
 * workflow can change its state wrt/ priority, progress, run-state, &c.
 */
public abstract class WorkflowChangeEvent {

  private WorkflowInProgress wip;

  WorkflowChangeEvent(WorkflowInProgress wip) {
    this.wip = wip;
  }

  /**
   * Get the workflow object for which the change is reported.
   */
  WorkflowInProgress getWorkflowInProgress() {
    return wip;
  }
}