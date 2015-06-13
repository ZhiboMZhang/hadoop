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

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;

/**
 * A {@link WorkflowSchedulingProtocol} allows its {@link SchedulingPlan} to be
 * set and retrieved.
 */
public interface WorkflowSchedulingProtocol extends VersionedProtocol {

  public static long versionID = 1L;

  /**
   * Set the {@link SchedulingPlan} of the protocol.
   *
   * @param schedulingPlan The scheduling plan to set.
   */
  public void setWorkflowSchedulingPlan(SchedulingPlan schedulingPlan);

  /**
   * Get the {@link SchedulingPlan} of the protocol.
   *
   * @return The scheduling plan.
   */
  public SchedulingPlan getWorkflowSchedulingPlan();

}