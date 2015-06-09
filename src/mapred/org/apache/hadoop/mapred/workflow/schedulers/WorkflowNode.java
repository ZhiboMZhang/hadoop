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

/**
 * Represent a node/stage in a {@link WorkflowDAG}.
 */
public class WorkflowNode {

  private String name;
  private String job;
  private boolean isMapStage = true;
  private String machineTypes[];

  public WorkflowNode(String job, int numTasks, boolean isMapStage) {
    this.job = job;
    this.isMapStage = isMapStage;

    this.name = job + (isMapStage ? ".map" : ".red");
    this.machineTypes = new String[numTasks];
  }

  public String getName() {
    return name;
  }

  public String getJob() {
    return job;
  }

  public boolean isMapStage() {
    return isMapStage;
  }

  public int getNumTasks() {
    return machineTypes.length;
  }

  /**
   * Get the machine type of a given task (identified by a value 0 -> numTasks).
   * 
   * @param taskNumber The task in the stage to get a machine type for.
   * 
   * @return The string identifying the machine type, or null if an invalid task
   *         value was input.
   */
  public String getMachineType(int taskNumber) {
    if (taskNumber < 0 || taskNumber >= machineTypes.length) {
      return null;
    }
    return machineTypes[taskNumber];
  }

  /**
   * Set the machine type of the given task (identified by a value 0 ->
   * numTasks).
   * 
   * @param taskNumber The task in the stage to set a machine type for.
   * @param machineType The name of the machine type to use for the task.
   * 
   * @return True if the machine type was set, false otherwise.
   */
  public boolean setMachineType(int taskNumber, String machineType) {
    if (taskNumber >= 0 && taskNumber < machineTypes.length) {
      machineTypes[taskNumber] = machineType;
      return true;
    }
    return false;
  }

}