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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Represent a node/stage in a {@link WorkflowDAG}.
 */
public class WorkflowNode implements Writable {

  private String name;
  private String job;
  private boolean isMapStage = true;
  private String machineTypes[];

  /**
   * Only to be used when calling readFields() afterwards.
   */
  public WorkflowNode() {}

  /**
   * Create a WorkflowNode.
   *
   * In a WorkflowDAG, WorkflowNodes represent the map/reduce stages of a job.
   *
   * @param job The name of the job.
   * @param numTasks The number of tasks that the job is to execute.
   * @param isMapStage Whether the tasks are map or reduce tasks.
   */
  public WorkflowNode(String job, int numTasks, boolean isMapStage) {
    this.job = job;
    this.isMapStage = isMapStage;

    this.name = job + (isMapStage ? ".map" : ".red");
    this.machineTypes = new String[numTasks];
  }

  @Deprecated
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
   * @return The machine type name, or null if an invalid task value was input.
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

  @Override
  public int hashCode() {
    return (name.hashCode() * job.hashCode()) + (isMapStage ? 1 : 0);
  }

  @Override
  public boolean equals(Object obj) {

    if (obj == null) { return false; }
    if (this == obj) { return true; }
    if (getClass() != obj.getClass()) { return false; }

    final WorkflowNode other = (WorkflowNode) obj;

    if (machineTypes.length != other.machineTypes.length) { return false; }
    for (int i = 0; i < machineTypes.length; i++) {
      if (!machineTypes[i].equals(other.machineTypes[i])) { return false; }
    }

    if (name.equals(other.name) && job.equals(other.job)
        && isMapStage == other.isMapStage) {
      return true;
    }

    return false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    name = Text.readString(in);
    job = Text.readString(in);
    isMapStage = in.readBoolean();

    machineTypes = new String[in.readInt()];
    for (int i = 0; i < machineTypes.length; i++) {
      machineTypes[i] = Text.readString(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, job);
    out.writeBoolean(isMapStage);

    out.writeInt(machineTypes.length);
    for (String type : machineTypes) {
      Text.writeString(out, type);
    }
  }

}