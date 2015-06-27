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

public class WorkflowTask implements Writable {
  
  private String job;
  private boolean isMapTask;
  private String machineType = null;

  /**
   * Only to be used when calling readFields() afterwards.
   */
  public WorkflowTask() {}

  /**
   * Construct a workflow task object.
   *
   * @param job The {@link WorkflowNode} this task belongs to.
   * @param isMapTask Whether this task is a map or reduce task.
   */
  public WorkflowTask(WorkflowNode job, boolean isMapTask) {
    this.job = job.getJobName();
    this.isMapTask = isMapTask;
  }

  /**
   * Return whether the task is a map task or not (a reduce task).
   */
  public boolean isMapTask() {
    return isMapTask;
  }

  /**
   * Get the machine type of the task.
   *
   * @return The machine type name, or null if the type has not yet been set.
   */
  public String getMachineType() {
    return machineType;
  }

  /**
   * Set the machine type of the task.
   *
   * @param machineType The name of the machine type to use for the task.
   */
  public void setMachineType(String machineType) {
    this.machineType = machineType;
  }

  /**
   * Return the name of the job that this task belongs to.
   */
  public String getJobName() {
    return job;
  }

  /**
   * Get the unique identifier for the task.
   * 
   * Returns a string of the pattern: 'jobName.taskType', where jobName is the
   * name of the job that this task belongs to, and taskType is either 'map' or
   * 'reduce'.
   */
  public String getName() {
    return job + "." + toString();
  }

  @Override
  public String toString() {
    return (isMapTask ? "map" : "red");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    job = Text.readString(in);
    machineType = Text.readString(in);
    isMapTask = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, job);
    Text.writeString(out, machineType);
    out.writeBoolean(isMapTask);
  }

}