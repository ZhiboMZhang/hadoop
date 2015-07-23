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
package org.apache.hadoop.mapred.workflow.scheduling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Represent a node/job in a {@link WorkflowDAG}.
 */
public class WorkflowNode implements Writable {

  private String jobName;
  private List<WorkflowTask> mapTasks;
  private List<WorkflowTask> redTasks;

  /**
   * Only to be used when calling readFields() afterwards.
   */
  public WorkflowNode() {
    mapTasks = new ArrayList<WorkflowTask>();
    redTasks = new ArrayList<WorkflowTask>();
  }

  /**
   * Create a WorkflowNode.
   *
   * In a WorkflowDAG, WorkflowNodes represent the jobs to be executed.
   *
   * @param job The unique job name.
   * @param numMapTasks The number of map tasks that the job is to execute.
   * @param numRedTasks The number of reduce tasks that the job is to execute.
   */
  public WorkflowNode(String job, int numMapTasks, int numRedTasks) {
    this();

    this.jobName = job;
    for (int i = 0; i < numMapTasks; i++) {
      mapTasks.add(new WorkflowTask(this, true));
    }
    for (int i = 0; i < numRedTasks; i++) {
      redTasks.add(new WorkflowTask(this, false));
    }
  }

  /**
   * Return the name of the job this workflow node represents.
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * Return a collection of the map tasks belonging to this job/node.
   */
  public Collection<WorkflowTask> getMapTasks() {
    return mapTasks;
  }

  /**
   * Return a collection of reduce tasks belonging to this job/node.
   */
  public Collection<WorkflowTask> getReduceTasks() {
    return redTasks;
  }

  /**
   * Return a read-only view (membership changes have no affect on the
   * underlying workflow job) of the tasks belonging to this job/node.
   */
  public Collection<WorkflowTask> getTasks() {
    Collection<WorkflowTask> tasks = new HashSet<WorkflowTask>();
    tasks.addAll(mapTasks);
    tasks.addAll(redTasks);

    return tasks;
  }

  /**
   * Return the number of tasks this job/node has.
   */
  public int getNumTasks() {
    return mapTasks.size() + redTasks.size();
  }

  @Override
  public String toString() {
    return jobName + ": " + Arrays.toString(getTasks().toArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    jobName = Text.readString(in);

    int numTasks = in.readInt();
    for (int i = 0; i < numTasks; i++) {
      WorkflowTask task = new WorkflowTask();
      task.readFields(in);
      if (task.isMapTask()) { mapTasks.add(task); } else { redTasks.add(task); }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, jobName);

    out.writeInt(getNumTasks());
    for (WorkflowTask task : getTasks()) {
      task.write(out);
    }
  }

}