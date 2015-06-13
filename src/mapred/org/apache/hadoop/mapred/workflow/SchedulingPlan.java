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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
import org.apache.hadoop.mapred.workflow.schedulers.WorkflowUtil.MachineTypeJobNamePair;

public abstract class SchedulingPlan implements Writable {

  /**
   * Generate a scheduling plan.
   * 
   * The scheduling plan consists of a mapping between
   * 
   * @param machineTypes The available hardware configurations.
   * @param machines The actual machines present in the cluster.
   * @param table The time-price table to use.
   * @param workflow The workflow to be run.
   * 
   * @return
   */
  public abstract boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow);

  /**
   * Get an ordered list of tasks to be executed, each task specified by a
   * machine type and job name pair.
   */
  public abstract List<MachineTypeJobNamePair> getTaskMapping();

  /**
   * Get the mapping from actual available machines to machine types.
   */
  public abstract Map<String, String> getTrackerMapping();

  @Override
  public abstract void readFields(DataInput in) throws IOException;

  @Override
  public abstract void write(DataOutput out) throws IOException;
}