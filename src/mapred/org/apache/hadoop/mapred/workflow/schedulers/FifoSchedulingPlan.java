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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.MachineType;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
import org.apache.hadoop.mapred.workflow.WorkflowConf;

/**
 * A basic workflow scheduling plan, schedules jobs & their tasks in a first-in
 * first-out manner.
 */
// TODO
public class FifoSchedulingPlan extends SchedulingPlan {

  public static final Log LOG = LogFactory.getLog(FifoSchedulingPlan.class);

  // -> It is sufficient to assume that all tasks have the same execution time
  // (which is given).. which should be close anyway as input splits would have
  // more or less the same amount of data.
  private List<String> taskPriorities;
  private Map<String, MachineType> nodeMachineType;
  private Map<MachineType, Set<ResourceStatus>> machineTypeNode;

  @Override
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) {

    LOG.info("In FairScheduler.class generatePlan() function");

    LOG.info("Machine types:");
    for (MachineType mType : machineTypes) {
      LOG.info("Machine " + mType.getName() + " has:");
      LOG.info("Num processors: " + mType.getNumProcessors());
      LOG.info("Cpu Frequency: " + mType.getCpuFrequency());
      LOG.info("Total Memory: " + mType.getTotalPhysicalMemory());
      LOG.info("Total Disk Space: " + mType.getAvailableSpace());
      LOG.info("Charge Rate: " + mType.getChargeRate());
    }

    LOG.info("Machines:");
    for (String machine : machines.keySet()) {
      ResourceStatus machineStatus = machines.get(machine);
      LOG.info("Machine " + machine + " has:");
      LOG.info("Num processors: " + machineStatus.getNumProcessors());
      LOG.info("Cpu Frequency: " + machineStatus.getCpuFrequency());
      LOG.info("Total Memory: " + machineStatus.getTotalPhysicalMemory());
      LOG.info("Total Disk Space: " + machineStatus.getAvailableSpace());
      LOG.info("Map slots: " + machineStatus.getMaxMapSlots());
      LOG.info("Reduce slots: " + machineStatus.getMaxReduceSlots());
    }

    LOG.info("Time Price Table:");
    for (TableKey key : table.keySet()) {
      TableEntry entry = table.get(key);
      LOG.info(entry.jobName + "/" + entry.machineTypeName + "/"
          + (entry.isMapTask ? "map" : "red") + ": " + entry.execTime
          + "seconds, $" + entry.cost);
    }

    return true;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }
}