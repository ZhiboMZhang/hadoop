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
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;

/**
 * A basic workflow scheduling plan, schedules jobs & their tasks in a first-in
 * first-out manner.
 */
// TODO
public class FifoSchedulingPlan extends SchedulingPlan {

  public static final Log LOG = LogFactory.getLog(FifoSchedulingPlan.class);

  // We can assume that all tasks have the same execution time (which is given).
  // Priorities list keeps a list of WorkflowNodes.
  // (corresponding to TASKS, not stages).
  private List<WorkflowNode> priorities;
  private Map<WorkflowNode, MachineType> jobToType;
  private Map<MachineType, Set<ResourceStatus>> typeToMachine;

  private List<MachineType> sortedMachineTypes;

  @Override
  /**
   * Plan generation in this case will return a basic scheduling, disregarding
   *  constraints.
   */
  // @formatter: off
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) {

    LOG.info("In FairScheduler.class generatePlan() function");
    printDebugInfo(machineTypes, machines, table);

    // Get a sorted list of machine types by cost/unit time.
    // TODO test
    sortedMachineTypes = new ArrayList<MachineType>(machineTypes);
    Collections.sort(sortedMachineTypes, WorkflowUtil.MACHINE_TYPE_COST_ORDER);

    // Find out what machines we actually have wrt/ the available machine types.
    // TODO test
    typeToMachine = WorkflowUtil.matchResourceTypes(machineTypes, machines);

    // Get the workflow DAG corresponding to the workflow configuration, &c.
    // TODO test
    WorkflowDAG workflowDag =
        WorkflowDAG.construct(machineTypes, machines, workflow);

    // Set all machines to use the least expensive machine type.
    for (WorkflowNode node : workflowDag.getNodes()) {
      for (int task = 0; task < node.getNumTasks(); task++) {
        node.setMachineType(task, sortedMachineTypes.get(0).getName());
      }
    }

    // Check that constraints aren't violated.
    // Time is in seconds, Cost is in $$. (see {@link TableEntry})
    // TODO test
    float pathTime = workflowDag.getTime(table);
    float pathCost = workflowDag.getCost(table);

    // Find the task to be rescheduled.
    // TODO: Use critical path to reschedule tasks on quicker machines, until
    // TODO: the next step takes us over the given budget.
    // TODO: Find best stage/task on critical path for rescheduling.
    List<WorkflowNode> criticalPath = workflowDag.getCriticalPath(table);

    // TODO: when setting save as proper units.
    // TODO: Use TimePriceTable.getExecTime() for deadline conversion?

    String budgetConstraint = workflow.getConstraint(Constraints.BUDGET);
    String deadlineConstraint = workflow.getConstraint(Constraints.DEADLINE);


    // Return the current 'cheapest' scheduling as the final schedule. (??)

    // TODO: need to change code to consider tasks instead of stages
    // -----> only change to do is what to do to final machine-task pairing.?

    // TODO: need to convert unconstrained scheduling to constrained
    // (wrt/ number and type of actual machines)


    return true;
  }

  // @formatter: on

  @Deprecated
  private void printDebugInfo(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table) {

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
  }


  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

}