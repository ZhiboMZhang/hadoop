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
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.mapred.workflow.schedulers.WorkflowUtil.Pair;

/**
 * A basic workflow scheduling plan, schedules jobs & their tasks in a first-in
 * first-out manner.
 */
public class FifoSchedulingPlan extends SchedulingPlan {

  private static final Log LOG = LogFactory.getLog(FifoSchedulingPlan.class);

  // We can assume that all tasks have the same execution time (which is given).
  // Priorities list keeps a list of WorkflowNodes.
  // (corresponding to TASKS, not stages).

  @Override
  /**
   * Plan generation in this case will return a basic scheduling, disregarding
   *  constraints.
   */
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) {

    LOG.info("In FairScheduler.class generatePlan() function");
    WorkflowUtil.printMachineTypesInfo(machineTypes);
    WorkflowUtil.printMachinesInfo(machines);
    WorkflowUtil.printTableInfo(table);

    // Create a map from machine type name to the actual MachineType.
    Map<String, MachineType> machineType = new HashMap<String, MachineType>();
    for (MachineType type : machineTypes) {
      machineType.put(type.getName(), type);
    }
    LOG.info("Created map for machineType names to machineType.");

    // Get a sorted list of machine types by cost/unit time.
    List<MachineType> sortedMachineTypes = new ArrayList<MachineType>(machineTypes);
    Collections.sort(sortedMachineTypes, WorkflowUtil.MACHINE_TYPE_COST_ORDER);
    LOG.info("Sorted Machine Types.");
    WorkflowUtil.printMachineTypesInfo(sortedMachineTypes);

    // Get the workflow DAG corresponding to the workflow configuration, &c.
    WorkflowDAG workflowDag = WorkflowDAG.construct(machineTypes, machines, workflow);
    LOG.info("Constructed WorkflowDAG.");

    // Set all machines to use the least expensive machine type.
    for (WorkflowNode node : workflowDag.getNodes()) {
      for (int task = 0; task < node.getNumTasks(); task++) {
        node.setMachineType(task, sortedMachineTypes.get(0).getName());
      }
    }
    LOG.info("Set all nodes in workflow dag to least expensive type.");

    // Check that constraints aren't violated.
    // Time is in seconds, Cost is in $$. (see {@link TableEntry})
    float workflowCost = workflowDag.getCost(table);
    String budgetConstraint = workflow.getConstraint(Constraints.BUDGET);
    float constraintCost = WorkflowConf.parseBudgetConstraint(budgetConstraint);
    LOG.info("Computed initial path time and workflow cost.");
    LOG.info("Workflow cost: " + workflowCost + ", constraint: " + constraintCost);

    // Budget isn't enough to run the workflow even on the cheapest machines.
    if (workflowCost > constraintCost) {
      LOG.info("ERROR: Cheapest workflow cost is above budget constraint.");
      return false;
    }

    LOG.info("Cheapest workflow cost is below budget constraint, running alg.");
    // Iteratively, find the task to be rescheduled.
    Set<WorkflowTask> unReschedulableNodes = new HashSet<WorkflowTask>();
    List<WorkflowNode> criticalPath = workflowDag.getCriticalPath(table);
    LOG.info("Got critical path");

    // Find the best task on the critical path for rescheduling.
    // Just use a basic metric, reschedule the slowest task.
    do {

      WorkflowNode slowestNode = null;
      int slowestTask = -1;
      float maxTime = 0;

      // Find the slowest task.
      for (WorkflowNode node : criticalPath) {
        for (int task = 0; task < node.getNumTasks(); task++) {

          if (unReschedulableNodes.contains(new WorkflowTask(node, task))) {
            LOG.info("unReschedulableNodes contains " + node.getName() + ":"
                + task);
            continue;
          }
          LOG.info("Getting time for node " + node.getName() + ":" + task);

          String type = node.getMachineType(task);
          TableKey key = new TableKey(node.getJob(), type, node.isMapStage());
          float time = table.get(key).execTime;

          if (time > maxTime) {
            maxTime = time;
            slowestNode = node;
            slowestTask = task;
          }
        }
      }

      // No nodes on the critical path are able to be rescheduled.
      if (slowestNode == null) { break; }

      LOG.info("Slowest task on critical path is: " + slowestNode.getName()
          + ":" + slowestTask);

      // Update the machine type.
      String prevType = slowestNode.getMachineType(slowestTask);
      int prevTypeIdx = sortedMachineTypes.indexOf(machineType.get(prevType));

      if (sortedMachineTypes.size() == (prevTypeIdx + 1)) {
        // Can't reschedule the slowest node.
        // It's already running on the quickest machine
        LOG.info("Slowest task is already scheduled on the quickest machine.");
        unReschedulableNodes.add(new WorkflowTask(slowestNode, slowestTask));

      } else {

        String newType = sortedMachineTypes.get(prevTypeIdx + 1).getName();
        slowestNode.setMachineType(slowestTask, newType);
        LOG.info("Updated machine type on " + slowestNode.getName() + " from "
            + prevType + " to " + newType);

        // Check if the reschedule has increased the cost over the given budget.
        workflowCost = workflowDag.getCost(table);
        LOG.info("Updated Workflow cost is: " + workflowCost
            + ", constraint is: " + constraintCost);

        if (workflowCost > constraintCost) {
          // Cost is now above the constraint, undo & attempt w/ other tasks.
          LOG.info("Cost is now above budget constraint, continuing.");
          slowestNode.setMachineType(slowestTask, prevType);
          unReschedulableNodes.add(new WorkflowTask(slowestNode, slowestTask));
        } else {
          // Reschedule was fine, recompute the critical path.
          LOG.info("Reschedule was under budget, recomputing critical path.");
          criticalPath = workflowDag.getCriticalPath(table);
          unReschedulableNodes.clear();
        }
      }
    } while (true);

    // Return the current 'cheapest' scheduling as the final schedule.
    // Our scheduling plan is a list of WorkflowNodes (stages), each of which
    // is paired to a machine. Since tasks are 'the same', a WorkflowNode in
    // this case represents a task to be executed (WorkflowNodes are repeated).
    List<Pair<WorkflowNode, MachineType>> taskMapping;
    taskMapping = new ArrayList<Pair<WorkflowNode, MachineType>>();
    List<WorkflowNode> ordering = workflowDag.getTopologicalOrdering();
    Pair<WorkflowNode, MachineType> pair;

    for (WorkflowNode node : ordering) {
      for (int task = 0; task < node.getNumTasks(); task++) {
        String type = node.getMachineType(task);
        pair = new Pair<WorkflowNode, MachineType>(node, machineType.get(type));
        taskMapping.add(pair);
      }
    }
    LOG.info("Created task mapping.");
    for (Pair<WorkflowNode, MachineType> p : taskMapping) {
      LOG.info("Pair maps " + p.from.getName() + " to " + p.to.getName());
    }

    // Because our model assumes an unconstrained scheduling (unlimited
    // resources), we need to convert the plan to an actual scheduling wrt/
    // available cluster resources.
    // TODO test / ???
    Map<MachineType, Set<ResourceStatus>> typeToMachine;
    typeToMachine = WorkflowUtil.matchResourceTypes(machineTypes, machines);

    return true;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

}