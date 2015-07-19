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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.MachineType;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;
import org.apache.hadoop.mapred.workflow.WorkflowUtil;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowDAG;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowNode;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowSchedulingPlan;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowTask;
import org.apache.hadoop.mapreduce.TaskType;

// We can assume that all tasks have the same execution time (which is given).
// Priorities list keeps a list of WorkflowNodes.
// (corresponding to TASKS, not stages).
// TODO: convert unconstrained (unlimited res) to constained
public class GreedySchedulingPlan extends WorkflowSchedulingPlan {

  private static class WorkflowTaskPair {

    WorkflowTask slowest;
    WorkflowTask secondSlowest;

    public WorkflowTaskPair(WorkflowTask slowest, WorkflowTask secondSlowest) {
      this.slowest = slowest;
      this.secondSlowest = secondSlowest;
    }
  }

  private static class Utility implements Comparable<Utility> {

    WorkflowTask slowestTask;
    float utility;

    public Utility(WorkflowTask slowestTask, float utility) {
      this.slowestTask = slowestTask;
      this.utility = utility;
    }

    @Override
    public int compareTo(Utility other) {
      // Want a larger utility, so sort in descending order.
      if (utility == other.utility) { return 0; }
      if (utility > other.utility) { return -1; }
      if (utility < other.utility) { return 1; }
      return 0;
    }
  }

  private static final Log LOG = LogFactory.getLog(GreedySchedulingPlan.class);

  private WorkflowDAG workflowDag;

  // jobName / workflowNode (has tasks set to machine)
  private Map<String, WorkflowNode> taskMapping;
  private Map<String, String> trackerMapping;  // trackerName -> machineType

  // Used in getExecutableJobs().
  Collection<String> prevFinishedJobs;
  Collection<String> prevExecutableJobs;

  public GreedySchedulingPlan() {
    // The workflowDag & trackerMapping are init'd in generatePlan via a called
    // function. This is also why we call new for them in readFields but not here.
    prevFinishedJobs = new HashSet<String>();
    prevExecutableJobs = new HashSet<String>();
    taskMapping = new HashMap<String, WorkflowNode>();
  }

  @Override
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) throws IOException {

    LOG.info("In GreedySchedulingPlan generatePlan() function");

    // Read constraint information.
    String constraint = workflow.getConstraint(Constraints.BUDGET);
    float maxCost = WorkflowConf.parseBudgetConstraint(constraint);

    // Get a mapping between actual available machines and machine types.
    trackerMapping = WorkflowUtil.matchResourceTypes(machineTypes, machines);

    for (String type : trackerMapping.keySet()) {
      LOG.info("Mapped machinetype " + type + " to " + trackerMapping.get(type));
    }

    // Remove machine types that don't currently exist on the cluster.
    Iterator<MachineType> machineTypeIterator = machineTypes.iterator();
    while (machineTypeIterator.hasNext()) {
      MachineType machineType = machineTypeIterator.next();
      if (!trackerMapping.values().contains(machineType.getName())) {
        machineTypeIterator.remove();
      }
    }

    // Create a map from machine type name to the actual MachineType.
    Map<String, MachineType> machineType = new HashMap<String, MachineType>();
    for (MachineType type : machineTypes) {
      machineType.put(type.getName(), type);
    }
    LOG.info("Created map for machineType names to machineType.");

    // Get a sorted list of machine types by cost/unit time.
    List<MachineType> sortedMachines = new ArrayList<MachineType>(machineTypes);
    Collections.sort(sortedMachines, WorkflowUtil.MACHINE_TYPE_COST_ORDER);
    LOG.info("Sorted Machine Types.");
    WorkflowUtil.printMachineTypesInfo(sortedMachines);

    // Get the workflow DAG corresponding to the workflow configuration, &c.
    workflowDag = WorkflowDAG.construct(machineTypes, machines, workflow);
    LOG.info("Constructed WorkflowDAG.");

    for (WorkflowNode node : workflowDag.getNodes()) {
      taskMapping.put(node.getJobName(), node);
      LOG.info("Added pair: " + node.getJobName() + "/" + node);
    }

    // Initially set all machines to use the least expensive machine type.
    for (WorkflowNode node : workflowDag.getNodes()) {
      for (WorkflowTask task : node.getTasks()) {
        task.setMachineType(sortedMachines.get(0).getName());
      }
    }
    LOG.info("Set all nodes in workflow dag to least expensive type.");

    // Check that constraints aren't violated.
    // Time is in seconds, Cost is in $$. (see {@link TableEntry})
    float actualCost = workflowDag.getCost(table);
    LOG.info("Computed initial path time and workflow cost.");
    LOG.info("Workflow cost: " + actualCost + ", constraint: " + maxCost);

    // Budget isn't enough to run the workflow even on the cheapest machines.
    if (actualCost > maxCost) {
      LOG.info("ERROR: Cheapest workflow cost is above budget constraint.");
      return false;
    }
    LOG.info("Cheapest workflow cost is below budget constraint, running alg.");

    // Iteratively, find the task to be rescheduled.
    // Find the best task on the critical path for rescheduling.
    float remainingBudget = maxCost - actualCost;
    while (remainingBudget >= 0) {

      LOG.info("Next iteration of outer loop.  Budget left: " + remainingBudget);
      List<WorkflowNode> criticalPath = workflowDag.getCriticalPath(table);
      Collection<Utility> utilities = new TreeSet<Utility>();

      for (WorkflowNode node : criticalPath) {
        LOG.info("Checking stages of node " + node.getJobName() + " on critical path.");

        LOG.info("Getting the slowest pair of tasks for the map stage.");
        if (!node.getMapTasks().isEmpty()) {
          WorkflowTaskPair pair = getSlowestPair(table, node.getMapTasks());

          // Check if faster machine actually exists
          int slowestIdx = sortedMachines.indexOf(
              machineType.get(pair.slowest.getMachineType()));

          if (slowestIdx < (sortedMachines.size() - 1)) {
            // Compute the utility for the pair.
            float utility = computeUtility(table, pair, machineType, sortedMachines);
            utilities.add(new Utility(pair.slowest, utility));
            LOG.info("Can reschedule map task.");
          }
        }

        LOG.info("Getting the slowest pair of tasks for the reduce stage.");
        if (!node.getReduceTasks().isEmpty()) {
          WorkflowTaskPair pair = getSlowestPair(table, node.getReduceTasks());

          int slowestIdx = sortedMachines.indexOf(
              machineType.get(pair.slowest.getMachineType()));

          if (slowestIdx < (sortedMachines.size() - 1)) {
            float utility = computeUtility(table, pair, machineType, sortedMachines);
            utilities.add(new Utility(pair.slowest, utility));
            LOG.info("Can reschedule reduce task.");
          }
        }
      }

      Iterator<Utility> utilitiesIterator = utilities.iterator();
      while (utilitiesIterator.hasNext()) {
        WorkflowTask task = utilitiesIterator.next().slowestTask;
        LOG.info("Checking utility of task " + task + ".");

        String jobName = task.getJobName();
        boolean isMapTask = task.isMapTask();

        String currMachine = task.getMachineType();
        int currMachineIdx = sortedMachines.indexOf(machineType.get(currMachine));
        String newMachine = sortedMachines.get(currMachineIdx + 1).getName();

        // Get the old and new costs to compare.
        TableKey oldCostKey = new TableKey(jobName, currMachine, isMapTask);
        TableKey newCostKey = new TableKey(jobName, newMachine, isMapTask);
        float oldCost = table.get(oldCostKey).cost;
        float newCost = table.get(newCostKey).cost;
        float costDifference = (newCost - oldCost);

        if (remainingBudget < costDifference) {
          LOG.info("Not enough budget to reschedule task " + task + ".");
          // Don't consider rescheduling the task if it breaks the budget.
          utilitiesIterator.remove();
        } else {
          LOG.info("Rescheduled task " + task + " to run on " + newMachine + ".");
          // The task with the best utility can be rescheduled.. so do it!
          task.setMachineType(newMachine);
          remainingBudget -= costDifference;

          // After rescheduling recalculate the critical path & utility values.
          break;
        }
      }

      // Since the utilities are recomputed when one is used to reschedule a
      // task, the if statement is only entered if no tasks are able to be
      // rescheduled. We've done all that we can, exit from the algorithm.
      if (utilities.isEmpty()) {
        LOG.info("Unable to reschedule any tasks, exiting.");
        break;
      }
    }

    // Return the current 'cheapest' scheduling as the final schedule.
    // Our scheduling plan is a list of WorkflowNodes (stages), each of which
    // is paired to a machine. Since tasks are 'the same', in this case a
    // WorkflowNode represents a task to be executed (WorkflowNodes are
    // repeated).

    // Inform the user about the schedule.
    LOG.info("!!! Simulation complete. !!!");
    LOG.info("Workflow total cost: " + workflowDag.getCost(table));
    LOG.info("Workflow total time: " + workflowDag.getTime(table));
    LOG.info("Workflow budget constraint: $" + maxCost);
    LOG.info("Workflow deadline constraint: N/A");

    return true;
  }

  /**
   * Given price information along with a stage (collection of tasks), return
   * the two slowest tasks in the stage.
   */
  private WorkflowTaskPair getSlowestPair(
      Map<TableKey, TableEntry> table, Collection<WorkflowTask> tasks) {

    WorkflowTask slowestTask = null;
    WorkflowTask secondSlowestTask = null;
    float maxTime = 0;
    float secondMaxTime = 0;

    for (WorkflowTask task : tasks) {
      String type = task.getMachineType();
      TableKey key = new TableKey(task.getJobName(), type, task.isMapTask());
      float time = table.get(key).execTime;
      // TODO: could be nullpointer exception

      if (time > maxTime) {
        secondMaxTime = maxTime;
        secondSlowestTask = slowestTask;
        maxTime = time;
        slowestTask = task;
      } else if (time > secondMaxTime) {
        secondMaxTime = time;
        secondSlowestTask = task;
      }
    }

    return new WorkflowTaskPair(slowestTask, secondSlowestTask);
  }

  /**
   * Given price information along with machine type information and a pair of
   * slowest tasks for a stage, return the stage's utility value.
   */
  private float computeUtility(Map<TableKey, TableEntry> table,
      WorkflowTaskPair pair, Map<String, MachineType> machineType,
      List<MachineType> sortedMachineTypes) {

    // Time & Cost for slowest task on current machine.
    TableKey slowestCurKey = new TableKey(
        pair.slowest.getJobName(),
        pair.slowest.getMachineType(),
        pair.slowest.isMapTask());
    float slowestCurTime = table.get(slowestCurKey).execTime;
    float slowestCurCost = table.get(slowestCurKey).cost;

    // Time & Cost for slowest task on next faster machine.
    int slowestCurMachine = sortedMachineTypes.indexOf(machineType.get(pair.slowest.getMachineType()));
    String slowestNewMachine = sortedMachineTypes.get(slowestCurMachine + 1).getName();

    TableKey slowestNewKey = new TableKey(
        pair.slowest.getJobName(),
        slowestNewMachine,
        pair.slowest.isMapTask());
    float slowestNewTime = table.get(slowestNewKey).execTime;
    float slowestNewCost = table.get(slowestNewKey).cost;

    // Time for second-slowest task on its current machine.
    TableKey secondSlowestCurKey = new TableKey(
        pair.secondSlowest.getJobName(),
        pair.secondSlowest.getMachineType(),
        pair.secondSlowest.isMapTask());
    float secondSlowestCurTime = table.get(secondSlowestCurKey).execTime;

    // Compute and return the utility.
    return Math.min((slowestCurTime - slowestNewTime),
        (slowestCurTime - secondSlowestCurTime))
        / (slowestNewCost - slowestCurCost);
  }

  /**
   * Execute (or test) a task from a job on a particular machine.
   *
   * @param machineType The type of machine to run the task on.
   * @param jobName The job that the task belongs to.
   * @param taskType The task type: map, or reduce.
   * @param isDryRun Whether the scheduling should be executed or not.
   */
  private boolean runTask(String machineType, String jobName,
      TaskType taskType, boolean isDryRun) {

    Collection<WorkflowTask> tasks = taskMapping.get(jobName).getTasks();

    for (WorkflowTask task : tasks) {
      String machine = task.getMachineType();
      String name = task.getJobName();
      TaskType type = (task.isMapTask() ? TaskType.MAP : TaskType.REDUCE);

      LOG.info("Match input is " + machineType + "/" + jobName + "/" + taskType);
      LOG.info("vs: " + machine + "/" + name + "/" + type);

      if (machine.equals(machineType) && name.equals(jobName) && type.equals(taskType)) {
        LOG.info("Found a match!");
        if (!isDryRun) { tasks.remove(task); }

        return true;
      }
    }
    return false;
  }

  @Override
  public boolean matchMap(String machineType, String jobName) {
    LOG.info("In matchMap function.");
    return runTask(machineType, jobName, TaskType.MAP, true);
  }

  @Override
  public boolean runMap(String machineType, String jobName) {
    LOG.info("In runMap function.");
    return runTask(machineType, jobName, TaskType.MAP, false);
  }

  @Override
  public boolean matchReduce(String machineType, String jobName) {
    LOG.info("In matchReduce function.");
    return runTask(machineType, jobName, TaskType.REDUCE, true);
  }

  @Override
  public boolean runReduce(String machineType, String jobName) {
    LOG.info("In runReduce function.");
    return runTask(machineType, jobName, TaskType.REDUCE, false);
  }

  @Override
  // TODO: what if first call finishedJobs isn't null --> error checking
  public List<String> getExecutableJobs(Collection<String> finishedJobs) {

    List<String> executableJobs = new ArrayList<String>();

    // If there are no finished jobs then return the entry nodes.
    if (finishedJobs == null || finishedJobs.size() == 0) {
      for (WorkflowNode node : workflowDag.getEntryNodes()) {
        executableJobs.add(node.getJobName());
      }
      LOG.info("No jobs finished, returning the set of entry jobs.");
      return executableJobs;
    }

    // We've previously sent some executable jobs. If the set of finished jobs
    // is the same as we previously sent, then nothing needs to be done.
    // Otherwise, the new finished jobs determine the next executable jobs.
    if (finishedJobs.size() == workflowDag.getNodes().size()) {
      LOG.info("All jobs are finished, returning the empty set.");
      executableJobs.clear();
      return executableJobs;
    } else if (prevFinishedJobs.equals(new HashSet<String>(finishedJobs))) {
      LOG.info("Set of finished jobs is the same as before (no progress made).");
      return new ArrayList<String>(prevExecutableJobs);
    } else {
      // Modify finishedJobs so that we only consider newly finished jobs.
      // And add the new finished jobs to our previously finished jobs.
      LOG.info("Got new finished jobs.");
      finishedJobs.removeAll(prevFinishedJobs);
      prevFinishedJobs.addAll(finishedJobs);
    }

    // A successor of a finished job is eligible for execution if all of its
    // dependencies are finished.
    for (String job : finishedJobs) {

      LOG.info("Checking to add successors of job " + job + ".");

      for (WorkflowNode successor : workflowDag.getSuccessors(taskMapping.get(job))) {
        LOG.info("Checking eligibility of successor " + successor.getJobName() + ".");
        boolean eligible = true;

        for (WorkflowNode predecessor : workflowDag.getPredecessors(successor)) {
          LOG.info("Checking if predecessor " + predecessor.getJobName() + " is finished.");
          String predecessorName = predecessor.getJobName();
          if (!finishedJobs.contains(predecessorName)
              && !prevFinishedJobs.contains(predecessorName)) {
            eligible = false;
          }
        }
        if (eligible) {
          LOG.info("Adding " + successor.getJobName() + " to list of executable jobs.");
          executableJobs.add(successor.getJobName());
        }
      }
    }

    prevExecutableJobs = executableJobs;
    return executableJobs;
  }

  @Override
  public Map<String, String> getTrackerMapping() {
    return trackerMapping;
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    workflowDag = new WorkflowDAG();
    workflowDag.readFields(in);

    // Set the taskMapping variable.
    for (WorkflowNode node : workflowDag.getNodes()) {
      taskMapping.put(node.getJobName(), node);
    }

    trackerMapping = new HashMap<String, String>();
    int numTrackerMappings = in.readInt();
    for (int i = 0; i < numTrackerMappings; i++) {
      String key = Text.readString(in);
      String value = Text.readString(in);
      trackerMapping.put(key, value);
    }

    int numFinishedJobs = in.readInt();
    for (int i = 0; i < numFinishedJobs; i++) {
      prevFinishedJobs.add(Text.readString(in));
    }

    int numExecutableJobs = in.readInt();
    for (int i = 0; i < numExecutableJobs; i++) {
      prevExecutableJobs.add(Text.readString(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    workflowDag.write(out);

    out.writeInt(trackerMapping.size());
    for (String key : trackerMapping.keySet()) {
      Text.writeString(out, key);
      Text.writeString(out, trackerMapping.get(key));
    }

    out.writeInt(prevFinishedJobs.size());
    for (String job : prevFinishedJobs) {
      Text.writeString(out, job);
    }

    out.writeInt(prevExecutableJobs.size());
    for (String job : prevExecutableJobs) {
      Text.writeString(out, job);
    }
  }

}