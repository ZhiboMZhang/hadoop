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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class OptimalSchedulingPlan extends WorkflowSchedulingPlan {

  private static final Log LOG = LogFactory.getLog(OptimalSchedulingPlan.class);

  private WorkflowDAG workflowDag;

  // jobName / workflowNode (has tasks set to machine)
  private Map<String, WorkflowNode> taskMapping;
  private Map<String, String> trackerMapping;  // trackerName -> machineType

  // We can assume that all tasks have the same execution time (which is given).
  // Priorities list keeps a list of WorkflowNodes.
  // (corresponding to TASKS, not stages).

  // TODO: convert unconstrained (unlimited res) to constained

  @Override
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) throws IOException {

    LOG.info("In OptimalSchedulingPlan generatePlan() function.");

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
    List<MachineType> sortedMachineTypes = new ArrayList<MachineType>(machineTypes);
    Collections.sort(sortedMachineTypes, WorkflowUtil.MACHINE_TYPE_COST_ORDER);
    LOG.info("Sorted Machine Types.");
    WorkflowUtil.printMachineTypesInfo(sortedMachineTypes);

    // Get the workflow DAG corresponding to the workflow configuration, &c.
    workflowDag = WorkflowDAG.construct(machineTypes, machines, workflow);
    LOG.info("Constructed WorkflowDAG.");

    // Initially set all machines to use the least expensive machine type.
    for (WorkflowNode node : workflowDag.getNodes()) {
      for (WorkflowTask task : node.getTasks()) {
        task.setMachineType(sortedMachineTypes.get(0).getName());
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

    // Test all configurations of machineType to task mappings, selecting the
    // set which results in the minimum execution time while still satisfying
    // the budget constraint.

    // Compute the number of permutations.
    // First create a list of tasks.
    List<WorkflowTask> tasks = new ArrayList<WorkflowTask>();
    for (WorkflowNode node : workflowDag.getNodes()) {
      tasks.addAll(node.getTasks());
    }
    
    // Generate the permutations.
    List<List<MachineType>> permutations =
        WorkflowUtil.<MachineType> getPermutations(machineTypes, tasks.size());
    LOG.info("Generated machineType permutations.");

    // Pair up a permutation to the task list.
    float minTime = Float.MAX_VALUE;
    Map<WorkflowTask, String> optimalScheduling = new HashMap<WorkflowTask, String>();

    for (List<MachineType> permutation : permutations) {

      for (int i = 0; i < tasks.size(); i++) {
        tasks.get(i).setMachineType(permutation.get(i).getName());
      }
      LOG.info("Set " + permutations.indexOf(permutation) + "th permutation.");
      LOG.info("Permutation is: " + Arrays.toString(permutation.toArray(new MachineType[0])));

      // Calculate the cost and time.
      // Time is in seconds, Cost is in $$. (see {@link TableEntry})
      float actualCost = workflowDag.getCost(table);
      LOG.info("Workflow cost: " + actualCost + ", constraint: " + maxCost);

      if (actualCost > maxCost) {
        LOG.info("Current schedule is above budget constraint, continuing.");
        continue;
      }
      LOG.info("Current schedule is below budget constraint, considering.");

      // Update the taskMapping if necessary.
      float actualTime = workflowDag.getTime(table);
      if (actualTime < minTime) {
        LOG.info("Current schedule is best seen so far, saving.");
        minTime = actualTime;
        optimalScheduling.clear();
        
        for (WorkflowTask task : tasks) {
          optimalScheduling.put(task, task.getMachineType());
        }
      }
      LOG.info("Continuing to check next schedule.");
    }

    // Update the actual mapping. Need to do this as the taskMapping has nodes
    // whose task's mapping may have been overwritten between when the optimal
    // schedule was found and now.
    for (WorkflowTask task : tasks) {
      task.setMachineType(optimalScheduling.get(task));
    }
    LOG.info("Set workflow dag to have the best schedule.");

    taskMapping = new HashMap<String, WorkflowNode>();

    for (WorkflowNode node : workflowDag.getNodes()) {
      taskMapping.put(node.getJobName(), node);
      LOG.info("Added pair: " + node.getJobName() + "/" + node);
    }

    // Inform the user about the schedule.
    LOG.info("!!! Simulation complete. !!!");
    LOG.info("Workflow total cost: " + workflowDag.getCost(table));
    LOG.info("Workflow total time: " + workflowDag.getTime(table));
    LOG.info("Workflow budget constraint: $" + maxCost);
    LOG.info("Workflow deadline constraint: N/A");

    return true;
  }

  @Override
  public boolean matchMap(String machineType, String jobName) {
    LOG.info("In matchMap function");
    return match(machineType, jobName, TaskType.MAP);
  }

  @Override
  public boolean matchReduce(String machineType, String jobName) {
    LOG.info("In matchReduce function");
    return match(machineType, jobName, TaskType.REDUCE);
  }

  private boolean match(String machineType, String jobName, TaskType taskType) {

    Collection<WorkflowTask> tasks = taskMapping.get(jobName).getTasks();

    for (WorkflowTask task : tasks) {
      String machine = task.getMachineType();
      String name = task.getJobName();
      TaskType type = (task.isMapTask() ? TaskType.MAP : TaskType.REDUCE);

      LOG.info("Match input is " + machineType + "/" + jobName + "/" + taskType);
      LOG.info("vs: " + machine + "/" + name + "/" + type);

      if (machine.equals(machineType) && name.equals(jobName)
          && type.equals(taskType)) {
        LOG.info("Found a match!");
        // Assume task will be successful when executed.
        tasks.remove(task);

        return true;
      }
    }
    return false;
  }

  Collection<String> prevFinishedJobs = new HashSet<String>();
  Collection<String> prevExecutableJobs = new HashSet<String>();

  @Override
  // TODO: what if first call finishedJobs isn't null --> error checking
  public List<String> getExecutableJobs(Collection<String> finishedJobs) {

    LOG.info("In getExecutableJobs.");
    List<String> executableJobs = new ArrayList<String>();

    // If there are no finished jobs then return the entry nodes.
    if (finishedJobs == null || finishedJobs.size() == 0) {
      for (WorkflowNode node : workflowDag.getEntryNodes()) {
        executableJobs.add(node.getJobName());
      }
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

    // TODO: better
    Map<String, WorkflowNode> map = new HashMap<String, WorkflowNode>();
    for (WorkflowNode node : workflowDag.getNodes()) {
      map.put(node.getJobName(), node);
    }

    // A successor of a finished job is eligible for execution if all of its
    // dependencies are finished.
    for (String job : finishedJobs) {

      LOG.info("Checking to add successors of job " + job + ".");

      for (WorkflowNode successor : workflowDag.getSuccessors(map.get(job))) {
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

    taskMapping = new HashMap<String, WorkflowNode>();
    int numTaskMappings = in.readInt();
    for (int i = 0; i < numTaskMappings; i++) {
      String key = Text.readString(in);
      WorkflowNode value = new WorkflowNode();
      value.readFields(in);
      taskMapping.put(key, value);
    }

    trackerMapping = new HashMap<String, String>();
    int numTrackerMappings = in.readInt();
    for (int i = 0; i < numTrackerMappings; i++) {
      String key = Text.readString(in);
      String value = Text.readString(in);
      trackerMapping.put(key, value);
    }

    prevFinishedJobs = new HashSet<String>();
    int numFinishedJobs = in.readInt();
    for (int i = 0; i < numFinishedJobs; i++) {
      prevFinishedJobs.add(Text.readString(in));
    }

    prevExecutableJobs = new HashSet<String>();
    int numExecutableJobs = in.readInt();
    for (int i = 0; i < numExecutableJobs; i++) {
      prevExecutableJobs.add(Text.readString(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    workflowDag.write(out);

    out.writeInt(taskMapping.size());
    for (String key : taskMapping.keySet()) {
      Text.writeString(out, key);
      taskMapping.get(key).write(out);
    }

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