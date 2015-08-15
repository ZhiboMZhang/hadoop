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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.MachineType;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowUtil;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowDAG;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowNode;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowSchedulingPlan;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowTask;
import org.apache.hadoop.mapreduce.TaskType;

// A custom scheduler is not needed as we only deal with one workflow at a time.
public class ProgressBasedSchedulingPlan extends WorkflowSchedulingPlan {

  private class SchedulingEvent implements Comparable<SchedulingEvent>,
      Writable {
    public long time;
    public String jobName;
    public int numMaps = 0;
    public int numReduces = 0;

    /** Only to be used when calling readFields() afterwards. */
    public SchedulingEvent() {}

    public SchedulingEvent(long time, String jobName, int numMaps, int numReds) {
      this.time = time;
      this.jobName = jobName;
      this.numMaps = numMaps;
      this.numReduces = numReds;
    }

    @Override
    public String toString() {
      return jobName;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      time = in.readLong();
      jobName = Text.readString(in);
      numMaps = in.readInt();
      numReduces = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(time);
      Text.writeString(out, jobName);
      out.writeInt(numMaps);
      out.writeInt(numReduces);
    }

    @Override
    public int compareTo(SchedulingEvent other) {
      if (time > other.time) { return 1; }
      if (time < other.time) { return -1; }
      if (time == other.time) { return 0; }
      return 0;
    }
  }

  // Events related to the simulation of task assignment: jobs being added, and
  // jobs being completed (slots being freed).
  private class FreeEvent implements Comparable<FreeEvent> {
    public long time;
    public int freedMapSlots;
    public int freedRedSlots;

    /** At the provided time, the specified number of slots are freed. */
    public FreeEvent(long time, int freedMapSlots, int freedRedSlots) {
      this.time = time;
      this.freedMapSlots = freedMapSlots;
      this.freedRedSlots = freedRedSlots;
    }

    @Override
    public int compareTo(FreeEvent other) {
      if (time > other.time) { return 1; }
      if (time < other.time) { return -1; }
      return 0;
    }
  }

  private static final Log LOG = LogFactory.getLog(ProgressBasedSchedulingPlan.class);

  // For generatePlan & class itself.
  private WorkflowDAG workflowDag;
  private WorkflowPrioritizer prioritizer;
  private Map<String, WorkflowNode> taskMapping;  // job -> wfNode (tasks have machine)
  private Map<String, String> trackerMapping;  // trackerName -> machineType

  // For match & executable job functions.
  private Map<TableKey, TableEntry> table;
  private Queue<SchedulingEvent> scheduleEvents;
  private Set<String> finishedJobs;
  private long currentTime = 0;

  public ProgressBasedSchedulingPlan() {
    taskMapping = new HashMap<String, WorkflowNode>();
    scheduleEvents = new PriorityQueue<SchedulingEvent>();
    finishedJobs = new HashSet<String>();
  }

  @Override
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) throws IOException {

    LOG.info("In ProgressBasedSchedulingPlan generatePlan() function");

    this.table = table;

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

    // Get the most expensive machine for scheduling, the fastest machine.
    // We assume cost & execution time are inversely proportional.
    // TODO: as plan is made should select whatever machine would be available
    // at that time.
    MachineType fastestMachine = null;
    for (MachineType type : machineTypes) {
      if (fastestMachine == null
          || type.getChargeRate() > fastestMachine.getChargeRate()) {
        fastestMachine = type;
      }
    }

    // Get the workflow DAG corresponding to the workflow configuration, &c.
    workflowDag = WorkflowDAG.construct(machineTypes, machines, workflow);
    LOG.info("Constructed WorkflowDAG.");

    // Set the taskMapping variable.
    for (WorkflowNode node : workflowDag.getNodes()) {
      taskMapping.put(node.getJobName(), node);
      LOG.info("Added pair: " + node.getJobName() + "/" + node);
    }
    
    // Prioritize the jobs/nodes in the workflow dag by our criterion.
    // TODO: reflection to select whatever prioritizer concrete type
    prioritizer = new HighestLevelFirstPrioritizer(workflowDag);

    // Construct a map to hold the number of unscheduled tasks.
    // jobname -> number of unscheduled tasks
    Map<String, Integer> mapTasks = new HashMap<String, Integer>();
    Map<String, Integer> redTasks = new HashMap<String, Integer>();
    
    for (WorkflowNode node : workflowDag.getNodes()) {
      mapTasks.put(node.getJobName(), node.getMapTasks().size());
      redTasks.put(node.getJobName(), node.getReduceTasks().size());
    }
    LOG.info("Created jobname to unscheduled tasks map.");

    // Compute the total number of slots.
    int totalMapSlots = 0;
    int totalRedSlots = 0;

    for (ResourceStatus status : machines.values()) {
      totalMapSlots += status.getMaxMapSlots();
      totalRedSlots += status.getMaxReduceSlots();
    }
    LOG.info("Computed total number of map (" + totalMapSlots + ") and reduce (" + totalRedSlots + ") slots.");

    // Event queues to hold slot free events in order of occurrence time.
    Queue<FreeEvent> mapQueue = new PriorityQueue<FreeEvent>();
    Queue<FreeEvent> redQueue = new PriorityQueue<FreeEvent>();

    mapQueue.add(new FreeEvent(0, totalMapSlots, 0));
    redQueue.add(new FreeEvent(0, 0, totalRedSlots));

    // The job priority metric determines the first/next jobs to execute.
    Queue<WorkflowNode> addMapQueue = new PriorityQueue<WorkflowNode>(11, prioritizer);
    Queue<WorkflowNode> addRedQueue = new PriorityQueue<WorkflowNode>(11, prioritizer);
    Queue<WorkflowNode> addSuccQueue = new PriorityQueue<WorkflowNode>(11, prioritizer);
    Set<WorkflowNode> unfinishedJobs = new HashSet<WorkflowNode>();

    addMapQueue.addAll(prioritizer.getExecutableJobs(new HashSet<WorkflowNode>()));
    LOG.info("The addQueue initially has " + addMapQueue.size() + " jobs.");

    long currentTime = 0L;
    int freeMapSlots = 0;
    int freeRedSlots = 0;

    // Main loop, simulating execution on the slots.
    while (!addMapQueue.isEmpty() || !addRedQueue.isEmpty()) {

      // Free any slots at the current time.
      FreeEvent free;
      while ((free = mapQueue.peek()) != null && free.time <= currentTime) {
        LOG.info("Added " + free.freedMapSlots + " freed map slots.");
        freeMapSlots += mapQueue.poll().freedMapSlots;
      }
      while ((free = redQueue.peek()) != null && free.time <= currentTime) {
        LOG.info("Added " + free.freedRedSlots + " freed reduce slots.");
        freeRedSlots += redQueue.poll().freedRedSlots;
      }

      // While there exist free slots AND we've not seen all jobs,
      // schedule tasks starting from high priority jobs.
      while (freeMapSlots > 0 && !addMapQueue.isEmpty()) {
        LOG.info("Looking for map tasks to schedule.");

        // Get the next highest priority job.
        WorkflowNode job = addMapQueue.poll();
        String jobName = job.getJobName();
        LOG.info("Got next job to schedule maps for as " + jobName + ".");

        // If the job has map tasks then schedule as many possible.
        int numMapTasks = mapTasks.get(jobName);
        if (numMapTasks > 0) {

          // Add unfinished jobs back to the queue for next time.
          unfinishedJobs.add(job);

          // Compute the number of maps that can be scheduled.
          int mapsToSchedule = Math.min(numMapTasks, freeMapSlots);
          int remainingMaps = numMapTasks - mapsToSchedule;
          LOG.info("Scheduling " + mapsToSchedule + " maps from job " + jobName);

          // Update the number of available slots and the task count for the job.
          mapTasks.put(jobName, remainingMaps);
          freeMapSlots -= mapsToSchedule;

          // Add the events: a scheduling event, and a free event.
          scheduleEvents.add(new SchedulingEvent(currentTime, jobName, mapsToSchedule, 0));

          String selectedMachine = fastestMachine.getName();
          setMachineTypes(selectedMachine, job, mapsToSchedule, true);
          TableKey key = new TableKey(jobName, selectedMachine, true);
          TableEntry entry = table.get(key);
          long freeTime = currentTime + entry.execTime;
          mapQueue.add(new FreeEvent(freeTime, mapsToSchedule, 0));
          LOG.info("Added free event for " + mapsToSchedule
              + " map slots at time " + freeTime + "s.");
        }
        // If the job doesn't have map tasks to schedule then we want
        // to schedule reduce events (at the current time).
        else {
          addRedQueue.add(job);
        }
      }
      addMapQueue.addAll(unfinishedJobs);
      unfinishedJobs.clear();

      // While there exist free slots AND we've not seen all jobs,
      // schedule tasks starting from high priority jobs.
      // Can't consider previously scheduled maps (above) without advancing
      // time, so we use a collection containing filtered add events.
      while (freeRedSlots > 0 && !addRedQueue.isEmpty()) {
        LOG.info("Looking for reduce tasks to schedule.");

        // Get the next highest priority job.
        WorkflowNode job = addRedQueue.poll();
        String jobName = job.getJobName();
        LOG.info("Got next job to schedule reduces for as " + jobName + ".");

        // If the job has reduce tasks then schedule as many as possible.
        int numRedTasks = redTasks.get(jobName);
        if (numRedTasks > 0) {

          // Add unfinished jobs back to the queue for the next time.
          unfinishedJobs.add(job);

          // Compute the number of reduces that can be scheduled.
          int redsToSchedule = Math.min(numRedTasks, freeRedSlots);
          int remainingReds = numRedTasks - redsToSchedule;
          LOG.info("Scheduling " + redsToSchedule + " reds from job " + jobName);

          // Update the number of available slots and the task count for the job.
          redTasks.put(jobName, remainingReds);
          freeRedSlots -= redsToSchedule;

          // Add the events: a scheduling event, and a free event.
          scheduleEvents.add(new SchedulingEvent(currentTime, jobName, 0, redsToSchedule));

          String selectedMachine = fastestMachine.getName();
          setMachineTypes(selectedMachine, job, redsToSchedule, false);
          TableKey key = new TableKey(jobName, selectedMachine, false);
          TableEntry entry = table.get(key);
          long freeTime = currentTime + entry.execTime;
          redQueue.add(new FreeEvent(freeTime, 0, redsToSchedule));
          LOG.info("Added free event for " + redsToSchedule
              + " reduce slots at time " + freeTime + "s.");
        }
        // If the job doesn't have reduce tasks then we want to add its
        // successors to the collection of executable jobs.
        else {
          addSuccQueue.add(job);
        }
      }
      addRedQueue.addAll(unfinishedJobs);
      unfinishedJobs.clear();

      // Add any successor jobs.
      if (!addSuccQueue.isEmpty()) {
        LOG.info("There are successors that can possibly be run.");
        List<WorkflowNode> finishedJobs = Arrays.asList(addSuccQueue.toArray(new WorkflowNode[0]));
        List<WorkflowNode> eligibleJobs = prioritizer.getExecutableJobs(finishedJobs);
        LOG.info(("New jobs can be run: " + Arrays.toString(eligibleJobs.toArray(new WorkflowNode[0]))));
        if (!eligibleJobs.isEmpty()) { addMapQueue.addAll(eligibleJobs); }
        addSuccQueue.clear();
      }

      // Advance the current time to the minimum of free slot event times.
      if (mapQueue.peek() != null || redQueue.peek() != null) {
        long freeMap = Long.MAX_VALUE;
        long freeRed = Long.MAX_VALUE;
        if (mapQueue.peek() != null) { freeMap = mapQueue.peek().time; }
        if (redQueue.peek() != null) { freeRed = redQueue.peek().time; }
        currentTime = Math.min(freeRed, freeMap);
        LOG.info("Updated current time to " + currentTime + ".");
      }
    }

    // Inform the user about the schedule.
    LOG.info("!!! Simulation complete. !!!");
    LOG.info("Workflow total cost: " + workflowDag.getCost(table));
    LOG.info("Workflow total time: " + workflowDag.getTime(table));
    LOG.info("Workflow budget constraint: N/A");
    LOG.info("Workflow deadline constraint: N/A");

    LOG.info("Events are as follows:");
    List<SchedulingEvent> events = Arrays.asList(scheduleEvents.toArray(new SchedulingEvent[0]));
    Collections.sort(events);
    for (SchedulingEvent event : events) {
      LOG.info(event.jobName + ": " + event.numMaps + " maps, "
          + event.numReduces + " reduces, at " + event.time);
    }
    LOG.info("Done listing events.");

    LOG.info("Task Mapping:");
    for (String jobName : taskMapping.keySet()) {
      WorkflowNode node = taskMapping.get(jobName);
      for (WorkflowTask task : node.getTasks()) {
        LOG.info("Mapped " + jobName + "." + task.getName() + " to: "
            + task.getMachineType());
      }
    }

    // Not meeting any constraints, so schedule is always valid.
    return true;
  }

  // Set the number of scheduled tasks to the selected machine type.
  private void setMachineTypes(String machine, WorkflowNode node, int numTasks, boolean isMap) {
    int setTasks = 0;
    Collection<WorkflowTask> tasks = (isMap ? node.getMapTasks() : node.getReduceTasks());

    for (WorkflowTask task : tasks) {
      if (task.getMachineType() == null) {
        task.setMachineType(machine);
        setTasks++;
      }
      if (setTasks == numTasks) { break; }
    }
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
    LOG.info("In runTask function (" + jobName + ").");
    LOG.info("Current time is: " + currentTime);

    // Get the next event to run (which starts before the current time).
    SchedulingEvent event = scheduleEvents.peek();
    if (event == null || event.time > currentTime) {
      LOG.info("Event is null or has time > currentTime.");
      return false;
    }
    LOG.info("Next scheduling event is " + event);

    // Don't allow jobs that haven't actually been submitted.
    // Also to find a match the names have to match :).
    if (!event.jobName.equals(jobName)) {
      LOG.info("Event job does not match job to be scheduled.");
      return false;
    }
    // Event must have proper tasks to still be in the requirement list.
    if (taskType == TaskType.MAP && event.numMaps == 0) {
      LOG.info("No maps left to schedule for event " + event.jobName);
      return false;
    }
    if (taskType == TaskType.REDUCE && event.numReduces == 0) {
      LOG.info("No reduces left to schedule for event" + event.jobName);
      return false;
    }
    LOG.info("Event " + event + " matches queued job & has tasks to be run.");

    // Get the tasks from the job.
    Collection<WorkflowTask> tasks = null;
    WorkflowNode job = taskMapping.get(jobName);
    if (taskType == TaskType.MAP) { tasks = job.getMapTasks(); }
    if (taskType == TaskType.REDUCE) { tasks = job.getReduceTasks(); }

    // Find a task that is supposed to run on the given machine type.
    for (WorkflowTask task : tasks) {
      if (machineType.equals(task.getMachineType())) {
        LOG.info("Found a match!");

        if (!isDryRun) {
          // Don't update structures if this is a dry-run check/test.
          if (taskType == TaskType.MAP) { event.numMaps--; }
          if (taskType == TaskType.REDUCE) { event.numReduces--; }
          tasks.remove(task);

          if (event.numMaps == 0 && event.numReduces == 0) {
            // Remove the event if it has no more tasks to be scheduled.
            LOG.info("Event has no more tasks, removing it.");
            scheduleEvents.remove(event);

            // Update time if the job has no tasks left (it is done).
            LOG.info("Job has no more tasks, updating time.");
            TableKey key = new TableKey(jobName, machineType, false);
            float execTime = table.get(key).execTime;
            if (currentTime < event.time + execTime) {
              currentTime = (long) Math.ceil(event.time + execTime);
            }
          }
        }

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
  public List<String> getExecutableJobs(Collection<String> finishedJobs) {

    LOG.info("Got as finished jobs: " + finishedJobs.toString());
    this.finishedJobs.addAll(finishedJobs);

    // Get the set of events that start at or before the current time.
    Set<SchedulingEvent> validEvents = new HashSet<SchedulingEvent>();
    List<String> validEventNames = new ArrayList<String>();

    SchedulingEvent event;
    while ((event = scheduleEvents.peek()) != null && event.time <= currentTime) {
      validEventNames.add(event.jobName);
      validEvents.add(event);
      scheduleEvents.poll();
    }
    scheduleEvents.addAll(validEvents);
    LOG.info("Valid events are: " + Arrays.toString(validEventNames.toArray()));

    // Ensure jobs with incomplete dependencies aren't returned.
    Iterator<String> validEventNamesIterator = validEventNames.iterator();
    while (validEventNamesIterator.hasNext()) {
      String eventName = validEventNamesIterator.next();
      LOG.info("Getting deps of job " + eventName);
      WorkflowNode eventNode = taskMapping.get(eventName);
      Set<WorkflowNode> predecessors = workflowDag.getPredecessors(eventNode);
      LOG.info("Deps are " + Arrays.toString(predecessors.toArray(new WorkflowNode[0])));
      Set<String> predecessorNames = new HashSet<String>();
      for (WorkflowNode node : predecessors) {
        predecessorNames.add(node.getJobName());
      }
      // We can't execute a job until all its predecessors are complete.
      if (!this.finishedJobs.containsAll(predecessorNames)) {
        LOG.info("Predecessors of " + eventName + " are not all finished, removing it.");
        LOG.info("Predecessors are: " + Arrays.toString(predecessorNames.toArray()));
        validEventNamesIterator.remove();
      }
    }
    LOG.info("Returned as executable jobs: " + validEventNames);

    return validEventNames;
  }

  @Override
  public Map<String, String> getTrackerMapping() {
    return trackerMapping;
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    // For generatePlan & class itself.
    workflowDag = new WorkflowDAG();
    workflowDag.readFields(in);

    // TODO: reflection to select whatever prioritizer concrete type
    prioritizer = new HighestLevelFirstPrioritizer();
    prioritizer.setWorkflowDag(workflowDag);
    prioritizer.readFields(in);

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

    // For match & executable job functions.
    table = new HashMap<TableKey, TableEntry>();
    int numTableEntries = in.readInt();
    for (int i = 0; i < numTableEntries; i++) {
      TableKey key = new TableKey();
      TableEntry entry = new TableEntry();
      key.readFields(in);
      entry.readFields(in);
      table.put(key, entry);
    }

    int numRequirements = in.readInt();
    for (int i = 0; i < numRequirements; i++) {
      SchedulingEvent event = new SchedulingEvent();
      event.readFields(in);
      scheduleEvents.add(event);
    }

    int numFinishedJobs = in.readInt();
    for (int i = 0; i < numFinishedJobs; i++) {
      finishedJobs.add(Text.readString(in));
    }

    currentTime = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {

    // For generatePlan & class itself.
    workflowDag.write(out);
    prioritizer.write(out);

    // Don't write out taskMapping as it's generated from the workflowDag.

    out.writeInt(trackerMapping.size());
    for (String key : trackerMapping.keySet()) {
      Text.writeString(out, key);
      Text.writeString(out, trackerMapping.get(key));
    }

    // For match & executable job functions.
    out.writeInt(table.size());
    for (TableKey key : table.keySet()) {
      key.write(out);
      table.get(key).write(out);
    }

    out.writeInt(scheduleEvents.size());
    for (SchedulingEvent event : scheduleEvents) { event.write(out); }

    out.writeInt(finishedJobs.size());
    for (String job : finishedJobs) { Text.writeString(out, job); }

    out.writeLong(currentTime);
  }

}