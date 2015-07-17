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

// A custom scheduler is not needed as we only deal with one workflow at a time.
//
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

  private static enum EventType { FREE, ADD };

  // Events related to the simulation of task assignment: jobs being added, and
  // jobs being completed (slots being freed).
  private abstract class QueueEvent implements Comparable<QueueEvent> {
    public long time;
    protected EventType eventType;

    public QueueEvent(long time, EventType eventType) {
      this.time = time;
      this.eventType = eventType;
    }

    @Override
    public int compareTo(QueueEvent other) {
      if (time > other.time) { return 1; }
      if (time < other.time) { return -1; }
      if (time == other.time) {
        if (eventType.equals(EventType.FREE)) { return -1; } else { return 1; }
      }
      return 0;
    }
  }

  private class AddEvent extends QueueEvent {
    public WorkflowNode job;

    /** At the provided time, the specified jobs can be started. */
    public AddEvent(long time, WorkflowNode job) {
      super(time, EventType.ADD);
      this.job = job;
    }
  }

  private class FreeEvent extends QueueEvent {
    public int freedMapSlots = 0;
    public int freedRedSlots = 0;

    /** At the provided time, the specified number of slots are freed. */
    public FreeEvent(long time, int freedMapSlots, int freedRedSlots) {
      super(time, EventType.FREE);
      this.freedMapSlots = freedMapSlots;
      this.freedRedSlots = freedRedSlots;
    }
  }

  private List<AddEvent> createAddEvents(long time, List<WorkflowNode> nodes) {
    List<AddEvent> events = new ArrayList<AddEvent>();
    for (WorkflowNode node : nodes) { events.add(new AddEvent(time, node)); }
    return events;
  }

  private static final Log LOG = LogFactory.getLog(ProgressBasedSchedulingPlan.class);

  // For generatePlan & class itself.
  private WorkflowDAG workflowDag;
  private Map<String, WorkflowNode> taskMapping;  // job -> wfNode (tasks have machine)
  private Map<String, String> trackerMapping;  // trackerName -> machineType

  // For match functions.
  private long currentTime = 0;
  private Map<TableKey, TableEntry> table;
  private Queue<SchedulingEvent> scheduleEvents;

  // For getExecutableJobs.
  // finishedJobs -> executableJobs.
  private Map<Collection<String>, List<String>> executableJobMap;

  public ProgressBasedSchedulingPlan() {
    taskMapping = new HashMap<String, WorkflowNode>();
    scheduleEvents = new PriorityQueue<SchedulingEvent>();
    executableJobMap = new HashMap<Collection<String>, List<String>>();
  }

  @Override
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) throws IOException {

    this.table = table;

    // Get a mapping between actual available machines and machine types.
    trackerMapping = WorkflowUtil.matchResourceTypes(machineTypes, machines);

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
    MachineType fastestMachine = null;
    for (MachineType type : machineTypes) {
      if (fastestMachine == null
          || type.getChargeRate() > fastestMachine.getChargeRate()) {
        fastestMachine = type;
      }
    }

    // Keep track of the finished workflowNodes.
    Collection<WorkflowNode> finishedJobs = new HashSet<WorkflowNode>();

    // Get the workflow DAG corresponding to the workflow configuration, &c.
    workflowDag = WorkflowDAG.construct(machineTypes, machines, workflow);
    LOG.info("Constructed WorkflowDAG.");
    
    // Prioritize the jobs/nodes in the workflow dag by our criterion.
    // TODO

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

    // Custom priority queue based on job priority metric,
    // determines next jobs to execute.
    // TODO: get starting nodes in order from prioritization
    Queue<AddEvent> addQueue = new PriorityQueue<AddEvent>();
    addQueue.addAll(createAddEvents(0, new ArrayList<WorkflowNode>(workflowDag.getEntryNodes())));

    long currentTime = 0L;
    int freeMapSlots = 0;
    int freeRedSlots = 0;

    // Main loop, simulating execution on the slots.
    while (!addQueue.isEmpty()) {

      // Free any slots at the current time.
      FreeEvent free;
      while ((free = mapQueue.peek()) != null && free.time <= currentTime) {
        freeMapSlots += mapQueue.poll().freedMapSlots;
      }
      while ((free = redQueue.peek()) != null && free.time <= currentTime) {
        freeRedSlots += redQueue.poll().freedRedSlots;
      }

      Set<AddEvent> unfinishedAddEvents = new HashSet<AddEvent>();
      Queue<AddEvent> addRedQueue = new PriorityQueue<AddEvent>(); // TODO: custom comparator here for priorities
      Queue<AddEvent> addSuccQueue = new PriorityQueue<AddEvent>(); // TODO: custom comparator here for priorities

      // While there exist free slots AND we've not seen all jobs,
      // schedule tasks starting from high priority jobs.
      while (freeMapSlots > 0 && !addQueue.isEmpty()) {

        // Get the next highest priority job.
        AddEvent addEvent = addQueue.poll();
        WorkflowNode job = addEvent.job;
        String jobName = job.getJobName();

        // If the job has map tasks then schedule as many possible.
        int numMapTasks = mapTasks.get(jobName);
        if (numMapTasks > 0) {

          // Compute the number of maps that can be scheduled.
          int mapsToSchedule = Math.min(numMapTasks, freeMapSlots);
          int remainingMaps = numMapTasks - mapsToSchedule;

          // More tasks need to be scheduled after time is advanced.
          if (remainingMaps > 0) { unfinishedAddEvents.add(addEvent); }

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
        }
        // If the job doesn't have map tasks to schedule then we want
        // to schedule reduce events (at the current time).
        else {
          addRedQueue.add(addEvent);
        }
      }

      // While there exist free slots AND we've not seen all jobs,
      // schedule tasks starting from high priority jobs.
      // Can't consider previously scheduled maps (above) without advancing
      // time, so we use a collection containing filtered add events.
      while (freeRedSlots > 0 && !addRedQueue.isEmpty()) {

        // Get the next highest priority job.
        AddEvent addEvent = addRedQueue.poll();
        WorkflowNode job = addEvent.job;
        String jobName = job.getJobName();

        // If the job has reduce tasks then schedule as many as possible.
        int numRedTasks = redTasks.get(jobName);
        if (numRedTasks > 0) {

          // Compute the number of reduces that can be scheduled.
          int redsToSchedule = Math.min(numRedTasks, freeRedSlots);
          int remainingReds = numRedTasks - redsToSchedule;

          // More tasks need to be scheduled after time is advanced.
          if (remainingReds > 0) { unfinishedAddEvents.add(addEvent); }

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
        }
        // If the job doesn't have reduce tasks then we want to add its
        // successors to the collection of executable jobs.
        else {
          addSuccQueue.add(addEvent);
        }
      }

      // Add any successor jobs.
      // TODO: how to do this?
      // --> each set of new eligible jobs need to start after the just finished event
      // --> keep map of eligible job to just finished events
      // --> starts at latest? earliest? time of all finished events
      // .....
      Set<WorkflowNode> eligibleJobs = new HashSet<WorkflowNode>();
      while (!addSuccQueue.isEmpty()) {
        WorkflowNode job = addSuccQueue.poll().job;
        finishedJobs.add(job);
        eligibleJobs.addAll(getEligibleJobs(finishedJobs, job));
      }
      if (!eligibleJobs.isEmpty()) {
        // TODO: priorities how?
        addQueue.addAll(createAddEvents(0, new ArrayList<WorkflowNode>(eligibleJobs)));
      }

      // Add all of the jobs which still have tasks to run back to the queue.
      addQueue.addAll(unfinishedAddEvents);

      // Advance the current time to the minimum of free slot event times.
      // TODO: Shouldn't be a case where both are null.
      long freeMap = (mapQueue.peek() == null) ? Long.MAX_VALUE : mapQueue.peek().time;
      long freeRed = (redQueue.peek() == null) ? Long.MAX_VALUE : redQueue.peek().time;
      currentTime = Math.min(freeMap, freeRed);
    }

    // Set the taskMapping variable.
    for (WorkflowNode node : workflowDag.getNodes()) {
      taskMapping.put(node.getJobName(), node);
    }

    // Inform the user about the schedule.
    LOG.info("!!! Simulation complete. !!!");
    LOG.info("Workflow total cost: " + workflowDag.getCost(table));
    LOG.info("Workflow total time: " + workflowDag.getTime(table));
    LOG.info("Workflow budget constraint: N/A");
    LOG.info("Workflow deadline constraint: N/A");

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

  // Get the successor jobs of a node that are eligible for execution given a
  // collection of finished jobs.
  // TODO: priorities
  private List<WorkflowNode> getEligibleJobs(
      Collection<WorkflowNode> finishedJobs, WorkflowNode node) {

    List<WorkflowNode> eligibleJobs = new ArrayList<WorkflowNode>();

    for (WorkflowNode succ : workflowDag.getSuccessors(node)) {
      boolean eligible = true;
      for (WorkflowNode dep : workflowDag.getPredecessors(succ)) {
        if (!finishedJobs.contains(dep)) { eligible = false; }
      }
      if (eligible) { eligibleJobs.add(succ); }
    }

    executableJobMap.put(nodesToNames(finishedJobs), nodesToNames(eligibleJobs));
    return eligibleJobs;
  }

  // TODO: Enable priorities.
  private List<String> nodesToNames(Collection<WorkflowNode> nodes) {
    List<String> nodeNames = new ArrayList<String>();
    for (WorkflowNode node : nodes) { nodeNames.add(node.getJobName()); }
    return nodeNames;
  }

  @Override
  public boolean matchMap(String machineType, String jobName) {
    LOG.info("In matchMap function");

    // Get the set of events to start at or before the current time.
    Set<SchedulingEvent> validEvents = new HashSet<SchedulingEvent>();
    while (scheduleEvents.peek().time <= currentTime) {
      validEvents.add(scheduleEvents.poll());
    }
    scheduleEvents.addAll(validEvents);
    LOG.info("Valid events are: " + Arrays.toString(validEvents.toArray(new SchedulingEvent[0])));

    // Find a job add/execution that matches.
    for (SchedulingEvent event : validEvents) {
      // Event must have maps to still be in the requirement list.
      if (event.numMaps == 0) { continue; }
      if (!event.jobName.equals(jobName)) { continue; }

      LOG.info("Event " + event + " matches queued job & has maps to be run.");
      Collection<WorkflowTask> tasks = taskMapping.get(jobName).getMapTasks();

      // Find a task that is supposed to run on the given machine type.
      for (WorkflowTask task : tasks) {
        if (machineType.equals(task.getMachineType())) {
          LOG.info("Found a match: " + task);
          event.numMaps--;
          tasks.remove(task);

          if (event.numMaps == 0 && event.numReduces == 0) {
            LOG.info("Event has no more tasks, removing it.");
            scheduleEvents.remove(event);

            // Update time.
            TableKey key = new TableKey(jobName, machineType, true);
            float execTime = table.get(key).execTime;
            if (currentTime < execTime) {
              currentTime = (long) Math.floor(execTime);
            }
          }

          return true;
        }
      }
    }

    return false;
  }

  @Override
  public boolean matchReduce(String machineType, String jobName) {
    LOG.info("In matchReduce function");

    // Get the set of events to start at or before the current time.
    Set<SchedulingEvent> validEvents = new HashSet<SchedulingEvent>();
    while (scheduleEvents.peek().time <= currentTime) {
      validEvents.add(scheduleEvents.poll());
    }
    scheduleEvents.addAll(validEvents);
    LOG.info("Valid events are: " + Arrays.toString(validEvents.toArray(new SchedulingEvent[0])));

    // Find a job add/execution that matches.
    for (SchedulingEvent event : validEvents) {
      // Event must have reduces to still be in the requirement list.
      if (event.numReduces == 0) { continue; }
      if (!event.jobName.equals(jobName)) { continue; }

      LOG.info("Event " + event + " matches queued job & has reduces to be run.");
      Collection<WorkflowTask> tasks = taskMapping.get(jobName).getReduceTasks();

      // Find a task that is supposed to run on the given machine type.
      for (WorkflowTask task : tasks) {
        if (machineType.equals(task.getMachineType())) {
          LOG.info("Found a match: " + task);
          event.numReduces--;
          tasks.remove(task);

          if (event.numMaps == 0 && event.numReduces == 0) {
            LOG.info("Event has no more tasks, removing it.");
            scheduleEvents.remove(event);

            // Update time.
            TableKey key = new TableKey(jobName, machineType, false);
            float execTime = table.get(key).execTime;
            if (currentTime < execTime) {
              currentTime = (long) Math.floor(execTime);
            }
          }

          return true;
        }
      }
    }

    return false;
  }

  @Override
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

    // TODO: It works... maybe should use some sort of test against contained
    // elements rather than a whole set check though.
    LOG.info("Got as finished jobs: " + finishedJobs.toString());
    LOG.info("Returned as executable jobs: " + executableJobMap.get(finishedJobs).toString());
    return executableJobMap.get(finishedJobs);
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

    // For match functions.
    currentTime = in.readLong();

    int numRequirements = in.readInt();
    for (int i = 0; i < numRequirements; i++) {
      SchedulingEvent event = new SchedulingEvent();
      event.readFields(in);
      scheduleEvents.add(event);
    }

    table = new HashMap<TableKey, TableEntry>();
    int numTableEntries = in.readInt();
    for (int i = 0; i < numTableEntries; i++) {
      TableKey key = new TableKey();
      TableEntry entry = new TableEntry();
      key.readFields(in);
      entry.readFields(in);
      table.put(key, entry);
    }

    // For getExecutableJobs.
    int numJobEntries = in.readInt();
    for (int i = 0; i < numJobEntries; i++) {
      Collection<String> key = new HashSet<String>();
      List<String> value = new ArrayList<String>();
      int keySize = in.readInt();
      int valueSize = in.readInt();
      for (int j = 0; j < keySize; j++) { key.add(Text.readString(in)); }
      for (int j = 0; j < valueSize; j++) { value.add(Text.readString(in)); }
       executableJobMap.put(key, value); 
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    // For generatePlan & class itself.
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

    // For match functions.
    out.writeLong(currentTime);

    out.writeInt(scheduleEvents.size());
    for (SchedulingEvent event : scheduleEvents) { event.write(out); }

    out.writeInt(table.size());
    for (TableKey key : table.keySet()) {
      key.write(out);
      table.get(key).write(out);
    }

    // For getExecutableJobs.
    out.writeInt(executableJobMap.size());
    for (Collection<String> key : executableJobMap.keySet()) {
      Collection<String> value = executableJobMap.get(key);
      out.writeInt(key.size());
      out.writeInt(value.size());
      for (String keyComponent : key) { Text.writeString(out, keyComponent); }
      for (String valueComponent : value) { Text.writeString(out, valueComponent); }
    }
  }

}