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
  private Map<String, WorkflowNode> taskMapping;  // job -> wfNode (tasks have machine)
  private Map<String, String> trackerMapping;  // trackerName -> machineType
  private WorkflowPrioritizer prioritizer;

  // For match functions.
  private long currentTime = 0;
  private Map<TableKey, TableEntry> table;
  private Queue<SchedulingEvent> scheduleEvents;

  public ProgressBasedSchedulingPlan() {
    taskMapping = new HashMap<String, WorkflowNode>();
    scheduleEvents = new PriorityQueue<SchedulingEvent>();
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
    Queue<WorkflowNode> addQueue = new PriorityQueue<WorkflowNode>(11, prioritizer);
    addQueue.addAll(prioritizer.getExecutableJobs(new HashSet<WorkflowNode>()));
    LOG.info("The addQueue initially has " + addQueue.size() + " jobs.");

    long currentTime = 0L;
    int freeMapSlots = 0;
    int freeRedSlots = 0;

    // Main loop, simulating execution on the slots.
    while (!addQueue.isEmpty()) {

      // Free any slots at the current time.
      FreeEvent free;
      while ((free = mapQueue.peek()) != null && free.time <= currentTime) {
        freeMapSlots += mapQueue.poll().freedMapSlots;
        LOG.info("Added freed map slots.");
      }
      while ((free = redQueue.peek()) != null && free.time <= currentTime) {
        freeRedSlots += redQueue.poll().freedRedSlots;
        LOG.info("Added freed reduce slots.");
      }

      Set<WorkflowNode> unfinishedJobs = new HashSet<WorkflowNode>();
      Queue<WorkflowNode> addRedQueue = new PriorityQueue<WorkflowNode>(11, prioritizer);
      Queue<WorkflowNode> addSuccQueue = new PriorityQueue<WorkflowNode>(11, prioritizer);

      // While there exist free slots AND we've not seen all jobs,
      // schedule tasks starting from high priority jobs.
      while (freeMapSlots > 0 && !addQueue.isEmpty()) {
        LOG.info("Looking for map tasks to schedule.");

        // Get the next highest priority job.
        WorkflowNode job = addQueue.poll();
        String jobName = job.getJobName();
        LOG.info("Got next job to schedule maps for as " + jobName + ".");

        // If the job has map tasks then schedule as many possible.
        int numMapTasks = mapTasks.get(jobName);
        if (numMapTasks > 0) {

          // Compute the number of maps that can be scheduled.
          int mapsToSchedule = Math.min(numMapTasks, freeMapSlots);
          int remainingMaps = numMapTasks - mapsToSchedule;
          LOG.info("Scheduling " + mapsToSchedule + " maps from job " + jobName);

          // More tasks need to be scheduled after time is advanced.
          unfinishedJobs.add(job);

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
          // TODO: join so more even split between map & reduce
          // TODO: rather than all map happening before reduces start
          addRedQueue.add(job);
        }
      }

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

          // Compute the number of reduces that can be scheduled.
          int redsToSchedule = Math.min(numRedTasks, freeRedSlots);
          int remainingReds = numRedTasks - redsToSchedule;
          LOG.info("Scheduling " + redsToSchedule + " reds from job " + jobName);

          // More tasks need to be scheduled after time is advanced.
          unfinishedJobs.add(job);

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

      // Add any successor jobs.
      if (!addSuccQueue.isEmpty()) {
        LOG.info("There are successors that can possibly be run.");
        Set<WorkflowNode> finishedJobs = new HashSet<WorkflowNode>(
            Arrays.asList(addSuccQueue.toArray(new WorkflowNode[0])));

        List<WorkflowNode> eligibleJobs = prioritizer.getExecutableJobs(finishedJobs);
        if (!eligibleJobs.isEmpty()) {
          LOG.info(("New jobs can be run: " + Arrays.toString(eligibleJobs.toArray(new WorkflowNode[0]))));
          addQueue.addAll(eligibleJobs);
        }
      }

      // Add all of the jobs which still have tasks to run back to the queue.
      addQueue.addAll(unfinishedJobs);
      LOG.info("Added back unfinished jobs to queue: "
          + Arrays.toString(unfinishedJobs.toArray(new WorkflowNode[0])));

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

    LOG.info("Events are as follows:");
    List<SchedulingEvent> events = Arrays.asList(scheduleEvents.toArray(new SchedulingEvent[0]));
    Collections.sort(events);
    for (SchedulingEvent event : events) {
      LOG.info(event.jobName + ": " + event.numMaps + " maps, "
          + event.numReduces + " reduces, at " + event.time);
    }
    LOG.info("Done listing events.");

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

  @Override
  public boolean matchMap(String machineType, String jobName) {
    LOG.info("In matchMap function");
    LOG.info("Current time is: " + currentTime);

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
          }

          // Update time if the job has no map tasks left (it is done).
          if (tasks.isEmpty()) {
            TableKey key = new TableKey(jobName, machineType, false);
            float execTime = table.get(key).execTime;
            if (currentTime < event.time + execTime) {
              currentTime = (long) Math.ceil(event.time + execTime);
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
    LOG.info("Current time is: " + currentTime);

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
          }

          // Update time if the job has no reduce tasks left (it is done).
          if (tasks.isEmpty()) {
            TableKey key = new TableKey(jobName, machineType, false);
            float execTime = table.get(key).execTime;
            if (currentTime < event.time + execTime) {
              currentTime = (long) Math.ceil(event.time + execTime);
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

    // Get the set of events to start at or before the current time.
    Set<SchedulingEvent> validEvents = new HashSet<SchedulingEvent>();
    List<String> validEventNames = new ArrayList<String>();
    // TODO: i definitely need to take into account dependencies,
    // TODO: so that jobs don't start until all deps are finished.
    while (scheduleEvents.peek().time <= currentTime) {
      SchedulingEvent event = scheduleEvents.poll();
      validEventNames.add(event.jobName);
      validEvents.add(event);
    }
    scheduleEvents.addAll(validEvents);
    LOG.info("Valid events are: " + Arrays.toString(validEvents.toArray(new SchedulingEvent[0])));

    //LOG.info("Got as finished jobs: " + finishedJobs.toString());
    //List<String> executableJobs = prioritizer.getExecutableJobs(finishedJobs);
    //executableJobs.removeAll(validEventNames);
    //LOG.info("Returned as executable jobs: " + executableJobs);
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
  }

  @Override
  public void write(DataOutput out) throws IOException {

    // For generatePlan & class itself.
    workflowDag.write(out);
    prioritizer.write(out);

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
  }

}