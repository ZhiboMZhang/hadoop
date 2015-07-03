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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

// Only dealing with a single workflow at a time, so don't worry about
// implementing a custom scheduler.
public class ProgressBasedSchedulingPlan extends WorkflowSchedulingPlan {

  private class SchedulingEvent implements Writable {
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
  }

  private static enum EventType { FREE, ADD };

  // Events related to the simulation of task assignment: jobs being added, and
  // jobs being completed (slots being freed).
  private class QueueEvent implements Comparable<QueueEvent> {
    public long time;
    public EventType type;

    public int freedMapSlots = 0;
    public int freedRedSlots = 0;
    public Collection<WorkflowNode> jobs = new ArrayList<WorkflowNode>();

    /** At the provided time, the specified number of slots are freed. */
    public QueueEvent(long time, int freedMapSlots, int freedRedSlots) {
      this.time = time;
      this.freedMapSlots = freedMapSlots;
      this.freedRedSlots = freedRedSlots;
      this.type = EventType.FREE;
    }

    /** At the provided time, the specified jobs can be started. */
    public QueueEvent(long time, Collection<WorkflowNode> jobs) {
      this.time = time;
      this.jobs.addAll(jobs);
      this.type = EventType.ADD;
    }

    @Override
    public int compareTo(QueueEvent other) {
      if (time > other.time) { return 1; }
      if (time < other.time) { return -1; }
      if (time == other.time) {
        // Free events occur before add events if they have the same time.
        if (type == EventType.FREE && other.type == EventType.ADD) { return -1; }
        return 0;
      }
      return 0;
    }
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
  private Map<Collection<String>, Collection<String>> executableJobMap;

  public ProgressBasedSchedulingPlan() {
    taskMapping = new HashMap<String, WorkflowNode>();
    scheduleEvents = new PriorityQueue<SchedulingEvent>();
    executableJobMap = new HashMap<Collection<String>, Collection<String>>();
  }

  @Override
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table,
      WorkflowConf workflow) throws IOException {

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
    
    // Construct a map to hold the number of scheduled tasks.
    // jobname -> number of unscheduled tasks
    Map<String, Integer> mapTasks = new HashMap<String, Integer>();
    Map<String, Integer> redTasks = new HashMap<String, Integer>();
    
    for (WorkflowNode node : workflowDag.getNodes()) {
      mapTasks.put(node.getJobName(), node.getMapTasks().size());
      redTasks.put(node.getJobName(), node.getReduceTasks().size());
    }
    LOG.info("Created jobname to unscheduled tasks map.");

    // Compute the total number of slots.
    int mapSlots = 0;
    int redSlots = 0;

    for (ResourceStatus status : machines.values()) {
      mapSlots += status.getMaxMapSlots();
      redSlots += status.getMaxReduceSlots();
    }
    LOG.info("Computed total number of map (" + mapSlots + ") and reduce ("
        + redSlots + ") slots.");

    // Event queue to hold system events in ascending order of occurrence time.
    Queue<QueueEvent> eventQueue = new PriorityQueue<QueueEvent>();
    eventQueue.add(new QueueEvent(0, mapSlots, redSlots));
    eventQueue.add(new QueueEvent(0, workflowDag.getEntryNodes()));

    Queue<WorkflowNode> activeJobs = new PriorityQueue<WorkflowNode>();
    long time = 0L;

    // TODO: starvation bc/ free slot event after job add event @ same time ??
    // Main loop, simulating execution on the slots.
    while (!eventQueue.isEmpty()) {
      QueueEvent event = eventQueue.poll();
      time = event.time;

      // Process the event.
      if (event.type.equals(EventType.FREE)) {
        mapSlots += event.freedMapSlots;
        redSlots += event.freedRedSlots;
      } else {
        // Get the currently finished jobs.
        // TODO: this work?
        HashSet<String> finishedJobNames = new HashSet<String>();
        for (WorkflowNode node : finishedJobs) {
          finishedJobNames.add(node.getJobName());
        }
        // Consider adding more executable jobs if finishedJobs hasn't changed.
        Collection<String> executableJobNames = executableJobMap.get(finishedJobNames);
        if (executableJobNames == null) {
          executableJobNames = new HashSet<String>();
          executableJobMap.put(finishedJobNames, executableJobNames);
        }
        for (WorkflowNode node : event.jobs) {
          executableJobNames.add(node.getJobName());
        }

        activeJobs.addAll(event.jobs);
      }
      
      if (activeJobs.isEmpty()) { continue; }

      if ((mapSlots + redSlots) > 0) {
        // Slots are available for scheduling.
        WorkflowNode node = activeJobs.peek();
        int nodeMapTasks = mapTasks.get(node.getJobName());
        int nodeRedTasks = redTasks.get(node.getJobName());

        if (nodeMapTasks > 0) {
          // Schedule map tasks.
          int scheduledMaps = Math.min(nodeMapTasks, mapSlots);
          scheduleEvents.add(
              new SchedulingEvent(time, node.getJobName(), scheduledMaps, 0));
          mapSlots -= scheduledMaps;
          mapTasks.put(node.getJobName(), (nodeMapTasks - scheduledMaps));

          if (scheduledMaps > 0) {
            // Select a machine type to run the scheduled map tasks on.
            MachineType selectedMachine = fastestMachine;
            
            // Set the number of scheduled tasks to the selected machine type.
            int setTasks = 0;
            for (WorkflowTask task : node.getMapTasks()) {
              if (task.getMachineType() == null) {
                task.setMachineType(selectedMachine.getName());
                setTasks++;
              }
              if (setTasks == scheduledMaps) { break; }
            }

            TableKey nodeMapKey =
                new TableKey(node.getJobName(), selectedMachine.getName(), true);
            TableEntry entry = table.get(nodeMapKey);
            eventQueue.add(new QueueEvent(time + entry.execTime, scheduledMaps, 0));
          }
        } else {
          // Schedule reduce tasks.
          int scheduledReds = Math.min(nodeRedTasks, redSlots);
          scheduleEvents.add(new SchedulingEvent(time, node.getJobName(), 0,
              scheduledReds));
          redSlots -= scheduledReds;
          redTasks.put(node.getJobName(), (nodeRedTasks - scheduledReds));

          // Select a machine type to run the reduce tasks on.
          MachineType selectedMachine = fastestMachine;

          // Set the number of scheduled tasks to the selected machine type.
          int setTasks = 0;
          for (WorkflowTask task : node.getReduceTasks()) {
            if (task.getMachineType() == null) {
              task.setMachineType(selectedMachine.getName());
              setTasks++;
            }
            if (setTasks == scheduledReds) {
              break;
            }
          }

          TableKey nodeRedKey =
              new TableKey(node.getJobName(), selectedMachine.getName(), false);
          TableEntry entry = table.get(nodeRedKey);
          eventQueue.add(new QueueEvent(time + entry.execTime, 0, scheduledReds));

          // If all reduces have been scheduled then we can add new jobs.
          if (scheduledReds == nodeRedTasks) {
            // Get the set of successor nodes that can now be run.
            Collection<WorkflowNode> eligibleJobs = new HashSet<WorkflowNode>();
            Set<WorkflowNode> succs = workflowDag.getSuccessors(node);
            for (WorkflowNode succ : succs) {
              boolean eligible = true;
              for (WorkflowNode dep : workflowDag.getPredecessors(succ)) {
                if (!finishedJobs.contains(dep)) {
                  eligible = false;
                }
              }
              if (eligible) { eligibleJobs.add(succ); }
            }

            eventQueue.add(new QueueEvent(time + entry.execTime, eligibleJobs));
            activeJobs.remove(node);
            finishedJobs.add(node);
          }
        }
      }
    }

    // Set the taskMapping variable.
    for (WorkflowNode node : workflowDag.getNodes()) {
      taskMapping.put(node.getJobName(), node);
      LOG.info("Added pair: " + node.getJobName() + "/" + node);
    }

    // Not meeting any constraints, so schedule is always valid.
    return true;
  }

  @Override
  public boolean matchMap(String machineType, String jobName) {
    LOG.info("In matchMap function");

    // Get the set of events to start at or before the current time.
    Set<SchedulingEvent> validEvents = new HashSet<SchedulingEvent>();
    for (SchedulingEvent event = scheduleEvents.peek();
        event.time <= currentTime;
        validEvents.add(event));

    // Find a job add/execution that matches.
    for (SchedulingEvent event : validEvents) {
      // Event must have reduces to still be in the requirement list.
      if (event.numMaps == 0) { continue; }
      if (!event.jobName.equals(jobName)) { continue; }

      Collection<WorkflowTask> tasks = taskMapping.get(jobName).getMapTasks();

      // Find a task that is supposed to run on the given machine type.
      for (WorkflowTask task : tasks) {
        if (machineType.equals(task.getMachineType())) {
          event.numMaps--;
          tasks.remove(task);

          if (event.numMaps == 0 && event.numReduces == 0) {
            scheduleEvents.remove(event);

            // Update time.
            TableKey key = new TableKey(jobName, machineType, true);
            float execTime = table.get(key).execTime;
            if (currentTime < execTime) {
              currentTime = (int) Math.floor(execTime);
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
    for (SchedulingEvent event = scheduleEvents.peek();
        event.time <= currentTime;
        validEvents.add(event));

    // Find a job add/execution that matches.
    for (SchedulingEvent event : validEvents) {
      // Event must have maps to still be in the requirement list.
      if (event.numReduces == 0) { continue; }
      if (!event.jobName.equals(jobName)) { continue; }

      Collection<WorkflowTask> tasks = taskMapping.get(jobName).getReduceTasks();

      // Find a task that is supposed to run on the given machine type.
      for (WorkflowTask task : tasks) {
        if (machineType.equals(task.getMachineType())) {
          event.numReduces--;
          tasks.remove(task);

          if (event.numMaps == 0 && event.numReduces == 0) {
            scheduleEvents.remove(event);

            // Update time.
            TableKey key = new TableKey(jobName, machineType, false);
            float execTime = table.get(key).execTime;
            if (currentTime < execTime) {
              currentTime = (int) Math.floor(execTime);
            }
          }

          return true;
        }
      }
    }

    return false;
  }

  @Override
  public Collection<String> getExecutableJobs(Collection<String> finishedJobs) {
    // TODO: test
    return executableJobMap.get(new HashSet<String>(finishedJobs));
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
      Collection<String> value = new HashSet<String>();
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