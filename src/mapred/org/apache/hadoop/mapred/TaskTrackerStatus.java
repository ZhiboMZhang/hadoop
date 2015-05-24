/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.mapred.TaskStatus.State;

/**************************************************
 * A TaskTrackerStatus is a MapReduce primitive.  Keeps
 * info on a TaskTracker.  The JobTracker maintains a set
 * of the most recent TaskTrackerStatus objects for each
 * unique TaskTracker it knows about.
 *
 * This is NOT a public interface!
 **************************************************/
public class TaskTrackerStatus implements Writable {
  public static final Log LOG = LogFactory.getLog(TaskTrackerStatus.class);
  
  static {                                        // register a ctor
    WritableFactories.setFactory
      (TaskTrackerStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new TaskTrackerStatus(); }
       });
  }

  String trackerName;
  String host;
  int httpPort;
  int taskFailures;
  int dirFailures;
  List<TaskStatus> taskReports;

  volatile long lastSeen;
  private int maxMapTasks;
  private int maxReduceTasks;
  private TaskTrackerHealthStatus healthStatus;
  private ResourceStatus resStatus;
  
  /**
   */
  public TaskTrackerStatus() {
    taskReports = new ArrayList<TaskStatus>();
    resStatus = new ResourceStatus();
    this.healthStatus = new TaskTrackerHealthStatus();
  }

  TaskTrackerStatus(String trackerName, String host) {
    this();
    this.trackerName = trackerName;
    this.host = host;
  }

  /**
   */
  public TaskTrackerStatus(String trackerName, String host, 
                           int httpPort, List<TaskStatus> taskReports, 
                           int taskFailures, int dirFailures,
                           int maxMapTasks, int maxReduceTasks) {
    this.trackerName = trackerName;
    this.host = host;
    this.httpPort = httpPort;

    this.taskReports = new ArrayList<TaskStatus>(taskReports);
    this.taskFailures = taskFailures;
    this.dirFailures = dirFailures;
    this.maxMapTasks = maxMapTasks;
    this.maxReduceTasks = maxReduceTasks;
    this.resStatus = new ResourceStatus();
    this.healthStatus = new TaskTrackerHealthStatus();

    this.resStatus.setMaxMapSlots(maxMapTasks);
    this.resStatus.setMaxReduceSlots(maxReduceTasks);
  }

  /**
   */
  public String getTrackerName() {
    return trackerName;
  }
  /**
   */
  public String getHost() {
    return host;
  }

  /**
   * Get the port that this task tracker is serving http requests on.
   * @return the http port
   */
  public int getHttpPort() {
    return httpPort;
  }
    
  /**
   * Get the number of tasks that have failed on this tracker.
   * @return The number of failed tasks
   */
  public int getTaskFailures() {
    return taskFailures;
  }

  /**
   * Get the number of local directories that have failed on this tracker.
   * @return The number of failed local directories
   */
  public int getDirFailures() {
    return dirFailures;
  }
    
  /**
   * Get the current tasks at the TaskTracker.
   * Tasks are tracked by a {@link TaskStatus} object.
   * 
   * @return a list of {@link TaskStatus} representing 
   *         the current tasks at the TaskTracker.
   */
  public List<TaskStatus> getTaskReports() {
    return taskReports;
  }
   
  /**
   * Is the given task considered as 'running' ?
   * @param taskStatus
   * @return
   */
  private boolean isTaskRunning(TaskStatus taskStatus) {
    TaskStatus.State state = taskStatus.getRunState();
    return (state == State.RUNNING || state == State.UNASSIGNED || 
            taskStatus.inTaskCleanupPhase());
  }
  
  /**
   * Get the number of running map tasks.
   * @return the number of running map tasks
   */
  public int countMapTasks() {
    int mapCount = 0;
    for (TaskStatus ts : taskReports) {
      if (ts.getIsMap() && isTaskRunning(ts)) {
        mapCount++;
      }
    }
    return mapCount;
  }

  /**
   * Get the number of occupied map slots.
   * @return the number of occupied map slots
   */
  public int countOccupiedMapSlots() {
    int mapSlotsCount = 0;
    for (TaskStatus ts : taskReports) {
      if (ts.getIsMap() && isTaskRunning(ts)) {
        mapSlotsCount += ts.getNumSlots();
      }
    }
    return mapSlotsCount;
  }
  
  /**
   * Get available map slots.
   * @return available map slots
   */
  public int getAvailableMapSlots() {
    return getMaxMapSlots() - countOccupiedMapSlots();
  }
  
  /**
   * Get the number of running reduce tasks.
   * @return the number of running reduce tasks
   */
  public int countReduceTasks() {
    int reduceCount = 0;
    for (TaskStatus ts : taskReports) {
      if ((!ts.getIsMap()) && isTaskRunning(ts)) {
        reduceCount++;
      }
    }
    return reduceCount;
  }

  /**
   * Get the number of occupied reduce slots.
   * @return the number of occupied reduce slots
   */
  public int countOccupiedReduceSlots() {
    int reduceSlotsCount = 0;
    for (TaskStatus ts : taskReports) {
      if ((!ts.getIsMap()) && isTaskRunning(ts)) {
        reduceSlotsCount += ts.getNumSlots();
      }
    }
    return reduceSlotsCount;
  }
  
  /**
   * Get available reduce slots.
   * @return available reduce slots
   */
  public int getAvailableReduceSlots() {
    return getMaxReduceSlots() - countOccupiedReduceSlots();
  }
  

  /**
   */
  public long getLastSeen() {
    return lastSeen;
  }
  /**
   */
  public void setLastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  /**
   * Get the maximum map slots for this node.
   * @return the maximum map slots for this node
   */
  public int getMaxMapSlots() {
    return maxMapTasks;
  }
  
  /**
   * Get the maximum reduce slots for this node.
   * @return the maximum reduce slots for this node
   */
  public int getMaxReduceSlots() {
    return maxReduceTasks;
  }  
  
  /**
   * Return the {@link ResourceStatus} object configured with this
   * status.
   * 
   * @return the resource status
   */
  public ResourceStatus getResourceStatus() {
    return resStatus;
  }

  /**
   * Returns health status of the task tracker.
   * @return health status of Task Tracker
   */
  public TaskTrackerHealthStatus getHealthStatus() {
    return healthStatus;
  }

  /**
   * Static class which encapsulates the Node health
   * related fields.
   * 
   */
  /**
   * Static class which encapsulates the Node health
   * related fields.
   * 
   */
  static class TaskTrackerHealthStatus implements Writable {
    
    private boolean isNodeHealthy;
    
    private String healthReport;
    
    private long lastReported;
    
    public TaskTrackerHealthStatus(boolean isNodeHealthy, String healthReport,
        long lastReported) {
      this.isNodeHealthy = isNodeHealthy;
      this.healthReport = healthReport;
      this.lastReported = lastReported;
    }
    
    public TaskTrackerHealthStatus() {
      this.isNodeHealthy = true;
      this.healthReport = "";
      this.lastReported = System.currentTimeMillis();
    }

    /**
     * Sets whether or not a task tracker is healthy or not, based on the
     * output from the node health script.
     * 
     * @param isNodeHealthy
     */
    void setNodeHealthy(boolean isNodeHealthy) {
      this.isNodeHealthy = isNodeHealthy;
    }

    /**
     * Returns if node is healthy or not based on result from node health
     * script.
     * 
     * @return true if the node is healthy.
     */
    boolean isNodeHealthy() {
      return isNodeHealthy;
    }

    /**
     * Sets the health report based on the output from the health script.
     * 
     * @param healthReport
     *          String listing cause of failure.
     */
    void setHealthReport(String healthReport) {
      this.healthReport = healthReport;
    }

    /**
     * Returns the health report of the node if any, The health report is
     * only populated when the node is not healthy.
     * 
     * @return health report of the node if any
     */
    String getHealthReport() {
      return healthReport;
    }

    /**
     * Sets when the TT got its health information last 
     * from node health monitoring service.
     * 
     * @param lastReported last reported time by node 
     * health script
     */
    public void setLastReported(long lastReported) {
      this.lastReported = lastReported;
    }

    /**
     * Gets time of most recent node health update.
     * 
     * @return time stamp of most recent health update.
     */
    public long getLastReported() {
      return lastReported;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      isNodeHealthy = in.readBoolean();
      healthReport = Text.readString(in);
      lastReported = in.readLong();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(isNodeHealthy);
      Text.writeString(out, healthReport);
      out.writeLong(lastReported);
    }
    
  }
  
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, trackerName);
    Text.writeString(out, host);
    out.writeInt(httpPort);
    out.writeInt(taskFailures);
    out.writeInt(dirFailures);
    out.writeInt(maxMapTasks);
    out.writeInt(maxReduceTasks);
    resStatus.write(out);
    out.writeInt(taskReports.size());

    for (TaskStatus taskStatus : taskReports) {
      TaskStatus.writeTaskStatus(out, taskStatus);
    }
    getHealthStatus().write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.trackerName = Text.readString(in);
    this.host = Text.readString(in);
    this.httpPort = in.readInt();
    this.taskFailures = in.readInt();
    this.dirFailures = in.readInt();
    this.maxMapTasks = in.readInt();
    this.maxReduceTasks = in.readInt();
    resStatus.readFields(in);
    taskReports.clear();
    int numTasks = in.readInt();

    for (int i = 0; i < numTasks; i++) {
      taskReports.add(TaskStatus.readTaskStatus(in));
    }
    getHealthStatus().readFields(in);
  }
}
