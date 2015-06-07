package org.apache.hadoop.mapred.workflow.schedulers;

/**
 * Represent a node/stage in a {@link WorkflowDAG}.
 */
public class WorkflowNode {

  private String name;
  private String job;
  private boolean isMapStage = true;
  private String machineTypes[];

  public WorkflowNode(String job, int numTasks, boolean isMapStage) {
    this.job = job;
    this.isMapStage = isMapStage;

    this.name = job + (isMapStage ? ".map" : ".red");
    this.machineTypes = new String[numTasks];
  }

  public String getName() {
    return name;
  }

  public String getJob() {
    return job;
  }

  public boolean isMapStage() {
    return isMapStage;
  }

  public int getNumTasks() {
    return machineTypes.length;
  }

  /**
   * Get the machine type of a given task (identified by a value 0 -> numTasks).
   * 
   * @param taskNumber The task in the stage to get a machine type for.
   * 
   * @return The string identifying the machine type, or null if an invalid task
   *         value was input.
   */
  public String getMachineType(int taskNumber) {
    if (taskNumber < 0 || taskNumber >= machineTypes.length) {
      return null;
    }
    return machineTypes[taskNumber];
  }

  /**
   * Set the machine type of the given task (identified by a value 0 ->
   * numTasks).
   * 
   * @param taskNumber The task in the stage to set a machine type for.
   * @param machineType The name of the machine type to use for the task.
   * 
   * @return True if the machine type was set, false otherwise.
   */
  public boolean setMachineType(int taskNumber, String machineType) {
    if (taskNumber >= 0 && taskNumber < machineTypes.length) {
      machineTypes[taskNumber] = machineType;
      return true;
    }
    return false;
  }

}