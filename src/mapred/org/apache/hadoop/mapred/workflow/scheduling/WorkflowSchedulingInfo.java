package org.apache.hadoop.mapred.workflow.scheduling;

import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowStatus;


public class WorkflowSchedulingInfo extends SchedulingInfo {

  private WorkflowID id;

  public WorkflowSchedulingInfo(WorkflowStatus status) {
    super(status.getStartTime());
    id = status.getWorkflowId();
  }

  public WorkflowID getWorkflowId() {
    return id;
  }

  @Override
  public String toString() {
    return "WorkflowSchedulingInfo: id: " + id.toString() + " startTime: "
        + getStartTime();
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) { return false; }
    if (object.getClass() != WorkflowSchedulingInfo.class) { return false; }
    if (object == this) { return true; }

    if (object instanceof WorkflowSchedulingInfo) {
      WorkflowSchedulingInfo other = (WorkflowSchedulingInfo) object;
      return id.equals(other.id) && getStartTime() == other.getStartTime();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

}