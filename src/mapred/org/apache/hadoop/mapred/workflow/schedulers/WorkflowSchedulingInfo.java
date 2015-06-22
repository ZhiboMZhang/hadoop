package org.apache.hadoop.mapred.workflow.schedulers;

import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowStatus;

class WorkflowSchedulingInfo extends SchedulingInfo {
  private WorkflowID id;

  public WorkflowSchedulingInfo(WorkflowStatus status) {
    super(status.getSubmissionTime());
    id = status.getWorkflowId();
  }

  public WorkflowID getWorkflowId() {
    return id;
  }

  @Override
  public boolean equals(Object object) {
    if (!super.equals(object)) {
      return false;
    }
    if (object == null || object.getClass() != WorkflowSchedulingInfo.class) {
      return false;
    } else if (object == this) {
      return true;
    } else if (object instanceof WorkflowSchedulingInfo) {
      WorkflowSchedulingInfo other = (WorkflowSchedulingInfo) object;
      return (id.equals(other.id));
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (id.hashCode() + getStartTime());
  }

}