package org.apache.hadoop.mapred.workflow.scheduling;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;

// See JobQueueJobInProgressListener
public class JobSchedulingInfo extends SchedulingInfo {

  private JobPriority priority;
  private JobID id;

  public JobSchedulingInfo(JobStatus status) {
    super(status.getStartTime());
    priority = status.getJobPriority();
    id = status.getJobID();
  }

  public JobPriority getPriority() {
    return priority;
  }

  public JobID getJobId() {
    return id;
  }

  @Override
  public String toString() {
    return "JobSchedulingInfo: id: " + id.toString() + " priority: "
        + priority.toString() + " startTime: " + getStartTime();
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) { return false; }
    if (object.getClass() != JobSchedulingInfo.class) {  return false; }
    if (object == this) { return true; }

    if (object instanceof JobSchedulingInfo) {
      JobSchedulingInfo other = (JobSchedulingInfo) object;
      return id.equals(other.id) && priority == other.priority
          && getStartTime() == other.getStartTime();
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (id.hashCode() * priority.hashCode());
  }
}