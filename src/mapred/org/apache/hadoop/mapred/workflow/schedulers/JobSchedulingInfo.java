package org.apache.hadoop.mapred.workflow.schedulers;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;

// See JobQueueJobInProgressListener
class JobSchedulingInfo extends SchedulingInfo {
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
  public boolean equals(Object object) {
    if (!super.equals(object)) { return false; }
    if (object == null || object.getClass() != JobSchedulingInfo.class) {
      return false;
    } else if (object == this) {
      return true;
    } else if (object instanceof JobSchedulingInfo) {
      JobSchedulingInfo other = (JobSchedulingInfo) object;
      return (id.equals(other.id) && priority == other.priority);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (id.hashCode() * priority.hashCode() + getStartTime());
  }
}