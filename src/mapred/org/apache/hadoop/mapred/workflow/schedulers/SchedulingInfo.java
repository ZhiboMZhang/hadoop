package org.apache.hadoop.mapred.workflow.schedulers;

class SchedulingInfo {
  private long startTime;

  public SchedulingInfo(long startTime) {
    this.startTime = startTime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public boolean equals(Object object) {
    if (null == object || object.getClass() != SchedulingInfo.class) {
      return false;
    } else if (this == object) {
      return true;
    } else if (object instanceof SchedulingInfo) {
      SchedulingInfo other = (SchedulingInfo) object;
      return (startTime == other.startTime);
    }

    return false;
  }
}