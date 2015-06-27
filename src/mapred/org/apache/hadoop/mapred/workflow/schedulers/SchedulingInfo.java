package org.apache.hadoop.mapred.workflow.schedulers;


abstract class SchedulingInfo {

  private long startTime;

  public SchedulingInfo(long startTime) {
    this.startTime = startTime;
  }

  public long getStartTime() {
    return startTime;
  }

}