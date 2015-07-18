package org.apache.hadoop.mapred.workflow.schedulers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowDAG;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowNode;

/**
 * The {@link ProgressBasedSchedulingPlan} uses a WorkflowPrioritizer to assign
 * priorities to workflow jobs, which are then used for scheduling.
 */
abstract class WorkflowPrioritizer implements Writable,
    Comparator<WorkflowNode> {

  protected WorkflowDAG workflowDag;

  /** Only to be used when calling readFields() on a subclass. */
  WorkflowPrioritizer() {}

  WorkflowPrioritizer(WorkflowDAG workflowDag) {
    this.workflowDag = workflowDag;
  }

  /**
   * Given a set of newly finished jobs, return a prioritized list of newly
   * executable jobs.
   * 
   * If the set is null or empty, return the initial list of jobs (if it has
   * not been returned yet).
   */
  public abstract List<WorkflowNode> getExecutableJobs(Set<WorkflowNode> finishedJobs);

  /**
   * Given a collection of ALL finished jobs, return a prioritized list of
   * newly executable jobs.
   */
  public abstract List<String> getExecutableJobs(Collection<String> finishedJobs);

  /**
   * Order jobs according to the current priority metric.
   */
  @Override
  public abstract int compare(WorkflowNode node, WorkflowNode other);

  @Override
  public void write(DataOutput out) throws IOException {
    workflowDag.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.workflowDag = new WorkflowDAG();
    workflowDag.readFields(in);
  }
}