package org.apache.hadoop.mapred.workflow.schedulers;

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

  /**
   * Given a set of newly finished jobs, return a prioritized list of newly
   * executable jobs.
   *
   * If the set is null or empty, return the initial list of jobs (if it has not
   * been returned yet).
   *
   * This function is called during generation of the scheduling plan.
   */
  public abstract List<WorkflowNode> getExecutableJobs(Set<WorkflowNode> finishedJobs);

  /**
   * Order jobs according to the current priority metric.
   */
  @Override
  public abstract int compare(WorkflowNode node, WorkflowNode other);

  /**
   * Set the prioritizer's workflow dag. Only to be used after readfields().
   */
  public abstract void setWorkflowDag(WorkflowDAG workflowDag);
}