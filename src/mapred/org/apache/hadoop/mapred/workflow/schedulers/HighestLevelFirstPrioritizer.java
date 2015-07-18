package org.apache.hadoop.mapred.workflow.schedulers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.workflow.scheduling.WorkflowDAG;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowNode;

/**
 * Jobs are arranged into levels, where those with no dependencies are assigned
 * to level 0. Any job on level i must have all dependencies on levels smaller
 * than i, with at least one on level i-1.
 * 
 * The level corresponds with priority, where a higher level indicates a higher
 * priority.
 */
class HighestLevelFirstPrioritizer extends WorkflowPrioritizer {

  Map<WorkflowNode, Integer> priorities = new HashMap<WorkflowNode, Integer>();

  /** Only to be used when calling readFields(). */
  HighestLevelFirstPrioritizer() {}

  HighestLevelFirstPrioritizer(WorkflowDAG workflowDag) {
    super(workflowDag);
    generatePriorities();
  }

  private void generatePriorities() {

    // All entry nodes are assigned a level of 0.
    for (WorkflowNode node : workflowDag.getEntryNodes()) {
      priorities.put(node, 0);
    }

    // A list of nodes whose priorities haven't been finalized.
    List<WorkflowNode> nodes = new ArrayList<WorkflowNode>(workflowDag.getEntryNodes());

    while (!nodes.isEmpty()) {
      WorkflowNode currentNode = nodes.remove(0);

      for (WorkflowNode successor : workflowDag.getSuccessors(currentNode)) {

        nodes.add(successor);
        Integer currentPriority = priorities.get(successor);
        int newPriority = priorities.get(currentNode) + 1;

        if (currentPriority == null) {
          priorities.put(successor, newPriority);
        } else {
          priorities.put(successor, Math.max(currentPriority, newPriority));
        }
      }
    }
  }

  @Override
  public List<WorkflowNode> getExecutableJobs(Set<WorkflowNode> arg1) {
    // TODO: time to add: minimum finish time of all dependencies
    return null;
  }

  @Override
  public List<String> getExecutableJobs(Collection<String> arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compare(WorkflowNode node, WorkflowNode other) {
    // Higher priority should go first, eg. return a lower value.
    return -(priorities.get(node) - priorities.get(other));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    // TODO Auto-generated method stub
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    // TODO Auto-generated method stub
  }

}