package org.apache.hadoop.mapred.workflow.schedulers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
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

  private static final Log LOG = LogFactory
      .getLog(HighestLevelFirstPrioritizer.class);

  WorkflowDAG workflowDag;
  Map<WorkflowNode, Integer> priorities = new HashMap<WorkflowNode, Integer>();
  Map<String, WorkflowNode> jobNameMap = new HashMap<String, WorkflowNode>();
  Set<WorkflowNode> planningFinishedJobs = new HashSet<WorkflowNode>();

  /** Only to be used when calling readFields(). */
  HighestLevelFirstPrioritizer() {}

  /** Only to be used when calling readFields(). */
  public void setWorkflowDag(WorkflowDAG workflowDag) {
    this.workflowDag = workflowDag;
  }

  HighestLevelFirstPrioritizer(WorkflowDAG workflowDag) {
    setWorkflowDag(workflowDag);
    generatePriorities();
  }

  private void generatePriorities() {
    LOG.info("Generating job priorities.");

    // All entry nodes are assigned a level of 0.
    for (WorkflowNode node : workflowDag.getEntryNodes()) {
      priorities.put(node, 0);
      LOG.info(node.getJobName() + " priority set to 0.");
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
          LOG.info(successor.getJobName() + " priority set to " + newPriority + ".");
        } else {
          int priority = Math.max(currentPriority, newPriority);
          priorities.put(successor, priority);
          LOG.info(successor.getJobName() + " priority set to " + priority + ".");
        }
      }
    }
  }

  @Override
  // Runs during plan generation.
  public List<WorkflowNode> getExecutableJobs(Set<WorkflowNode> finishedJobs) {

    List<WorkflowNode> executableJobs = new ArrayList<WorkflowNode>();

    // If no jobs are finished.
    if (finishedJobs == null || finishedJobs.isEmpty()) {
      LOG.info("No finished jobs, returning entry nodes.");
      for (WorkflowNode node : workflowDag.getEntryNodes()) {
        jobNameMap.put(node.getJobName(), node);
        executableJobs.add(node);
      }
      Collections.sort(executableJobs, this);
      return executableJobs;
    }

    // If all jobs are finished, return an empty list.
    if (finishedJobs.size() == workflowDag.getNodes().size()) {
      LOG.info("All jobs finished, returning empty set.");
      return executableJobs;
    }

    this.planningFinishedJobs.addAll(finishedJobs);

    // Otherwise find the newly executable jobs.
    // A job is executable if all of its dependencies have finished execution.
    for (WorkflowNode job : finishedJobs) {
      for (WorkflowNode successor : workflowDag.getSuccessors(job)) {
        boolean eligible = true;

        for (WorkflowNode predecessor : workflowDag.getPredecessors(successor)) {
          if (!this.planningFinishedJobs.contains(predecessor)) { eligible = false; }
        }
        if (eligible) {
          jobNameMap.put(successor.getJobName(), successor);
          executableJobs.add(successor);
        }
      }
    }

    LOG.info("Got: " + finishedJobs + ", returning: " + executableJobs);
    Collections.sort(executableJobs, this);
    return executableJobs;
  }

  @Override
  public int compare(WorkflowNode node, WorkflowNode other) {
    return priorities.get(node) - priorities.get(other);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // Write out all structures using node names.

    out.writeInt(priorities.size());
    for (WorkflowNode node : priorities.keySet()) {
      Text.writeString(out, node.getJobName());
      out.writeInt(priorities.get(node));
    }

    out.writeInt(planningFinishedJobs.size());
    for (WorkflowNode node : planningFinishedJobs) {
      Text.writeString(out, node.getJobName());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // When reading in structures build the node name map, and use it with the
    // set workflow dag to build the other structures. We have to do things
    // this we to keep object identities (.equals()) working. I forget why but
    // there was a reason we couldn't implement (.equals()) for WorkflowNode
    // class, it broke some other parts of the code.

    // Build job name map.
    jobNameMap = new HashMap<String, WorkflowNode>();
    for (WorkflowNode node : workflowDag.getNodes()) {
      jobNameMap.put(node.getJobName(), node);
    }

    // Read in other structures.
    priorities = new HashMap<WorkflowNode, Integer>();
    int numPriorities = in.readInt();
    for (int i = 0; i < numPriorities; i++) {
      WorkflowNode node = jobNameMap.get(Text.readString(in));
      int priority = in.readInt();
      priorities.put(node, priority);
    }

    planningFinishedJobs = new HashSet<WorkflowNode>();
    int numPlanningFinishedJobs = in.readInt();
    for (int i = 0; i < numPlanningFinishedJobs; i++) {
      planningFinishedJobs.add(jobNameMap.get(Text.readString(in)));
    }
  }

}