package org.apache.hadoop.mapred.workflow.schedulers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.MachineType;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.JobInfo;

/**
 * Class representing a workflow directed acyclic graph.
 */
public class WorkflowDAG {
  
  private static final Log LOG = LogFactory.getLog(WorkflowDAG.class);

  private Set<WorkflowNode> nodes;
  private Set<WorkflowNode> entryNodes = null;
  private Set<WorkflowNode> exitNodes = null;
  private Map<WorkflowNode, Set<WorkflowNode>> successors;
  private Map<WorkflowNode, Set<WorkflowNode>> predecessors;

  public WorkflowDAG() {
    this.nodes = new HashSet<WorkflowNode>();
    this.successors = new HashMap<WorkflowNode, Set<WorkflowNode>>();
    this.predecessors = new HashMap<WorkflowNode, Set<WorkflowNode>>();
  }

  private void addNode(WorkflowNode node) {
    // Initialize the successors and predecessors of a node before it is added.
    this.successors.put(node, new HashSet<WorkflowNode>());
    this.predecessors.put(node, new HashSet<WorkflowNode>());

    // Add the node to the workflow.
    nodes.add(node);
  }

  private void removeNode(WorkflowNode node) {
    // Remove pointers to the node, and pointers from the node.
    for (WorkflowNode linkedNode : nodes) {
      successors.get(linkedNode).remove(node);
      predecessors.get(linkedNode).remove(node);
    }
    successors.remove(node);
    predecessors.remove(node);

    // Remove the node itself.
    nodes.remove(node);
  }

  private void addSuccessor(WorkflowNode node, WorkflowNode successor) {
    successors.get(node).add(successor);
  }

  private void addPredecessor(WorkflowNode node, WorkflowNode predecessor) {
    predecessors.get(node).add(predecessor);
  }

  /**
   * Return the predecessors of the specified {@link WorkflowNode}.
   */
  public Set<WorkflowNode> getPredecessors(WorkflowNode node) {
    return predecessors.get(node);
  }

  /**
   * Return the successors of the specified {@link WorkflowNode}.
   */
  public Set<WorkflowNode> getSuccessors(WorkflowNode node) {
    return successors.get(node);
  }

  /**
   * Return the set of nodes for the workflow.
   */
  public Set<WorkflowNode> getNodes() {
    return nodes;
  }

  /**
   * Get the set of entry nodes for the workflow.
   */
  public Set<WorkflowNode> getEntryNodes() {
    if (entryNodes == null) {
      entryNodes = new HashSet<WorkflowNode>();
      for (WorkflowNode node : nodes) {
        if (predecessors.get(node).size() == 0) {
          entryNodes.add(node);
        }
      }
    }
    return entryNodes;
  }

  /**
   * Get the set of exit nodes for the workflow.
   */
  public Set<WorkflowNode> getExitNodes() {
    if (exitNodes == null) {
      exitNodes = new HashSet<WorkflowNode>();
      for (WorkflowNode node : nodes) {
        if (successors.get(node).size() == 0) {
          exitNodes.add(node);
        }
      }
    }
    return exitNodes;
  }

  /**
   * Using the current node-machineType pairings, return a critical path.
   *
   * @param table A time-price table to use for computation of times/prices.
   *
   * @return A list of {@link WorkflowNode}s on the critical path.
   */
  public List<WorkflowNode> getCriticalPath(Map<TableKey, TableEntry> table) {

    LOG.info("Computing Critical path.");
    Map<WorkflowNode, Float> distances = getWorkflowNodeWeights(table);
    List<WorkflowNode> criticalPath = new ArrayList<WorkflowNode>();

    // Get the exit nodes before adding the fake node so it is not included.
    Set<WorkflowNode> exitNodes = getExitNodes();

    // Add a fake exit node connecting to all real exit nodes.
    // This allows computation of only one path, rather than multiple paths.
    WorkflowNode fakeExitNode = new WorkflowNode("fakeNode", 0, true);
    this.addNode(fakeExitNode);
    LOG.info("Added fake exit node.");

    for (WorkflowNode exit : exitNodes) {
      this.addPredecessor(fakeExitNode, exit);
      LOG.info("Added " + exit.getName() + " as predecessor of fake exit node.");
    }

    WorkflowNode criticalNode = fakeExitNode;
    do {
      LOG.info("Creating critical path, added " + criticalNode.getName());
      criticalNode = getNextCriticalNode(distances, criticalNode);
      criticalPath.add(0, criticalNode);
    } while (criticalNode != null);

    // Null check is after adding, so remove the null element.
    criticalPath.remove(0);
    this.removeNode(fakeExitNode);
    LOG.info("Removed fake exit node.");

    return criticalPath;
  }

  private WorkflowNode getNextCriticalNode(
      Map<WorkflowNode, Float> distances, WorkflowNode current) {

    float maxDistance = 0;
    WorkflowNode criticalNode = null;

    for (WorkflowNode predecessor : getPredecessors(current)) {
      LOG.info("Checking distance of predecessor " + predecessor.getName()
          + " of node " + current.getName());
      float distance = distances.get(predecessor);
      if (distance > maxDistance) {
        maxDistance = distance;
        criticalNode = predecessor;
      }
    }
    return criticalNode;
  }

  /**
   * Compute the weights of WorkflowNodes. Each distance is the value of the
   * longest path from a source node to the node, as measured by maximum
   * execution time.
   *
   * @param table A time-price table to use for time/price computations.
   *
   * @return A map of {@link WorkflowNode} to their weightings.
   */
  private Map<WorkflowNode, Float> getWorkflowNodeWeights(
      Map<TableKey, TableEntry> table) {

    LOG.info("Computing workflow node weights.");
    Map<WorkflowNode, Float> distances = new HashMap<WorkflowNode, Float>();
    List<WorkflowNode> ordering = getTopologicalOrdering();

    // Initialize (time) distances.
    for (WorkflowNode node : getNodes()) {
      distances.put(node, Float.MIN_VALUE);
    }
    for (WorkflowNode entry : getEntryNodes()) {
      float maxTime = getNodeMaxTime(table, entry);
      distances.put(entry, maxTime);
      LOG.info("Updated entry node '" + entry.getName() + "' weight to "
          + maxTime);
    }
    LOG.info("Initialized node weights.");

    // Relax the nodes to find their proper weight.
    for (WorkflowNode node : ordering) {
      for (WorkflowNode next : getSuccessors(node)) {
        // Add the weight of executing the next node.
        float otherPath = distances.get(node) + getNodeMaxTime(table, next);

        if (distances.get(next) < otherPath) {
          distances.put(next, otherPath);
          LOG.info("Updated " + next.getName() + " weight to " + otherPath);
        }
      }
    }

    return distances;
  }

  // Get the execution time of a node/stage; the slowest contained task.
  private float getNodeMaxTime(Map<TableKey, TableEntry> table,
      WorkflowNode node) {
    float maxWeight = 0;
    for (int i = 0; i < node.getNumTasks(); i++) {
      String type = node.getMachineType(i);
      TableKey key = new TableKey(node.getJob(), type, node.isMapStage());
      float weight = table.get(key).execTime;

      if (weight > maxWeight) {
        maxWeight = weight;
      }
    }
    return maxWeight;
  }

  /**
   * Compute and return the total execution time (makespan) of a WorkflowDAG.
   */
  public float getTime(Map<TableKey, TableEntry> table) {
    // Get the critical path.
    List<WorkflowNode> criticalPath = getCriticalPath(table);

    // Compute the execution time along the path.
    float time = 0f;
    for (WorkflowNode node : criticalPath) {
      time += getNodeMaxTime(table, node);
    }

    return time;
  }

  /**
   * Compute and return the total cost of a WorkflowDAG.
   */
  public float getCost(Map<TableKey, TableEntry> table) {
    // Add up the cost of all the nodes/tasks in the dag.
    float cost = 0f;
    for (WorkflowNode node : getNodes()) {
      for (int task = 0; task < node.getNumTasks(); task++) {
        String type = node.getMachineType(task);
        TableKey key = new TableKey(node.getJob(), type, node.isMapStage());
        cost += table.get(key).cost;
      }
    }
    return cost;
  }

  /**
   * Compute and return a topological ordering on the input Workflow Dag.
   * 
   * @return A list of {@link WorkflowNode}.
   */
  // TODO: this is wrong? -> may have to include ALL tasks, not just stages?
  public List<WorkflowNode> getTopologicalOrdering() {

    Set<WorkflowNode> nodes = getEntryNodes();
    Set<WorkflowNode> marked = new HashSet<WorkflowNode>();
    List<WorkflowNode> ordering = new ArrayList<WorkflowNode>();

    for (WorkflowNode node : nodes) {
      constructTopologicalOrdering(node, marked, ordering);
    }

    return ordering;
  }

  private void constructTopologicalOrdering(
      WorkflowNode node, Set<WorkflowNode> marked, List<WorkflowNode> ordering) {

    marked.add(node);
    for (WorkflowNode next : getSuccessors(node)) {
      if (!marked.contains(next)) {
        constructTopologicalOrdering(next, marked, ordering);
      }
    }
    ordering.add(0, node);
  }

  /**
   * Construct a basic workflow DAG, with jobs/tasks initial state being
   * assigned to the least expensive machine type.
   *
   * @param machineTypes A set of {@link MachineType}.
   * @param machines A map of hadoop-named cluster machines/nodes, represented
   *          by their {@link ResourceStatus}.
   * @param workflow A {@link WorkflowConf}.
   *
   * @return A directed acyclic graph representing the workflow.
   */
  public static WorkflowDAG construct(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, WorkflowConf workflow) {

    LOG.info("Constructing WorkflowDAG.");
    WorkflowDAG dag = new WorkflowDAG();

    // A temporary mapping to help with DAG creation.
    Map<JobInfo, WorkflowNodePair> infoToMRStages =
        new HashMap<JobInfo, WorkflowNodePair>();

    // Create a WorkflowNode for each JobInfo (map & reduce).
    Map<String, JobInfo> workflowJobs = workflow.getJobs();
    for (String jobName : workflowJobs.keySet()) {

      JobInfo workflowJob = workflowJobs.get(jobName);
      int maps = workflowJob.numMaps;
      int reduces = workflowJob.numReduces;

      WorkflowNode mapStage = new WorkflowNode(jobName, maps, true);
      WorkflowNode redStage = new WorkflowNode(jobName, reduces, false);

      dag.addNode(mapStage);
      dag.addNode(redStage);
      infoToMRStages.put(workflowJob, new WorkflowNodePair(mapStage, redStage));
      LOG.info("Added nodes for job " + jobName);

      // Also set up a link between the map & reduce stages of a single job.
      dag.addSuccessor(mapStage, redStage);
      dag.addPredecessor(redStage, mapStage);
    }

    // Copy over dependencies, add successors & find entry/exit jobs.
    Map<String, Set<String>> workflowDependencies = workflow.getDependencies();
    for (String successor : workflowDependencies.keySet()) {
      Set<String> dependencies = workflowDependencies.get(successor);

      for (String dependency : dependencies) {

        WorkflowNode node = infoToMRStages.get(workflowJobs.get(successor)).map;
        WorkflowNode pre = infoToMRStages.get(workflowJobs.get(dependency)).red;
        dag.addPredecessor(node, pre);
        dag.addSuccessor(pre, node);
        LOG.info("Added link from " + pre.getName() + " to " + node.getName());
      }
    }

    return dag;
  }

  private static class WorkflowNodePair {

    WorkflowNode map;
    WorkflowNode red;

    public WorkflowNodePair(WorkflowNode map, WorkflowNode red) {
      this.map = map;
      this.red = red;
    }
  }

}