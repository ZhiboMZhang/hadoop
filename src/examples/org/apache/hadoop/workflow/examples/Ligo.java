package org.apache.hadoop.workflow.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.workflow.WorkflowClient;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;

public class Ligo {

  private static final Log LOG = LogFactory.getLog(Ligo.class);
  private static float MARGIN_OF_ERROR = 5E-8f;

  // In a normal jobs, splits would be generated wrt/ input data size
  // on the fly, whereas the input in workflow configuration is made to match
  // the number of generated splits.
  // For a ComputeJob - where the user can input a requested number of splits -
  // this means that the requested number of splits mocks the input data size,
  // and as such we still need to record the actual number of splits to enter
  // as configuration information in the workflow configuration.
  private static void addAndConfigComputeJob(WorkflowConf conf, String jobName,
      int numRequestedMaps, int numActualMaps, int numReds, float marginOfError)
      throws IOException {

    String jobArgs = numRequestedMaps + " " + numReds + " " + marginOfError;

    conf.addJob(jobName, "computejob.jar");
    conf.setJobMainClass(jobName, "org.apache.hadoop.workflow.examples.jobs.ComputeJob");
    conf.setJobArguments(jobName, jobArgs);

    conf.getJobs().get(jobName).setNumMapTasks(numActualMaps);
    conf.getJobs().get(jobName).setNumReduceTasks(numReds);
  }

  public static void main(String[] args) throws Exception {

    // Allow budget to be optionally passed into the function.
    String budget = "4.52";
    if (args.length > 2) { budget = args[2]; }

    WorkflowConf conf = new WorkflowConf(Ligo.class);
    conf.setWorkflowName("LIGO");

    // Set any constraints.
    // Budget takes an amount in dollars.
    // Deadline takes a time in any of seconds (s), minutes (m), or hours (h).
    conf.setConstraint(Constraints.BUDGET, budget);

    // Specify the jobs that comprise the workflow.
    // Entries for these jobs must appear in the time-price table XML file.

    // We'll have 5+4 input jobs feeding directly to another 5+4.
    // (two separate DAGS as one workflow!)
    for (int i = 1; i <= 9; i++) {
      addAndConfigComputeJob(conf, "tmpItBank-" + i, 4, 5, 1, MARGIN_OF_ERROR);
      addAndConfigComputeJob(conf, "inspiral-a-" + i, 1, 1, 1, MARGIN_OF_ERROR);
    }

    // Which then aggregate into a single job for each sub-dag.
    addAndConfigComputeJob(conf, "thinca-a-1", 5, 5, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "thinca-a-2", 4, 4, 1, MARGIN_OF_ERROR);

    // And then distribute back into another 5+4 paths.
    for (int i = 1; i <= 9; i++) {
      addAndConfigComputeJob(conf, "trigBank-" + i, 1, 1, 1, MARGIN_OF_ERROR);
      addAndConfigComputeJob(conf, "inspiral-b-" + i, 1, 1, 1, MARGIN_OF_ERROR);
    }

    // Finally joining back into a single job for each sub-dag.
    addAndConfigComputeJob(conf, "thinca-b-1", 5, 5, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "thinca-b-2", 4, 4, 1, MARGIN_OF_ERROR);

    // Add dependencies.
    conf.addDependencies("thinca-b-1", Arrays.asList(
        "inspiral-b-1", "inspiral-b-2", "inspiral-b-3", "inspiral-b-4", "inspiral-b-5"));
    conf.addDependencies("thinca-b-2", Arrays.asList(
        "inspiral-b-6", "inspiral-b-7", "inspiral-b-8", "inspiral-b-9"));

    for (int i = 1; i <= 9; i++) {
      conf.addDependency("inspiral-b-" + i, "trigBank-" + i);
      conf.addDependency("inspiral-a-" + i, "tmpItBank-" + i);
    }

    conf.addDependencies("thinca-a-1", Arrays.asList(
        "inspiral-a-1", "inspiral-a-2", "inspiral-a-3", "inspiral-a-4", "inspiral-a-5"));
    conf.addDependencies("thinca-a-2", Arrays.asList("inspiral-a-6",
        "inspiral-a-7", "inspiral-a-8", "inspiral-a-9"));

    for (int i = 1; i <= 5; i++) { conf.addDependency("trigBank-" + i, "thinca-a-1"); }
    for (int i = 6; i <= 9; i++) { conf.addDependency("trigBank-" + i, "thinca-a-2"); }

    // Set workflow input and output paths.
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // Lastly, run the workflow.

    // Record duration for testing.
    Date startTime = new Date();
    LOG.info("TESTING: Workflow " + conf.getWorkflowName() + " started: " + startTime);

    WorkflowClient.runWorkflow(conf);

    Date endTime = new Date();
    LOG.info("TESTING: Workflow " + conf.getWorkflowName() + " ended: " + endTime);

    long duration = endTime.getTime() - startTime.getTime();
    LOG.info("TESTING: Workflow " + conf.getWorkflowName() + " took "
        + (duration / 1000) + " seconds (" + duration + " ms).");
  }
}