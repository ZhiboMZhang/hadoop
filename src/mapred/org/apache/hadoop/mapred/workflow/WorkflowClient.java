/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.mapred.workflow;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobTrackerNotYetInitializedException;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.SafeModeException;
import org.apache.hadoop.mapred.workflow.WorkflowConf.JobInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * <code>WorkflowClient</code> is the primary interface for the user-workflow to
 * interact with the {@link JobTracker}.
 * 
 * 
 */
public class WorkflowClient extends Configured {

  /**
   * A NetworkedWorkflow is an implementation of RunningWorkflow. It holds a
   * WorkflowProfile object to provide some information, and interacts with the
   * remote service to provide certain functionality.
   */
  static class NetworkedWorkflow implements RunningWorkflow {

    private WorkflowSubmissionProtocol workflowSubmitClient;
    private WorkflowStatus status;
    private WorkflowProfile profile;
    private long statusTime;

    /**
     * We store a {@link WorkflowProfile} and a timestamp for when we last
     * acquired the job profile. If the job is null, then we cannot perform an
     * of the tasks, so we throw an exception.
     */
    public NetworkedWorkflow(WorkflowStatus status, WorkflowProfile profile,
        WorkflowSubmissionProtocol workflowSubmissionClient) {
      this.status = status;
      this.profile = profile;
      this.workflowSubmitClient = workflowSubmissionClient;
    }

    @Override
    public WorkflowID getID() {
      return null;
    }

    @Override
    public String getWorkflowName() {
      return null;
    }

    @Override
    public String getFailureInfo() throws IOException {
      return null;
    }

    @Override
    public WorkflowStatus getWorkflowStatus() throws IOException {
      return null;
    }

  }

  private static final Log LOG = LogFactory.getLog(WorkflowClient.class);

  private WorkflowSubmissionProtocol rpcWorkflowSubmitClient;
  private WorkflowSubmissionProtocol workflowSubmitClient;

  private Path stagingAreaDir = null;
  private UserGroupInformation ugi;

  /**
   * Constructor for the WorkflowClient.
   *
   * @param conf A {@link WorkflowConf} configuration.
   * @throws IOException
   */
  public WorkflowClient(WorkflowConf conf) throws IOException {
    setConf(conf);
    init(conf);
  }

  /**
   * Connect to the default {@link JobTracker}.
   *
   * @param conf The workflow configuration.
   * @throws IOException
   */
  private void init(WorkflowConf conf) throws IOException {

    String tracker = conf.get("mapred.job.tracker", "local");
    this.ugi = UserGroupInformation.getCurrentUser();

    if ("local".equals(tracker)) {
      throw new IOException("Workflow execution only implemented"
          + " for distributed cluster configuration.");
    } else {
      rpcWorkflowSubmitClient = createRPCProxy(JobTracker.getAddress(conf),
          conf);
      workflowSubmitClient = createProxy(rpcWorkflowSubmitClient, conf);
    }
  }

  /**
   * Create an RPC proxy to communicate with the JobTracker.
   */
  private static WorkflowSubmissionProtocol createRPCProxy(
      InetSocketAddress addr, Configuration conf) throws IOException {

    //@formatter:off
    WorkflowSubmissionProtocol rpcWorkflowSubmitClient=
        (WorkflowSubmissionProtocol) RPC.getProxy(
            WorkflowSubmissionProtocol.class,
            WorkflowSubmissionProtocol.versionID,
            addr,
            UserGroupInformation.getCurrentUser(),
            conf,
            NetUtils.getSocketFactory(conf, WorkflowSubmissionProtocol.class),
            0,
            RetryUtils.getMultipleLinearRandomRetry(
                conf,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY,
                JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT),
            false);
    //@formatter:on

    return rpcWorkflowSubmitClient;
  }

  /**
   * Create a wrapper for the RPC proxy.
   */
  private static WorkflowSubmissionProtocol createProxy(
      WorkflowSubmissionProtocol rpcWorkflowSubmitClient, Configuration conf)
      throws IOException {

    /*
     * Default is to retry on JobTrackerNotYetInitializedException; we wait for
     * the JobTracker to get to a RUNNING state and for SafeModeException.
     */
    //@formatter:off
    @SuppressWarnings("unchecked")
    RetryPolicy defaultPolicy = RetryUtils.getDefaultRetryPolicy(
        conf,
        JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY,
        JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
        JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY,
        JobClient.MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
        JobTrackerNotYetInitializedException.class,
        SafeModeException.class);

    final WorkflowSubmissionProtocol workflowSubmissionProtocol =
        (WorkflowSubmissionProtocol) RetryProxy.create(
            WorkflowSubmissionProtocol.class,
            rpcWorkflowSubmitClient,
            defaultPolicy,
            new HashMap<String, RetryPolicy>());
    //@formatter:on

    RPC.checkVersion(WorkflowSubmissionProtocol.class,
        WorkflowSubmissionProtocol.versionID, workflowSubmissionProtocol);

    return workflowSubmissionProtocol;
  }

  /**
   * Submit a workflow to the map-reduce system.
   *
   * This function returns a handle to the {@link RunningWorkflow} which can be
   * used to track the execution of the running workflow.
   *
   * @param workflow The workflow configuration.
   *
   * @return A handle to the {@link RunningWorkflow}.
   * @throws FileNotFoundException
   * @throws IOException
   */
  private RunningWorkflow submitWorkflow(WorkflowConf workflow)
      throws FileNotFoundException, IOException {
    try {
      return submitWorkflowInternal(workflow);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted", ie);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Class not found", cnfe);
    }
  }

  /**
   * Internal method for submitting workflows to the system.
   *
   * @param workflow The workflow configuration to submit.
   * @return A proxy object for running the workflow.
   * @throws FileNotFoundException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  private RunningWorkflow submitWorkflowInternal(final WorkflowConf workflow)
      throws FileNotFoundException, ClassNotFoundException,
      InterruptedException, IOException {

    // Configure the command line options correctly wrt/ the DFS.
    return ugi.doAs(new PrivilegedExceptionAction<RunningWorkflow>() {
      public RunningWorkflow run() throws FileNotFoundException,
          ClassNotFoundException, InterruptedException, IOException {

        WorkflowConf workflowCopy = workflow;

        // Set up the workflow staging area.
        Path stagingArea = WorkflowSubmissionFiles.getStagingDir(
            WorkflowClient.this, workflowCopy);
        WorkflowID workflowId = workflowSubmitClient.getNewWorkflowId();
        Path submitWorkflowDir = new Path(stagingArea, workflowId.toString());
        workflowCopy.set("mapreduce.workflow.dir", submitWorkflowDir.toString());

        LOG.info("Set up workflow staging area.");
        LOG.info("WorkflowID: " + workflowId.toString());
        LOG.info("Path submitWorkflowDir: " + submitWorkflowDir.toString());

        // Get cluster status information from the JobTracker.
        ClusterStatus clusterStatus;
        clusterStatus = workflowSubmitClient.getClusterStatus(true);
        Map<String, ResourceStatus> machines = clusterStatus.getTrackerInfo();

        LOG.info("Got ClusterStatus, tracker information.");

        // Read in the machine type information.
        // TODO: Should this file location/name be set in configuration?
        Path workflowJar = new Path(workflowCopy.getJar());
        String machineXml = workflowJar.getParent().toString()
            + Path.SEPARATOR + "machineTypes.xml";
        Set<MachineType> machineTypes = MachineType.parse(machineXml);

        LOG.info("Loaded machine types.");

        // Initialize/compute job information.
        updateJobInfo(workflowCopy);
        updateJobIoPaths(workflowCopy);

        // Check output.
        checkOutputSpecification(workflowCopy);

        // Generate the scheduling plan.
        // workflowCopy.generatePlan(machineTypes, machines);

        // TODO: write configuration into HDFS so that jobtracker can read it

        // TODO - how to split workflow in multiple jobs to run
        // copyAndConfigureFiles(workflowCopy, submitWorkflowDir);

        // Submit the workflow.
        // TODO: instead of ugi it should be workflowCopy.getCredentials()?
        // TODO: fill in on jobtracker side
        // WorkflowStatus status =
        // workflowSubmitClient.submitWorkflow(workflowId,
        // submitWorkflowDir.toString(), ugi.getShortUserName());

        // WorkflowProfile profile = workflowSubmitClient
        // .getWorkflowProfile(workflowId);

        // if (status != null && profile != null) {
        // return new NetworkedWorkflow(status, profile, workflowSubmitClient);
        // } else {
        // throw new IOException("Could not launch workflow.");
        // }

        // Clean up if things go wrong.
        LOG.info("Done submitWorkflowInternal.");
        return null;

      }
    });
  }

  /**
   * Update initial workflow job information.
   *
   * This function initializes all workflow job's staging & submit directories,
   * as well as setting up initial input and output directories. The number of
   * map and reduce tasks are also updated in the {@link JobInfo} class (so that
   * they can be used by a scheduler/planner).
   */
  private void updateJobInfo(WorkflowConf workflow) throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("In updateJobInfo.");
    Map<String, JobInfo> workflowJobs = workflow.getJobs();

    for (String job : workflowJobs.keySet()) {
      LOG.info("Updating information for job: " + job);
      JobInfo jobInfo = workflowJobs.get(job);
      JobConf jobConf = jobInfo.jobConf;

      // Staging & submit directories, other job setup information.
      Path stagingArea = WorkflowSubmissionFiles.getStagingDir(
          WorkflowClient.this, workflow);
      JobID jobId = workflowSubmitClient.getNewJobId();
      Path submitJobDir = new Path(stagingArea, jobId.toString());
      jobConf.set("mapreduce.job.dir", submitJobDir.toString());

      jobInfo.jobId = jobId;

      LOG.info("Path job staging area: " + stagingArea.toString());
      LOG.info("JobID:  " + jobId.toString());
      LOG.info("Path submitJobDir: " + submitJobDir.toString());

      // Compute # of maps/reduces.
      jobInfo.numReduces = jobConf.getNumReduceTasks();
      jobInfo.numMaps = jobConf.getNumMapTasks();

      // TODO: decide what to do -> can't compute maps wo/ intermediate data
      // JobContext context = new JobContext(jobConf, jobId);
      // jobInfo.numMaps = writeSplits(context, submitJobDir);
      // jobConf.setNumMapTasks(jobInfo.numMaps);

      LOG.info("Set # of reduces: " + jobInfo.numReduces);
      LOG.info("Set # of maps: " + jobInfo.numMaps);

      // Set up input directories for all jobs.
      String jobInputDir = jobConf.get("mapreduce.job.dir") + Path.SEPARATOR
          + "input";
      jobConf.set("mapred.input.dir", jobInputDir);

      LOG.info("Temporary job input directory: " + jobInputDir);

      // Set the main class from the parameters string (TODO:??)
      // (Look through execution of job to see how this is handled.)
    }
  }

  /**
   * Workflow job input and output paths are updated to their final values.
   *
   * The function takes into account workflow job dependency information to
   * properly update input and output data paths between jobs.
   */
  private void updateJobIoPaths(WorkflowConf workflow) {

    LOG.info("In updateJobIoPaths.");

    Map<String, JobInfo> workflowJobs = workflow.getJobs();
    Map<String, Set<String>> dependencyMap = workflow.getDependencies();
    Set<String> dependencies = new HashSet<String>();

    String workflowInputDir = workflow.get("mapred.input.dir");
    String workflowOutputDir = workflow.get("mapred.output.dir");
    LOG.info("Read workflowInputDir as: " + workflowInputDir);
    LOG.info("Read workflowOutputDir as: " + workflowOutputDir);

    // Iterate through jobs with dependencies to set their in/output paths.
    for (String job : dependencyMap.keySet()) {
      JobConf jobConf = workflowJobs.get(job).jobConf;
      Set<String> jobDependencies = dependencyMap.get(job);

      // A job with x as a dependency will have its input as x's output.
      String inputDir = jobConf.get("mapred.input.dir");
      for (String dependency : jobDependencies) {
        JobConf jobConfDependency = workflowJobs.get(dependency).jobConf;
        jobConfDependency.set("mapred.output.dir", inputDir);
        dependencies.add(dependency);

        LOG.info("Set output of " + dependency + "(now: "
            + jobConfDependency.get("mapred.output.dir") + ") to be input of "
            + job + " (" + inputDir + ")");
      }
    }

    // Job with no dependencies are entry jobs.
    for (String job : workflow.getJobs().keySet()) {
      if (dependencyMap.get(job) == null) {
        JobConf jobConf = workflowJobs.get(job).jobConf;
        jobConf.set("mapred.input.dir", workflowInputDir);

        LOG.info("Set input of " + job + " (now: "
            + jobConf.get("mapred.input.dir") + ") to be " + workflowInputDir);
      }
    }

    // Job's that aren't dependencies for any other jobs are exit jobs.
    for (String job : workflow.getJobs().keySet()) {
      if (!dependencies.contains(job)) {
        JobConf jobConf = workflowJobs.get(job).jobConf;
        jobConf.set("mapred.output.dir", workflowOutputDir);

        LOG.info("Set output of " + job + "(now: "
            + jobConf.get("mapred.output.dir") + ") to be " + workflowOutputDir);
      }
    }
  }

  /**
   * Check that input & output directories for all workflow jobs are valid.
   *
   * By default, Hadoop checks the output directories for jobs during execution.
   * However as we're dealing with a set of interdependent jobs it is better to
   * check a directory validity before the job is run.
   */
  private void checkOutputSpecification(WorkflowConf workflow)
      throws ClassNotFoundException, IOException, InterruptedException {

    LOG.info("In checkOutputSpecificaion.");
    Map<String, JobInfo> workflowJobs = workflow.getJobs();

    for (String job : workflowJobs.keySet()) {
      JobInfo jobInfo = workflowJobs.get(job);
      JobConf jobConf = jobInfo.jobConf;
      JobContext context = new JobContext(jobConf, jobInfo.jobId);

      if (jobConf.getNumReduceTasks() == 0 ? jobConf.getUseNewMapper()
          : jobConf.getUseNewReducer()) {
        org.apache.hadoop.mapreduce.OutputFormat<?, ?> output = ReflectionUtils
            .newInstance(context.getOutputFormatClass(), jobConf);
        output.checkOutputSpecs(context);
      } else {
        jobConf.getOutputFormat().checkOutputSpecs(null, jobConf);
      }
    }
  }

  // Just need to compute the number of splits, not actually split the input.
  // Done so that the number of splits can be used by the scheduler.
  private int writeSplits(org.apache.hadoop.mapreduce.JobContext job,
      Path jobSubmitDir) throws IOException, InterruptedException,
      ClassNotFoundException {

    JobConf jobConf = (JobConf) job.getConfiguration();
    int maps;

    if (jobConf.getUseNewMapper()) {
      InputFormat<?, ?> input = ReflectionUtils.newInstance(
          job.getInputFormatClass(), job.getConfiguration());
      maps = input.getSplits(job).size();
    } else {
      org.apache.hadoop.mapred.InputSplit[] splits = jobConf.getInputFormat()
          .getSplits(jobConf, jobConf.getNumMapTasks());
      maps = splits.length;
    }
    return maps;
  }

  public Path getStagingAreaDir() throws IOException {
    if (stagingAreaDir == null) {
      stagingAreaDir = new Path(
          workflowSubmitClient.getWorkflowStagingAreaDir());
    }
    return stagingAreaDir;
  }

  /**
   * Configure the {@link WorkflowConf} of the user.
   *
   * @param workflow The workflow configuration.
   * @param submitWorkflowDir The directory used by the workflow when submitted.
   * @throws IOException
   * @throws InterruptedException
   */
  private void copyAndConfigureFiles(WorkflowConf workflow,
      Path submitWorkflowDir) throws IOException, InterruptedException {

    short replication = (short) workflow.getInt("mapred.submit.replcation", 1);

    FileSystem fileSystem = submitWorkflowDir.getFileSystem(workflow);
    if (fileSystem.exists(submitWorkflowDir)) {
      throw new IOException("Not submitting workflow. Workflow directory "
          + submitWorkflowDir + " already exists!! This is unexpected."
          + " Please check what files are in that directory.");
    }

    // Write the WorkflowConf into a file (WHY? TODO). ???

    // Copy job jar + other files into HDFS.

    // Get the job information from the workflow configuration.
    // Create directories for all workflow jobs, copy over their data.
    // - Don't worry about output directories... (?)

    // Map<String, JobInfo> jobs = workflow.getJobConfs();

    // Modify JobConfs to replace local path in JobConf with HDFS path.
    // TODO: move over additional in Shen Li's WJobConf into JobConf ???

  }

  /**
   * Utility that submits a workflow, then polls for progress until the workflow
   * is finished.
   *
   * @param workflow The workflow configuration.
   * @return A RunningWorkflow to be used for monitoring.
   * @throws IOException if the workflow fails.
   */
  public static RunningWorkflow runWorkflow(WorkflowConf workflow)
      throws IOException {

    WorkflowClient workflowClient = new WorkflowClient(workflow);
    RunningWorkflow runningWorkflow = workflowClient.submitWorkflow(workflow);

    try {
      if (!workflowClient.monitorAndPrintWorkflow(workflow, runningWorkflow)) {
        LOG.info("Workflow Failed: " + runningWorkflow.getFailureInfo());
        throw new IOException("Workflow failed!");
      }

    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    return runningWorkflow;
  }

  /**
   * Monitor and print status in real-time as progress is made and tasks fail.
   *
   * @param conf The workflow's configuration.
   * @param workflow The workflow to track.
   * @return true if the job succeeded.
   * @throws IOException if communication to the {@link JobTracker} fails.
   * @throws InterruptedException
   */
  private boolean monitorAndPrintWorkflow(WorkflowConf conf,
      RunningWorkflow workflow) throws IOException, InterruptedException {
    // TODO
    return false;
  }
}