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
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobTrackerNotYetInitializedException;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.SafeModeException;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
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
        WorkflowSubmissionProtocol workflowSubmissionClient) throws IOException {
      this.status = status;
      this.profile = profile;
      this.workflowSubmitClient = workflowSubmissionClient;
      this.statusTime = System.currentTimeMillis();
      validateConstruction();
    }

    // Validate correct object construction.
    private void validateConstruction() throws IOException {
      if (status == null) {
        throw new IOException("The WorkflowStatus cannot be null.");
      } else if (profile == null) {
        throw new IOException("The WorkflowProfile cannot be null.");
      } else if (workflowSubmitClient == null) {
        throw new IOException("The WorkflowSubmissionProtocol cannot be null.");
      }
    }

    @Override
    public boolean isComplete() throws IOException {
      updateStatus();
      // TODO: is this correct? (does it have to be non-blocking?)
      return status.isFinished();
    }

    @Override
    public boolean isSuccessful() throws IOException {
      updateStatus();
      return (status.getRunState() == WorkflowStatus.RunState.SUCCEEDED);
    }

    @Override
    public WorkflowID getID() {
      // TODO: function not called yet in the code.
      return profile.getWorkflowId();
    }

    @Override
    public String getWorkflowName() {
      // TODO: function not called yet in the code.
      return profile.getWorkflowName();
    }

    @Override
    public String getFailureInfo() throws IOException {
      // This function is assumed to be called right after realization of job
      // failure. As such, RPC calls are avoided (eg. not calling updateStatus).
      ensureFreshStatus();
      return status.getFailureInfo();
    }

    @Override
    public WorkflowStatus getWorkflowStatus() throws IOException {
      // TODO: function not called yet in the code.
      updateStatus();
      return status;
    }

    // Some methods rely on having a recent {@link WorkflowStatus} object.
    private synchronized void ensureFreshStatus() throws IOException {
      if (System.currentTimeMillis() - statusTime > MAX_WORKFLOWPROFILE_AGE) {
        updateStatus();
      }
    }

    // Some methods require the current {@link WorkflowStatus} object.
    private synchronized void updateStatus() throws IOException {
      status = workflowSubmitClient.getWorkflowStatus(profile.getWorkflowId());
      if (status == null) {
        throw new IOException("The Workflow appears to have been removed.");
      }
      statusTime = System.currentTimeMillis();
    }

  }

  private static final Log LOG = LogFactory.getLog(WorkflowClient.class);

  private static final long MAX_WORKFLOWPROFILE_AGE = 1000 * 2;  // 2 seconds.

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
   *
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

        // Read in Job/Machine -> Time/Price information, & construct the table.
        String timeXml = workflowJar.getParent().toString()
            + Path.SEPARATOR + "workflowTaskTimes.xml";
        Map<TableKey, TableEntry> table = TimePriceTable.parse(timeXml);
        TimePriceTable.update(table, machineTypes);

        LOG.info("Loaded time-price table.");

        // Initialize/compute job information.
        updateJobInfo(workflowCopy, workflowId);
        updateJobIoPaths(workflowCopy);

        // Check output.
        checkOutputSpecification(workflowCopy);

        // Generate the scheduling plan. Needs to provide:
        // - ordering of tasks to be executed
        // - pairing, each task to a machine type
        SchedulingPlan plan = workflowCopy.getSchedulingPlan();
        if (!plan.generatePlan(machineTypes, machines, table, workflowCopy)) {
          throw new IOException("Workflow constraints are infeasible. Exiting.");
        }

        workflowSubmitClient.setWorkflowSchedulingPlan(plan);

        // Write configuration into HDFS so that JobTracker can read it.
        copyAndConfigureFiles(workflowCopy, submitWorkflowDir);

        // Submit the workflow.
        WorkflowStatus status = workflowSubmitClient.submitWorkflow(workflowId,
            submitWorkflowDir.toString());

        LOG.info("In WorkflowClient. Got WorkflowStatus back from JobTracker.");

        WorkflowProfile profile =
            workflowSubmitClient.getWorkflowProfile(workflowId);

        LOG.info("In WorkflowClient. Got WorkflowProfile back from JobTracker.");

        if (status != null && profile != null) {
          LOG.info("Done submitWorkflowInternal, returning.");
          return new NetworkedWorkflow(status, profile, workflowSubmitClient);
        } else {
          throw new IOException("Could not launch workflow.");
        }

        // TODO: Clean up.
      }
    });
  }

  private JobID getJobId(JobConf conf) throws IOException {
    JobID jobId;
    String jobIdString = conf.getJobId();

    if (jobIdString == null || jobIdString == "") {
      jobId = workflowSubmitClient.getNewJobId();
      conf.setJobId(jobId.toString());
    } else {
      jobId = JobID.forName(jobIdString);
    }

    return jobId;
  }

  /**
   * Update initial workflow job information.
   *
   * This function initializes all workflow job's staging & submit directories.
   * The number of map and reduce tasks are also updated in the {@link JobConf}
   * class (so that they can be used by a scheduler/planner).
   */
  private void updateJobInfo(WorkflowConf workflow, WorkflowID workflowId)
      throws IOException, InterruptedException, ClassNotFoundException {

    LOG.info("In updateJobInfo.");
    Map<String, JobConf> jobs = workflow.getJobs();

    for (String job : jobs.keySet()) {
      LOG.info("Updating information for job: " + job);
      JobConf jobConf = jobs.get(job);

      // Staging & submit directories.
      Path stagingArea = WorkflowSubmissionFiles.getStagingDir(
          WorkflowClient.this, workflow);
      JobID jobId = getJobId(jobConf);
      Path submitJobDir = new Path(stagingArea, jobId.toString());
      jobConf.set("mapreduce.job.dir", submitJobDir.toString());

      LOG.info("Path job staging area: " + stagingArea.toString());
      LOG.info("JobID:  " + jobId.toString());
      LOG.info("Path submitJobDir: " + submitJobDir.toString());

      // Other information that hasn't been set yet.
      jobConf.setWorkflowId(workflowId.toString());
      jobConf.setJobId(jobId.toString());

      // Compute # of maps/reduces.
      // Can't compute maps without intermediate data, so just assume proper
      // value is given in the configuration file.
      LOG.info("Set # of reduces: " + jobConf.getNumReduceTasks());
      LOG.info("Set # of maps: " + jobConf.getNumMapTasks());
    }
  }

  /**
   * Workflow job input and output paths are set to their final values.
   *
   * The function takes into account workflow job dependency information to
   * properly update input and output data paths between jobs.
   */
  private void updateJobIoPaths(WorkflowConf workflow) {

    LOG.info("In updateJobIoPaths.");

    Map<String, JobConf> jobs = workflow.getJobs();
    Map<String, Set<String>> dependencies = workflow.getDependencies();
    Set<String> deps = new HashSet<String>();  // Jobs that are dependencies.

    String workflowInput = workflow.get("mapred.input.dir");
    String workflowOutput = workflow.get("mapred.output.dir");

    LOG.info("Read workflowInputDir as: " + workflowInput);
    LOG.info("Read workflowOutputDir as: " + workflowOutput);

    // Set input and output directories for all jobs.
    for (JobConf conf : jobs.values()) {
      String jobName = conf.getJobName();
      String output = Path.SEPARATOR + workflow.getWorkflowName() + "_" + jobName;
      conf.setOutputDir(output);

      LOG.info("Default job output directory: " + output);
    }

    // Iterate through jobs with dependencies to set their in/output paths.
    for (String successor : dependencies.keySet()) {
      JobConf succ = jobs.get(successor);
      String input = "";

      // Add an input directory to each job for each of it's dependencies.
      for (String dependency : dependencies.get(successor)) {
        JobConf dep = jobs.get(dependency);
        input += dep.getOutputDir() + ", ";

        deps.add(dependency);
      }

      // Finalize the path and convert it to have a HDFS prefix.
      input = input.substring(0, input.length() - 2);
      FileInputFormat.setInputPaths(succ, input);

      LOG.info("Set " + succ.getJobName() + " input as " + succ.getInputDir());
    }

    // Deal with exit and entry jobs.
    for (String job : jobs.keySet()) {
      JobConf jobConf = jobs.get(job);

      // Jobs with no dependencies are entry jobs.
      if (dependencies.get(job) == null) {
        jobConf.setInputDir(workflowInput);

        LOG.info("Set input of " + job + " (now: "
            + jobConf.get("mapred.input.dir") + ") to be " + workflowInput);
      }

      // Jobs that aren't dependencies for any other jobs are exit jobs.
      if (!deps.contains(job)) {
        jobConf.setOutputDir(workflowOutput);

        LOG.info("Set output of " + job + "(now: "
            + jobConf.get("mapred.output.dir") + ") to be " + workflowOutput);
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
    Map<String, JobConf> workflowJobs = workflow.getJobs();

    for (String job : workflowJobs.keySet()) {
      JobConf jobConf = workflowJobs.get(job);
      JobContext context = new JobContext(jobConf, getJobId(jobConf));

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
  private void copyAndConfigureFiles(final WorkflowConf workflow,
      Path submitWorkflowDir) throws IOException, InterruptedException {

    LOG.info("In WorkflowClient copyAndConfigureFiles function.");

    short replication = (short) workflow.getInt("mapred.submit.replcation", 1);

    FileSystem fileSystem = submitWorkflowDir.getFileSystem(workflow);
    if (fileSystem.exists(submitWorkflowDir)) {
      throw new IOException("Not submitting workflow. Workflow directory "
          + submitWorkflowDir + " already exists!! This is unexpected."
          + " Please check what files are in that directory.");
    }

    // Create the workflow directory.
    // Don't need to create job directories, they're made when the jobs are run.
    submitWorkflowDir = fileSystem.makeQualified(submitWorkflowDir);
    FsPermission mapredSysPerms = new FsPermission(
        WorkflowSubmissionFiles.WORKFLOW_DIR_PERMISSION);
    FileSystem.mkdirs(fileSystem, submitWorkflowDir, mapredSysPerms);

    LOG.info("Created workflow directory: " + submitWorkflowDir.toString());

    // Copy the workflow configuration, to allow loading from JobTracker.
    Path confDir = WorkflowSubmissionFiles.getConfDir(submitWorkflowDir);
    FileSystem.mkdirs(fileSystem, confDir, mapredSysPerms);
    WorkflowSubmissionFiles.writeConf(fileSystem, confDir, workflow,
        replication);

    LOG.info("Wrote workflow configuration into " + confDir);

    // TODO: move jar files to dfs
    // TODO: should I also be moving over the individual job configurations?

    // Copy over the workflow jar file (TODO: is this necessary?).
    String oldWorkflowJarPath = workflow.getJar();
    Path oldWorkflowJarFile = new Path(oldWorkflowJarPath);
    Path newWorkflowJarFile = WorkflowSubmissionFiles
        .getWorkflowJar(submitWorkflowDir);

    workflow.setJar(newWorkflowJarFile.toString());
    fileSystem.copyFromLocalFile(oldWorkflowJarFile, newWorkflowJarFile);
    fileSystem.setReplication(newWorkflowJarFile, replication);
    fileSystem.setPermission(newWorkflowJarFile, new FsPermission(
        WorkflowSubmissionFiles.WORKFLOW_DIR_PERMISSION));

    LOG.info("Copied over workflow jar file into " + newWorkflowJarFile);
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
  // TODO:
  // TODO: returns unsuccessful even when execution is a success.
  private boolean monitorAndPrintWorkflow(WorkflowConf conf,
      RunningWorkflow workflow) throws IOException, InterruptedException {

    String lastReport = null;
    WorkflowID workflowId = workflow.getID();
    LOG.info("Running workflow: " + workflowId);

    while (!workflow.isComplete()) {
      Thread.sleep(1000); // Sleep for a second.

      // Get the state of all jobs.
      WorkflowStatus status = workflow.getWorkflowStatus();
      Collection<String> prepJobs = status.getPrepJobs();
      Collection<String> submittedJobs = status.getSubmittedJobs();
      Collection<String> runningJobs = status.getRunningJobs();
      Collection<String> finishedJobs = status.getFinishedJobs();

      String prepared = Arrays.toString(prepJobs.toArray());
      String submitted = Arrays.toString(submittedJobs.toArray());
      String running = Arrays.toString(runningJobs.toArray());
      String finished = Arrays.toString(finishedJobs.toArray());

      // Get the progress of each running job.
      // TODO
      String report = "\nPrepared: " + prepared + "\nSubmitted: " + submitted
          + "\nRunning: " + running + "\nFinished: " + finished;

      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }
    }

    return workflow.isSuccessful();
  }
}