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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.JobTrackerNotYetInitializedException;
import org.apache.hadoop.mapred.SafeModeException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.log.Log;

/**
 * <code>WorkflowClient</code> is the primary interface for the user-workflow to
 * interact with the {@link JobTracker}.
 * 
 * 
 */
public class WorkflowClient extends Configured {

  // TODO: These values okay?
  public static final String MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY = "mapreduce.jobclient.retry.policy.enabled";
  public static final Boolean MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = false;
  public static final String MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY = "mapreduce.jobclient.retry.policy.spec";
  public static final String MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10";

  /**
   * A NetworkedWorkflow is an implementation of RunningWorkflow. It holds a
   * WorkflowProfile object to provide some information, and interacts with the
   * remote service to provide certain functionality.
   */
  static class NetworkedWorkflow implements RunningWorkflow {

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

  }

  private UserGroupInformation ugi;
  private WorkflowSubmissionProtocol workflowSubmitClient;

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
      throw new IOException(
          "Workflow execution only implemented for cluster configuration.");
    } else {
      WorkflowSubmissionProtocol rpcWorkflowSubmitClient;
      rpcWorkflowSubmitClient = createRPCProxy(JobTracker.getAddress(conf),
          conf);
      this.workflowSubmitClient = createProxy(rpcWorkflowSubmitClient, conf);
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
                MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY,
                MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
                MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY,
                MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT),
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
        MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY,
        MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
        MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY,
        MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
        JobTrackerNotYetInitializedException.class,
        SafeModeException.class);

    /*
     * Method-specific retry policies for killJob and killTask: there are no
     * retries on any exception including ConnectonException and
     * SafeModeException.
     */
    // TODO: is this right?
    Map<String, RetryPolicy> methodToPolicy= new HashMap<String, RetryPolicy>();
    methodToPolicy.put("killJob", RetryPolicies.TRY_ONCE_THEN_FAIL);
    methodToPolicy.put("killTask", RetryPolicies.TRY_ONCE_THEN_FAIL);
    
    final WorkflowSubmissionProtocol workflowSubmissionProtocol =
        (WorkflowSubmissionProtocol) RetryProxy.create(
            WorkflowSubmissionProtocol.class,
            rpcWorkflowSubmitClient,
            defaultPolicy,
            methodToPolicy);
    //@formatter:on

    RPC.checkVersion(WorkflowSubmissionProtocol.class,
        WorkflowSubmissionProtocol.versionID, workflowSubmissionProtocol);

    return workflowSubmissionProtocol;
  }

  /**
   * TODO
   * 
   * @param workflow
   * @return
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

        // Generate the scheduling plan.
        WorkflowStatus status = null;
        ClusterStatus clusterStatus = workflowSubmitClient
            .getClusterStatus(true);

        // here we need to call JobTracker for cluster status information?

        copyAndConfigureFiles(workflowCopy, submitWorkflowDir);
        // Submit the workflow... ?
        // need to figure out also how to generate job conf / jobs from
        // the workflow conf, and how later to know to assigned their tasks
        // to a specific machine slot type.

        // Submit the Submitter job.

        // Clean up if things go wrong.
        return null;
      }
    });
  }

  /**
   * Configure the {@link WorkflowConf} of the user with the given command-line
   * options (-libjars, -files, -archives). Also, TODO workflow related things.
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

    // Write the WorkflowConf into a file (WHY? TODO).
    // Copy jar files into HDFS.
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
        Log.info("Workflow Failed: " + runningWorkflow.getFailureInfo());
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