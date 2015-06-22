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
package org.apache.hadoop.mapred.workflow.schedulers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskScheduler;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.schedulers.WorkflowUtil.MachineTypeJobNamePair;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.util.RunJar;

public class FifoWorkflowScheduler extends TaskScheduler implements
    WorkflowScheduler {

  private static final Log LOG = LogFactory.getLog(FifoWorkflowScheduler.class);

  private FifoWorkflowListener fifoWorkflowListener;

  private WorkflowSchedulingProtocol workflowSchedulingProtocol;
  private SchedulingPlan schedulingPlan;

  public FifoWorkflowScheduler() {
    LOG.info("In constructor.");
    fifoWorkflowListener = new FifoWorkflowListener();
  }

  @Override
  public synchronized void start() throws IOException {
    super.start();
    LOG.info("In start() method.");
    taskTrackerManager.addJobInProgressListener(fifoWorkflowListener);
    taskTrackerManager.addWorkflowInProgressListener(fifoWorkflowListener);
  }

  @Override
  public synchronized void terminate() throws IOException {
    LOG.info("In terminate() method.");
    if (fifoWorkflowListener != null) {
      taskTrackerManager.removeJobInProgressListener(fifoWorkflowListener);
      taskTrackerManager.removeWorkflowInProgressListener(fifoWorkflowListener);
    }
    super.terminate();
  }

  public synchronized void setWorkflowSchedulingProtocol(
      WorkflowSchedulingProtocol workflowSchedulingProtocol) {
    this.workflowSchedulingProtocol = workflowSchedulingProtocol;
  }

  @Override
  // Called from JobTracker heartbeat function, which is called by a taskTracker
  public List<Task> assignTasks(TaskTracker taskTracker) throws IOException {

    if (taskTrackerManager.isInSafeMode()) {
      LOG.info("JobTracker is in safe mode, not scheduling any tasks.");
      return null;
    }

    if (schedulingPlan == null) {
      schedulingPlan = workflowSchedulingProtocol.getWorkflowSchedulingPlan();
      if (schedulingPlan == null) { return null; }
    }

    Map<String, String> trackerMapping = schedulingPlan.getTrackerMapping();
    List<MachineTypeJobNamePair> taskMapping = schedulingPlan.getTaskMapping();

    // Find out what the next job/workflow to be executed is.
    Collection<Object> queue = fifoWorkflowListener.getQueue();

    synchronized (queue) {
      for (Object object : queue) {

        if (object instanceof JobInProgress) {
          JobInProgress job = (JobInProgress) object;
          LOG.info("Got job from queue.");

          // Find out which tasktracker wants a task.
          String tracker = taskTracker.getTrackerName();
          String machineType = trackerMapping.get(tracker);
          LOG.info("Got tracker name as: " + tracker);
          LOG.info("Got machine type as: " + machineType);

          // Match it with an available task.
          // check tracker type against machine type
          // if available job exists for machine type, run next job/task on it.
          // MachineTypeJobNamePair pair = taskMapping.get(currentTask);
          // LOG.info("MachineJobPair[" + currentTask + "] machineType: " +
          // pair.machineType);
          // LOG.info("MachineJobPair[" + currentTask + "] jobName: " +
          // pair.jobName);

          // String jobName = job.getJobConf().getJobName();
          // LOG.info("Job to be scheduled jobName: " + jobName);


        } else if (object instanceof WorkflowInProgress) {
          WorkflowInProgress workflow = (WorkflowInProgress) object;
          LOG.info("Got workflow from queue.");
          // final JobConf jobConf = workflow.obtainNewJob();
          JobConf jobConf = null;
          // TODO: get next job using schedulingplan

          if (jobConf == null) {
            LOG.info("All workflow jobs have been started.");
            continue;
          }

          LOG.info("Got next job from workflow as: " + jobConf.getJar());
          // LOG.info("Workflow jar: " + workflow.getConf().getJar());

          // Check the jar for a manifest & add required attributes.
          updateJarManifest(jobConf);

          // Submit the job.
          LOG.info("Submitting workflow job: " + jobConf.getJar());
          workflow.getStatus().addSubmittedJob(jobConf.getJobId());
          submitWorkflowJob(jobConf);
        }
      }
    }

    return null;
  }

  private void submitWorkflowJob(final JobConf jobConf) {
    new Thread(new Runnable() {
      public void run() {
        try {
          String[] args = { jobConf.getJar() };
          RunJar.main(args);
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    }).start();
  }

  // Check that the jar file has a manifest, and if not then add one.
  // Write configuration properties to the jar file's manifest.
  private void updateJarManifest(JobConf jobConf) throws IOException {
    LOG.info("In updateJarManifest.");

    String fileName = jobConf.getJar();
    JarInputStream jarInput = new JarInputStream(new FileInputStream(fileName));
    Manifest manifest = jarInput.getManifest();
    LOG.info("Jar file is located at: " + fileName);

    if (manifest == null) {
      LOG.info("Manifest is null.");
      manifest = new Manifest();
      manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    }

    Attributes attributes = manifest.getMainAttributes();
    attributes.putValue("Main-Class", jobConf.getMainClass());
    attributes.putValue("Arguments", jobConf.getArguments());
    attributes.putValue("Input-Directory", jobConf.getInputDir());
    attributes.putValue("Output-Directory", jobConf.getOutputDir());
    attributes.putValue("Workflow-Id", jobConf.getWorkflowId());
    attributes.putValue("Job-Id", jobConf.getJobId());
    attributes.putValue("Job-Name", jobConf.getJobName());
    LOG.info("Added manifest attributes.");

    // Write the new jar file.
    FileOutputStream out = new FileOutputStream(fileName + ".tmp", false);
    JarOutputStream jarOutput = new JarOutputStream(out, manifest);

    JarEntry entry;
    int bytesRead;
    byte[] buffer = new byte[1024];

    LOG.info("Writing new jar " + jarOutput.toString());
    while ((entry = jarInput.getNextJarEntry()) != null) {
      jarOutput.putNextEntry(entry);

      while ((bytesRead = jarInput.read(buffer)) != -1) {
        jarOutput.write(buffer, 0, bytesRead);
      }
    }
    jarOutput.close();
    jarInput.close();
    LOG.info("Created new jar.");

    // Remove the old jar & rename the new one.
    File jarIn = new File(fileName);
    File jarOut = new File(fileName + ".tmp");
    jarIn.delete();
    jarOut.renameTo(jarIn);
    LOG.info("Deleted old jar & renamed new one.");
  }

  @Override
  public Collection<JobInProgress> getJobs(String queueName) {
    LOG.info("In getJobs method.");
    return null;
  }

  // checkJobSubmission ?? (superclass method does nothing)
  // refresh ?? (superclass method does nothing)

}