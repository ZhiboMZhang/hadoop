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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.EagerTaskInitializationListener;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskScheduler;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowID;
import org.apache.hadoop.mapred.workflow.WorkflowInProgress;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowListener;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowScheduler;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowSchedulingPlan;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.util.RunJar;


public class WorkflowTaskScheduler extends TaskScheduler implements
    WorkflowScheduler {

  private static final Log LOG = LogFactory.getLog(WorkflowTaskScheduler.class);

  private WorkflowListener workflowListener;
  private EagerTaskInitializationListener eagerTaskInitializationListener;

  private Map<WorkflowID, WorkflowSchedulingPlan> schedulingPlans;

  public WorkflowTaskScheduler() {
    workflowListener = new WorkflowListener();
    schedulingPlans = new HashMap<WorkflowID, WorkflowSchedulingPlan>();
  }

  @Override
  public synchronized void start() throws IOException {
    super.start();
    taskTrackerManager.addJobInProgressListener(workflowListener);
    taskTrackerManager.addWorkflowInProgressListener(workflowListener);

    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(eagerTaskInitializationListener);
  }

  @Override
  public synchronized void terminate() throws IOException {
    if (workflowListener != null) {
      taskTrackerManager.removeJobInProgressListener(workflowListener);
      taskTrackerManager.removeWorkflowInProgressListener(workflowListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    eagerTaskInitializationListener = new EagerTaskInitializationListener(conf);
  }

  @Override
  public synchronized void addWorkflowSchedulingPlan(WorkflowID workflowId,
      WorkflowSchedulingPlan workflowSchedulingPlan) {
    schedulingPlans.put(workflowId, workflowSchedulingPlan);
  }

  @Override
  public synchronized void removeWorkflowSchedulingPlan(WorkflowID workflowId) {
    schedulingPlans.remove(workflowId);
  }

  @Override
  // Called from JobTracker heartbeat function, which is called by a taskTracker
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {

    if (taskTrackerManager.isInSafeMode()) {
      LOG.info("JobTracker is in safe mode, not scheduling any tasks.");
      return null;
    }

    // Find out what the next job/workflow to be executed is.
    WorkflowSchedulingPlan schedulingPlan;
    Collection<Object> queue = workflowListener.getQueue();
    List<Task> assignedTasks = new ArrayList<Task>();

    synchronized (queue) {
      for (Object object : queue) {

        if (object instanceof JobInProgress) {
          // At this point we can execute tasks from any job we get, as the jobs
          // must have been added by the workflowInPorgress object in the queue.

          JobInProgress job = (JobInProgress) object;
          if (job.getStatus().getRunState() != JobStatus.RUNNING) { continue; }
          LOG.info("Got job " + job.getJobConf().getJobName() + " from queue.");

          // Check & get the scheduling plan for the workflow.
          WorkflowID workflowId = job.getStatus().getWorkflowId();
          schedulingPlan = schedulingPlans.get(workflowId);

          if (schedulingPlan == null) {
            LOG.info("No scheduling plan set for workflow " + workflowId + ".");
            continue;
          }

          // A mapping between available machines (names) and machine types.
          Map<String, String> trackerMap = schedulingPlan.getTrackerMapping();

          // Find out which tasktracker wants a task, and it's machine type.
          String tracker = taskTracker.getTrackerName();
          String machineType = trackerMap.get(tracker);
          LOG.info("Got tracker: " + tracker + ", machineType: " + machineType);

          // Get cluster status information to see if there are free slots.
          ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
          TaskTrackerStatus tts = taskTracker.getStatus();
          final int clusterSize = clusterStatus.getTaskTrackers();
          final int uniqueHosts = taskTrackerManager.getNumberOfUniqueHosts();

          String jobName = job.getJobConf().getJobName();

          LOG.info("Checking if a map task can be run.");
          // Check if any slots are available on the tracker.
          final int mapCapacity = tts.getMaxMapSlots();
          final int runningMaps = tts.countMapTasks();
          final int availableMapSlots = mapCapacity - runningMaps;
          LOG.info("Available map slots: " + availableMapSlots + " = "
              + mapCapacity + " (capacity) - " + runningMaps + " (running)");

          // Check if a task can be executed, and if so run it.
          if (availableMapSlots > 0
              && schedulingPlan.matchMap(machineType, jobName)) {
            LOG.info("Map slots are available, and map can be run.");

            Task task = job.obtainNewMapTask(tts, clusterSize, uniqueHosts);
            LOG.info("Obtained map task is " + (task == null ? "null." : "not null."));

            if (task != null) {
              schedulingPlan.runMap(machineType, jobName);
              assignedTasks.add(task);
              LOG.info("Assigning map task " + task.toString() + ".");
            }
          }

          LOG.info("Checking if a reduce task can be run.");
          // Check if any slots are available on the tracker.
          final int reduceCapacity = tts.getMaxReduceSlots();
          final int runningReduces = tts.countReduceTasks();
          final int availableReduceSlots = reduceCapacity - runningReduces;
          LOG.info("Available reduce slots: " + availableReduceSlots + " = "
              + reduceCapacity + " (capacity) - " + runningReduces + " (running)");

          // Check if a task can be executed, and if so run it.
          if (availableReduceSlots > 0
              && schedulingPlan.matchReduce(machineType, jobName)) {
            LOG.info("Reduce slots are available, and reduce can be run.");

            Task task = job.obtainNewReduceTask(tts, clusterSize, uniqueHosts);
            LOG.info("Obtained reduce task is " + (task == null ? "null." : "not null."));

            if (task != null) {
              schedulingPlan.runReduce(machineType, jobName);
              assignedTasks.add(task);
              LOG.info("Assigning reduce task " + task.toString() + ".");
            }
          }

        } else if (object instanceof WorkflowInProgress) {

          WorkflowInProgress workflow = (WorkflowInProgress) object;
          LOG.info("Got workflow from queue.");

          // Check & get the scheduling plan for the workflow.
          WorkflowID workflowId = workflow.getStatus().getWorkflowId();
          schedulingPlan = schedulingPlans.get(workflowId);

          if (schedulingPlan == null) {
            LOG.info("No scheduling plan set for workflow " + workflowId + ".");
            continue;
          }

          Collection<String> finishedJobs = workflow.getStatus().getFinishedJobs();
          LOG.info("Passed in finished jobs: " + finishedJobs);
          Collection<String> jobNames = schedulingPlan.getExecutableJobs(finishedJobs);

          LOG.info("Got back executable jobs: " + jobNames);

          if (jobNames == null || jobNames.size() == 0) {
            LOG.info("All executable workflow jobs have been started.");
            continue;
          }

          for (String jobName : jobNames) {
            // Skip the job if it has already been started.
            if (!workflow.getStatus().getPrepJobs().contains(jobName)) {
              LOG.info("Skipping " + jobName + ", it has already been started.");
              continue;
            }

            JobConf jobConf = workflow.getConf().getJobs().get(jobName);

            // Check the jar for a manifest & add required attributes.
            updateJarManifest(workflow.getConf(), jobConf);

            // Submit the job.
            LOG.info("Submitting workflow job: " + jobConf.getJar());
            workflow.getStatus().addSubmittedJob(jobConf.getJobName());
            submitWorkflowJob(jobConf);
          }
        }
      }
    }

    return assignedTasks;
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
  private void updateJarManifest(WorkflowConf workflowConf, JobConf jobConf)
      throws IOException {
    LOG.info("In updateJarManifest.");

    FileSystem fileSystem = FileSystem.get(workflowConf);
    Path filePath = new Path(jobConf.getJar());
    Path newJarFile = new Path(filePath.toString() + ".tmp");

    // Read/create the manifest.
    JarInputStream jarInput = new JarInputStream(fileSystem.open(filePath));
    Manifest manifest = jarInput.getManifest();
    LOG.info("Jar file is located at: " + filePath.toString());

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
    FSDataOutputStream out = fileSystem.create(newJarFile);
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
    fileSystem.delete(filePath, true);
    fileSystem.rename(newJarFile, filePath);
    LOG.info("Deleted old jar & renamed new one.");
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String ignored) {

    // Both JobInProgress and WorkflowInProgress objects exist in the default
    // queue. Filter the queue to a Collection of JobInProgress objects.
    Collection<Object> queue = workflowListener.getQueue();
    Collection<JobInProgress> jobQueue = new ArrayList<JobInProgress>();

    for (Object object : queue) {
      if (object instanceof JobInProgress) {
        jobQueue.add((JobInProgress) object);
      }
    }

    return jobQueue;
  }

}