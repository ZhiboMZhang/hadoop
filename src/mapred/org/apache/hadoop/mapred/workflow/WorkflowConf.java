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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.util.ClassUtil;

public class WorkflowConf extends Configuration implements Writable {

  public static enum Constraints {
    BUDGET, DEADLINE;

    @Override
    public String toString() {
      switch (this) {
        case BUDGET:
          return "budget";
        case DEADLINE:
          return "deadline";
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  public class JobInfo implements Writable {
    public JobConf jobConf;
    public JobID jobId;
    public String parameters;

    public int numMaps;
    public int numReduces;

    @Override
    public void readFields(DataInput in) throws IOException {
      jobConf.readFields(in);
      jobId.readFields(in);
      parameters = Text.readString(in);
      numMaps = in.readInt();
      numReduces = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      jobConf.write(out);
      jobId.write(out);
      Text.writeString(out, parameters);
      out.writeInt(numMaps);
      out.writeInt(numReduces);
    }
  }

  public static final Log LOG = LogFactory.getLog(WorkflowConf.class);

  // private SchedulingPlan scheduler;
  private Map<String, JobInfo> jobs;
  private Map<String, Set<String>> dependencies;

  public WorkflowConf(Class<?> exampleClass) {
    this.jobs = new HashMap<String, JobInfo>();
    this.dependencies = new HashMap<String, Set<String>>();
    setJarByClass(exampleClass);
  }
  
  /**
   * Return the {@link JobInfo jobs} which comprise the workflow.
   */
  public Map<String, JobInfo> getJobs() {
    return jobs;
  }

  /**
   * Return workflow job dependencies, as indicated by job names.
   */
  public Map<String, Set<String>> getDependencies() {
    return dependencies;
  }

  // @formatter:off
  /**
   * Generate a scheduling plan for the WorklfowConf, given constraints and any
   * additional information.
   * 
   * Scheduling requires:
   * - machine type information (cost/stats of different rented nodes)
   * - cluster machine information (stats of nodes in the cluster)
   * - constraint information (in workflow conf)
   * - workflow job information [map splits, reduces] (in workflow conf)
   * - workflow information [dependencies] (in workflow conf)
   *
   * @return TODO
   */
  // @formatter:on
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines) {
    // return scheduler.generatePlan(machineTypes, machines, this);
    return true;
  }

  /**
   * Add a map-reduce job to the list of jobs to be executed in the workflow.
   * Each job is identified by a name, and includes a jar file and parameters.
   * 
   * Job jar files are assumed to be located in the same directory as the
   * Workflow jar file.
   * 
   * @param name A unique identifier for the job to be executed.
   * @param jarName The path to the jar file belonging to the job.
   */
  public void addJob(String name, String jarName, String parameters) {

    LOG.info("Adding job to workflow.");

    // Get location of workflow jar.
    Path jar = new Path(getJar());

    try {
      Path addedJarPath = new Path(jar.getParent(), jarName);
      boolean exists = FileSystem.getLocal(this).exists(addedJarPath);
      LOG.info("Path " + addedJarPath.toString()
          + (exists ? " exists" : " does not exist"));
      if (exists) {
        // Store contained jobs in JobInfo class as opposed to attributes.
        JobInfo job = new JobInfo();
        job.parameters = parameters;
        job.jobConf = new JobConf();
        job.jobConf.setJar(jarName);

        jobs.put(name, job);
      } else {
        // TODO: Throw exception, misconfigured job.
        LOG.info("Path to added job doesn't exist.");
      }

    } catch (Exception e) {
      // TODO: Throw exception,
      LOG.info("Error adding job to workflow.");
    }
  }

  /**
   * Add a dependency to a job.
   *
   * @param successor A unique job name.
   * @param predecessor A unique job name.
   */
  public void addDependency(String successor, String predecessor) {
    Set<String> dependencies = this.dependencies.get(successor);
    if (dependencies == null) {
      dependencies = new HashSet<String>();
      this.dependencies.put(successor, dependencies);
    }
    dependencies.add(predecessor);
  }

  /**
   * Get the user jar for the map-reduce workflow.
   * 
   * @return the user jar for the map-reduce workflow.
   */
  public String getJar() {
    return get("mapred.jar");
  }

  /**
   * Set the user jar for the map-reduce workflow.
   * 
   * @param jar the user jar for the map-reduce workflow.
   */
  public void setJar(String jar) {
    set("mapred.jar", jar);
  }

  /**
   * Set the job's jar file by finding an example class location.
   * 
   * @param cls the example class.
   */
  public void setJarByClass(Class<?> cls) {
    String jar = ClassUtil.findContainingJar(cls);
    if (jar != null) {
      setJar(jar);
    }
  }

  /**
   * Get the value of the value of the given workflow constraint.
   * 
   * @param constraintType The type of workflow constraint.
   * @return the constraint value, defaulting to "".
   */
  public String getConstraint(Constraints constraintType) {
    String property = "mapred.workflow." + constraintType.toString();
    return get(property, "");
  }

  /**
   * Set a constraint for the workflow.
   * 
   * @param constraintType The type of constraint.
   * @param value The constraint value.
   */
  public void setConstraint(Constraints constraintType, String value) {
    String property = "mapred.workflow." + constraintType.toString();
    set(property, value);
  }

  /**
   * Get the user-specified workflow name. This is only used to identify the
   * workflow to the user.
   * 
   * @return the workflow's name, defaulting to "".
   */
  public String getWorkflowName() {
    return get("mapred.workflow.name", "");
  }

  /**
   * Set the user-specified workflow name.
   * 
   * @param name the workflow's new name.
   */
  public void setWorkflowName(String name) {
    set("mapred.workflow.name", name);
  }

  /**
   * Set the current working directory for the default file system.
   * 
   * @param dir the new current working directory.
   */
  public void setWorkingDirectory(Path dir) {
    dir = new Path(getWorkingDirectory(), dir);
    set("mapred.working.dir", dir.toString());
  }

  /**
   * Get the current working directory for the default file system.
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() {
    String name = get("mapred.working.dir");
    if (name != null) {
      return new Path(name);
    } else {
      try {
        Path dir = FileSystem.get(this).getWorkingDirectory();
        set("mapred.working.dir", dir.toString());
        return dir;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    // Read in Jobs & Dependencies.
    int numJobs = in.readInt();
    for (int i = 0; i < numJobs; i++) {
      String job = Text.readString(in);
      JobInfo jobInfo = new JobInfo();
      jobInfo.readFields(in);
      jobs.put(job, jobInfo);
    }

    int numDependencies = in.readInt();
    for (int i = 0; i < numDependencies; i++) {
      String job = Text.readString(in);
      Set<String> jobDependencies = new HashSet<String>();

      // Read the value, a set of values.
      int numJobDependencies = in.readInt();
      for (int j = 0; j < numJobDependencies; j++) {
        jobDependencies.add(Text.readString(in));
      }
      dependencies.put(job, jobDependencies);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    // Write out Jobs & Dependencies.
    out.writeInt(jobs.size());
    for (String job : jobs.keySet()) {
      Text.writeString(out, job);
      jobs.get(job).write(out);
    }

    out.writeInt(dependencies.size());
    for (String job : dependencies.keySet()) {
      Text.writeString(out, job);

      // Write the value, which is itself a set of values..
      out.writeInt(dependencies.get(job).size());
      for (String dependency : dependencies.get(job)) {
        Text.writeString(out, dependency);
      }
    }
  }

}