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
import java.util.Collection;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableEntry;
import org.apache.hadoop.mapred.workflow.TimePriceTable.TableKey;
import org.apache.hadoop.mapred.workflow.schedulers.GreedySchedulingPlan;
import org.apache.hadoop.mapred.workflow.scheduling.WorkflowSchedulingPlan;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ReflectionUtils;

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

  public static final Log LOG = LogFactory.getLog(WorkflowConf.class);
  public static final String SCHEDULING_PLAN_PROPERTY_NAME =
      "mapred.workflow.schedulingPlan";

  private WorkflowSchedulingPlan schedulingPlan;
  private Map<String, JobConf> jobs;
  private Map<String, Set<String>> dependencies;

  /**
   * Constructor only to be called when using {@link #readFields(in)}
   * immediately afterwards.
   */
  public WorkflowConf() {}

  public WorkflowConf(Class<?> exampleClass) {

    this.jobs = new HashMap<String, JobConf>();
    this.dependencies = new HashMap<String, Set<String>>();

    setJarByClass(exampleClass);

    // Load the specified scheduling plan
    Class<? extends WorkflowSchedulingPlan> clazz =
        this.getClass(SCHEDULING_PLAN_PROPERTY_NAME, GreedySchedulingPlan.class,
            WorkflowSchedulingPlan.class);
    schedulingPlan = (WorkflowSchedulingPlan) ReflectionUtils.newInstance(clazz, this);
    LOG.info("Created new schedulingPlan: " + schedulingPlan.toString());;
  }

  /**
   * Return the {@link JobConf jobs} which comprise the workflow.
   *
   * The returned collection maps unique job names to their job configuration.
   */
  public Map<String, JobConf> getJobs() {
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
   * - time-price table (cost & price of a task wrt/ machine type)
   * - machine type information (cost/stats of different rented nodes)
   * - cluster machine information (stats of nodes in the cluster)
   * - constraint information (in workflow conf)
   * - workflow job information [map splits, reduces] (in workflow conf)
   * - workflow information [dependencies] (in workflow conf)
   *
   * @return Returns true if the workflow is able to be scheduled, false
   *         otherwise.
   */
  // @formatter:on
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, Map<TableKey, TableEntry> table)
      throws IOException {
    return schedulingPlan.generatePlan(machineTypes, machines, table, this);
  }

  public WorkflowSchedulingPlan getSchedulingPlan() {
    return schedulingPlan;
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
  public void addJob(String name, String jarName)
      throws IOException {

    LOG.info("Adding job " + name + " to workflow.");

    // Get location of workflow jar.
    Path jar = new Path(getJar());

    try {
      Path addedJarPath = new Path(jar.getParent(), jarName);
      boolean exists = FileSystem.getLocal(this).exists(addedJarPath);
      if (exists) {
        JobConf job = new JobConf();
        // TODO: Using full paths currently.
        LOG.info("Assuming full paths, job jar is: " + addedJarPath.toString());
        job.setJar(addedJarPath.toString());
        job.setJobName(name);
        jobs.put(name, job);

      } else {
        LOG.info("Error adding job: path '" + addedJarPath + "' doesn't exist.");
        throw new IOException("Error adding job: path '" + addedJarPath
            + "' doesn't exist.");
      }

    } catch (Exception e) {
      LOG.info("Error adding job to workflow.");
      throw new IOException("Error adding job to workflow. " + e);
    }
  }

  /**
   * Set the workflow job's command-line arguments.
   *
   * @param name The name of the workflow job.
   * @param arguments The arguments.
   * @throws IOException
   */
  public void setJobArguments(String name, String arguments) throws IOException {
    JobConf job = jobs.get(name);
    if (null == job) {
      throw new IOException("Cannot add arguments to job " + name
          + ". Job does not exist");
    }
    job.setArguments(arguments);
  }

  /**
   * Set the workflow job's main class.
   *
   * @param name The name of the workflow job.
   * @param mainClass The job's main class.
   * @throws IOException
   */
  public void setJobMainClass(String name, String mainClass) throws IOException {
    JobConf job = jobs.get(name);
    if (null == job) {
      throw new IOException("Cannot add main class to job " + name
          + ". Job does not exist.");
    }
    job.setMainClass(mainClass);
  }

  /**
   * Set custom input paths for an entry job.
   *
   * @param name The name of the workflow job.
   * @param commaSeparatedPaths A comma-separated list of absolute paths,
   *          assumed to existing in HDFS.
   */
  public void setJobInputPaths(String name, String commaSeparatedPaths)
      throws IOException {
    JobConf job = jobs.get(name);
    if (null == job) {
      throw new IOException("Cannot add main class to job " + name
          + ". Job does not exist.");
    }
    FileInputFormat.setInputPaths(job, commaSeparatedPaths);
  }

  /**
   * Add a dependency to a job.
   *
   * @param job A unique job name.
   * @param dependency A unique job name.
   */
  public void addDependency(String job, String dependency) {
    Set<String> dependencies = this.dependencies.get(job);
    if (dependencies == null) {
      dependencies = new HashSet<String>();
      this.dependencies.put(job, dependencies);
    }
    dependencies.add(dependency);
  }

  /**
   * Add a list of dependencies to a job.
   *
   * @param job A unique job name.
   * @param dependencies A collection of unique job names.
   */
  public void addDependencies(String job, Collection<String> dependencies) {
    Set<String> deps = this.dependencies.get(job);
    if (deps == null) {
      deps = new HashSet<String>();
      this.dependencies.put(job, deps);
    }
    deps.addAll(dependencies);
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
   * Parse a deadline constraint value and return a value measured in seconds.
   */
  public static long parseDeadlineConstraint(String text) {

    if (text == null || text.equals("")) {
      return -1L;
    }

    int multiplier = 1;

    if (text.endsWith("h")) {
      multiplier = 60 * 60;
      text = text.substring(0, text.length() - "h".length());

    } else if (text.endsWith("m")) {
      multiplier = 60;
      text = text.substring(0, text.length() - "m".length());

    } else if (text.endsWith("s")) {
      text = text.substring(0, text.length() - "s".length());
    }

    return Long.parseLong(text, 10) * multiplier;
  }

  /**
   * Parse a budget constraint value and return a value measured in dollars.
   */
  public static float parseBudgetConstraint(String constraint) {
    return Float.parseFloat(constraint);
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

    jobs = new HashMap<String, JobConf>();
    dependencies = new HashMap<String, Set<String>>();

    // Read in Jobs & Dependencies.
    int numJobs = in.readInt();
    for (int i = 0; i < numJobs; i++) {
      String job = Text.readString(in);
      JobConf jobConf = new JobConf();
      jobConf.readFields(in);
      jobs.put(job, jobConf);
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

    // Read in other properties.
    Class<? extends WorkflowSchedulingPlan> clazz =
        this.getClass(SCHEDULING_PLAN_PROPERTY_NAME, GreedySchedulingPlan.class,
            WorkflowSchedulingPlan.class);
    schedulingPlan = (WorkflowSchedulingPlan) ReflectionUtils.newInstance(clazz, this);
    schedulingPlan.readFields(in);
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

    // Write out other properties.
    schedulingPlan.write(out);
  }

}