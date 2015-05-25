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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.util.ClassUtil;

public class WorkflowConf extends Configuration {

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

  public class JobInfo {
    public JobConf jobConf;
    public String parameters;
  }

  // private SchedulingPlan scheduler;
  private HashMap<String, JobInfo> jobs;
  private Map<String, Set<String>> dependencies;

  public WorkflowConf(Class<?> exampleClass) {
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
   * Generate a scheduling plan for the WorklfowConf, given constraints and any
   * additional information.
   * 
   * @return TODO
   */
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
    // Get location of workflow jar.
    Path jar = new Path(getJar());

    try {
      Path addedJarPath = new Path(jar.getParent(), jarName);
      boolean exists = FileSystem.getLocal(this).exists(addedJarPath);
      if (exists) {
        // Store contained jobs in JobInfo class as opposed to attributes.
        JobInfo job = new JobInfo();
        job.parameters = parameters;
        job.jobConf = new JobConf();
        job.jobConf.setJar(jarName);

        jobs.put(name, job);
      } else {
        // TODO: Throw exception, misconfigured job.
      }

    } catch (Exception e) {

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

}