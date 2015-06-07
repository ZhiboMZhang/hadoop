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
package org.apache.hadoop.workflow.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.workflow.WorkflowClient;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;
import org.apache.hadoop.mapred.workflow.schedulers.FifoSchedulingPlan;

public class GrepCount {

  public static void main(String[] args) throws Exception {

    WorkflowConf workflowConf = new WorkflowConf(GrepCount.class);
    workflowConf.setWorkflowName("GrepCount");

    // Set any constraints.
    // Budget takes a dollar amount parsed as a double value.
    // Deadline takes a time amount in any of (whole) seconds (s), minutes (m),
    // hours (h). With no type flag the assumed type is seconds.
    workflowConf.setConstraint(Constraints.BUDGET, "42.23");
    workflowConf.setConstraint(Constraints.DEADLINE, "360");

    // Specify the jobs that comprise the workflow.
    // Also each job may or may not require command-line parameters.
    // TODO: Split out parameters into a separate function??
    workflowConf.addJob("Grep", "grep.jar", "org.apache.examples.Grep search");
    workflowConf.addJob("WordCount", "wordcount.jar",
        "org.apache.examples.Wordcount");

    // And we need to specify for each job its predecessors (if any).
    workflowConf.addDependency("WordCount", "Grep");

    // Also set the scheduler/scheduling plan.
    workflowConf.setSchedulerClass(FifoSchedulingPlan.class);

    // We also need to specify the input dataset.
    FileInputFormat.setInputPaths(workflowConf, new Path(args[0]));
    FileOutputFormat.setOutputPath(workflowConf, new Path(args[1]));

    WorkflowClient.runWorkflow(workflowConf);
  }
}