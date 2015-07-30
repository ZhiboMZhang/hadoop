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

public class RandomWordCount {

  public static void main(String[] args) throws Exception {

    WorkflowConf conf = new WorkflowConf(RandomWordCount.class);
    conf.setWorkflowName("RandomWordCount");

    // Set any constraints.
    // Budget takes an amount in dollars.
    // Deadline takes a time in any of seconds [s], minutes (m), or hours (h).
    conf.setConstraint(Constraints.BUDGET, "3.52");
    conf.setConstraint(Constraints.DEADLINE, "600");

    // Specify the jobs that comprise the workflow.
    // Entries for these jobs must appear in the time-price table xml file.
    conf.addJob("RandomWriter", "randomtextwriter.jar");
    conf.addJob("WordCountNew", "wordcountnew.jar");

    conf.getJobs().get("RandomWriter").setNumMapTasks(8);
    conf.getJobs().get("RandomWriter").setNumReduceTasks(0);

    conf.getJobs().get("WordCountNew").setNumMapTasks(8);
    conf.getJobs().get("WordCountNew").setNumReduceTasks(1);

    // Specify main classes (assuming not in jar files).
    conf.setJobMainClass("WordCountNew", "org.apache.hadoop.workflow.examples.jobs.WordCountNew");
    conf.setJobMainClass("RandomWriter", "org.apache.hadoop.workflow.examples.jobs.RandomTextWriter");

    // Specify dependencies.
    conf.addDependency("WordCountNew", "RandomWriter");

    // Specify command-line parameters if required.
    // -> None are.

    // Specify optional/additional input paths.
    // -> Not needed.

    // Set input and output paths for the workflow.
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // Lastly, run the workflow.
    WorkflowClient.runWorkflow(conf);
  }
}