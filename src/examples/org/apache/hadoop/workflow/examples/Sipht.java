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

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.workflow.WorkflowClient;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;

public class Sipht {

  private static void addAndConfigSleepJob(WorkflowConf conf, String jobName,
      int numMaps, int numReds, long mapTimeSeconds, long redTimeSeconds) throws IOException {

    String jobArgs = numMaps + " " + numReds + " " + (mapTimeSeconds * 1000) + " " + (redTimeSeconds * 1000);

    conf.addJob(jobName, "sleepjob.jar");
    conf.setJobMainClass(jobName, "org.apache.hadoop.workflow.examples.jobs.SleepJob");
    conf.setJobArguments(jobName, jobArgs);

    conf.getJobs().get(jobName).setNumMapTasks(numMaps);
    conf.getJobs().get(jobName).setNumReduceTasks(numReds);
  }

  public static void main(String[] args) throws Exception {

    WorkflowConf conf = new WorkflowConf(Sipht.class);
    conf.setWorkflowName("SIPHT");

    // Set any constraints.
    // Budget takes an amount in dollars.
    // Deadline takes a time in any of seconds [s], minutes (m), or hours (h).
    conf.setConstraint(Constraints.BUDGET, "14.52");
    conf.setConstraint(Constraints.DEADLINE, "30");

    // Specify the jobs that comprise the workflow.
    // Entries for these jobs must appear in the time-price table xml file.
    // TODO: explain names (see 'Characterization of Scientific Workflows')

    // Right side of diagram
    // Set the # of maps to be the number of dependencies * each # of reduces.
    addAndConfigSleepJob(conf, "patser-concat", 3, 1, 30, 30);

    for (int i = 1; i <= 3; i++) {
      String jobName = "patser-" + Integer.toString(i, 10);

      addAndConfigSleepJob(conf, jobName, 3, 1, 30, 30);
      // Assume 3 files in this directory = 3 maps.
      conf.setJobInputPaths(jobName, "/sipht-patser-input");
      conf.addDependency("patser-concat", jobName);
    }

    // Left side of diagram, left to right, top to bottom.
    // Main workflow input has 2 files for input = 2 maps.
    addAndConfigSleepJob(conf, "transterm", 2, 1, 30, 30);
    addAndConfigSleepJob(conf, "findterm", 2, 1, 30, 30);
    addAndConfigSleepJob(conf, "rna-motif", 2, 1, 30, 30);
    addAndConfigSleepJob(conf, "blast", 2, 1, 30, 30);

    addAndConfigSleepJob(conf, "srna", 4, 1, 30, 30);
    addAndConfigSleepJob(conf, "ffn-parse", 1, 1, 30, 30);

    addAndConfigSleepJob(conf, "blast-synteny", 2, 1, 30, 30);
    addAndConfigSleepJob(conf, "blast-candidate", 1, 1, 30, 30);
    addAndConfigSleepJob(conf, "blast-qrna", 1, 1, 30, 30);
    addAndConfigSleepJob(conf, "blast-paralogues", 1, 1, 30, 30);

    addAndConfigSleepJob(conf, "srna-annotate", 6, 1, 30, 30);
    addAndConfigSleepJob(conf, "last-transfer", 1, 1, 30, 30);

    // Specify remaining dependencies.
    conf.addDependency("last-transfer", "srna-annotate");

    conf.addDependencies("srna-annotate", Arrays.asList(
        "srna",
        "patser-concat",
        "blast-synteny",
        "blast-candidate",
        "blast-qrna",
        "blast-paralogues"));
    
    conf.addDependencies("blast-synteny", Arrays.asList("srna", "ffn-parse"));

    conf.addDependency("ffn-parse", "srna");
    conf.addDependency("blast-candidate", "srna");
    conf.addDependency("blast-qrna", "srna");
    conf.addDependency("blast-paralogues", "srna");

    conf.addDependencies("srna", Arrays.asList(
        "transterm",
        "findterm",
        "rna-motif",
        "blast"));

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // Lastly, run the workflow.
    WorkflowClient.runWorkflow(conf);
  }
}