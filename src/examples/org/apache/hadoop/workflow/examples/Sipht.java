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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.workflow.WorkflowClient;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;

public class Sipht {

  private static final Log LOG = LogFactory.getLog(Sipht.class);
  private static float MARGIN_OF_ERROR = 5E-8f;

  // In a normal jobs, splits would be generated wrt/ input data size
  // on the fly, whereas the input in workflow configuration is made to match
  // the number of generated splits.
  // For a ComputeJob - where the user can input a requested number of splits -
  // this means that the requested number of splits mocks the input data size,
  // and as such we still need to record the actual number of splits to enter
  // as configuration information in the workflow configuration.
  private static void addAndConfigComputeJob(WorkflowConf conf, String jobName,
      int numRequestedMaps, int numActualMaps, int numReds, float marginOfError)
      throws IOException {

    String jobArgs = numRequestedMaps + " " + numReds + " " + marginOfError;

    conf.addJob(jobName, "computejob.jar");
    conf.setJobMainClass(jobName, "org.apache.hadoop.workflow.examples.jobs.ComputeJob");
    conf.setJobArguments(jobName, jobArgs);

    conf.getJobs().get(jobName).setNumMapTasks(numActualMaps);
    conf.getJobs().get(jobName).setNumReduceTasks(numReds);
  }

  public static void main(String[] args) throws Exception {

    // Allow budget to be optionally passed into the function.
    String budget = "0.00";
    if (args.length > 2) { budget = args[2]; }

    WorkflowConf conf = new WorkflowConf(Sipht.class);
    conf.setWorkflowName("SIPHT");

    // Set any constraints.
    // Budget takes an amount in dollars.
    // Deadline takes a time in any of seconds (s), minutes (m), or hours (h).
    conf.setConstraint(Constraints.BUDGET, budget);

    // Specify the jobs that comprise the workflow.
    // Entries for these jobs must appear in the time-price table XML file.

    // Right side of diagram
    // Set the # of maps to be the number of dependencies * each # of reduces.
    addAndConfigComputeJob(conf, "patser-concat", 18, 18, 1, MARGIN_OF_ERROR);

    for (int i = 1; i <= 18; i++) {
      String jobName = "patser-" + Integer.toString(i, 10);

      addAndConfigComputeJob(conf, jobName, 3, 3, 1, MARGIN_OF_ERROR);
      conf.setJobInputPaths(jobName, "/sipht-patser-input");
      conf.addDependency("patser-concat", jobName);
    }

    // Left side of diagram, left to right, top to bottom.
    addAndConfigComputeJob(conf, "transterm", 2, 2, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "findterm", 2, 2, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "rna-motif", 2, 2, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "blast", 2, 2, 1, MARGIN_OF_ERROR);

    addAndConfigComputeJob(conf, "srna", 4, 4, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "ffn-parse", 1, 1, 1, MARGIN_OF_ERROR);

    addAndConfigComputeJob(conf, "blast-synteny", 2, 3, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "blast-candidate", 1, 1, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "blast-qrna", 1, 1, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "blast-paralogues", 1, 1, 1, MARGIN_OF_ERROR);

    addAndConfigComputeJob(conf, "srna-annotate", 6, 9, 1, MARGIN_OF_ERROR);
    addAndConfigComputeJob(conf, "last-transfer", 1, 1, 1, MARGIN_OF_ERROR);

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

    // Set workflow input and output paths.
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // Lastly, run the workflow.

    // Record duration for testing.
    Date startTime = new Date();
    LOG.info("Workflow " + conf.getWorkflowName() + " started: " + startTime);

    WorkflowClient.runWorkflow(conf);

    Date endTime = new Date();
    LOG.info("Job " + conf.getWorkflowName() + " ended: " + endTime);

    long duration = endTime.getTime() - startTime.getTime();
    LOG.info("Job " + conf.getWorkflowName() + " took " + (duration / 1000)
        + " seconds (" + duration + " ms).");

  }
}