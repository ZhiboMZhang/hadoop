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

public class WordCountWorkflow {
  public static void main(String[] args) throws Exception {

    WorkflowConf workflowConf = new WorkflowConf(WordCountWorkflow.class);
    workflowConf.setWorkflowName("WordCountWorkflow");

    // Set any constraints.
    // Budget takes an amount in dollars.
    // Deadline takes a time in any of seconds [s], minutes (m), or hours (h).
    workflowConf.setConstraint(Constraints.BUDGET, "100.49");
    workflowConf.setConstraint(Constraints.DEADLINE, "600s");

    // Specify jobs in the workflow, along with the number of tasks they have.
    workflowConf.addJob("WordCountNew", "wordcountnew.jar");
    workflowConf.setJobMainClass("WordCountNew", "org.apache.hadoop.workflow.examples.jobs.WordCountNew");

    workflowConf.getJobs().get("WordCountNew").setNumMapTasks(3);
    workflowConf.getJobs().get("WordCountNew").setNumReduceTasks(1);

    // Also specify the input dataset.
    FileInputFormat.setInputPaths(workflowConf, new Path(args[0]));
    FileOutputFormat.setOutputPath(workflowConf, new Path(args[1]));

    WorkflowClient.runWorkflow(workflowConf);
  }
}