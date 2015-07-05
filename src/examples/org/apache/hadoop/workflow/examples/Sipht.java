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

import java.util.Arrays;

import org.apache.hadoop.mapred.workflow.WorkflowClient;
import org.apache.hadoop.mapred.workflow.WorkflowConf;
import org.apache.hadoop.mapred.workflow.WorkflowConf.Constraints;

public class Sipht {

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
    conf.addJob("patser-concat", "patser-concat.jar");

    for (int i = 1; i <= 20; i++) {
      String formattedIndex = Integer.toString(i, 10);
      conf.addJob("patser-" + formattedIndex, "patser.jar");
      conf.addDependency("patser-concat", "patser-" + formattedIndex);
    }

    // Left side of diagram, left to right, top to bottom.
    conf.addJob("transterm", "transterm.jar");
    conf.addJob("findterm", "findterm.jar");
    conf.addJob("rna-motif", "rna-motif.jar");
    conf.addJob("blast", "blast.jar");

    conf.addJob("snra", "snra.jar");
    conf.addJob("ffn-parse", "ffn-parse.jar");

    conf.addJob("blast-synteny", "blast-synteny.jar");
    conf.addJob("blast-candidate", "blast-candidate.jar");
    conf.addJob("blast-qrna", "blast-qrna.jar");
    conf.addJob("blast-paralogues", "blast-paralogues.jar");

    conf.addJob("snra-annotate", "snra-annotate.jar");
    conf.addJob("last-transfer", "last-transfer.jar");

    // Specify remaining dependencies.
    conf.addDependency("last-transfer", "snra-annotate");

    conf.addDependencies("snra-annotate", Arrays.asList(
        "patser-concat",
        "blast-synteny",
        "blast-candidate",
        "blast-qrna",
        "blast-paralogues"));
    
    conf.addDependency("blast-synteny", "ffn-parse");
    conf.addDependency("blast-synteny", "snra");

    conf.addDependency("blast-candidate", "snra");
    conf.addDependency("blast-qnra", "snra");
    conf.addDependency("blast-paralogues", "snra");
    
    conf.addDependencies("snra", Arrays.asList(
        "transterm",
        "findterm",
        "rna-motif",
        "blast"));

    // Specify main classes (assuming not in jar files).
    // conf.setJobMainClass("patser", "org.company.project.class.Name");

    // Specify command-line parameters if required.
    // conf.setJobArguments("patser", "-d 42");

    // Specify input paths.
    conf.setJobInputPaths("patser", "/sipht-patser-input");

    // Lastly, run the workflow.
    WorkflowClient.runWorkflow(conf);
  }
}