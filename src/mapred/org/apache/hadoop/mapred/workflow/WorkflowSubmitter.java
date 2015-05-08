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

/**
 * Submit a workflow for deadline/budget constrained scheduling.
 */
public class WorkflowSubmitter {

  // Entry point for execution of workflow scheduling.
  public static void main(String[] args) {
    String usage = "workflow configuration-file.xml";

    if (args.length != 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    // Validate the input configuration.
    String fileName = args[0];
    WorkflowConf workflowConf = new WorkflowConf(fileName);

    // use code from hadoop.util.RunJar to check jar config?

  }
}
