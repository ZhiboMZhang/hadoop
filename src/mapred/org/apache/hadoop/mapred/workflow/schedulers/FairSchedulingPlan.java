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
package org.apache.hadoop.mapred.workflow.schedulers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.MachineType;
import org.apache.hadoop.mapred.workflow.SchedulingPlan;
import org.apache.hadoop.mapred.workflow.WorkflowConf;

public class FairSchedulingPlan extends SchedulingPlan {

  public static final Log LOG = LogFactory.getLog(FairSchedulingPlan.class);

  @Override
  // TODO
  public boolean generatePlan(Set<MachineType> machineTypes,
      Map<String, ResourceStatus> machines, WorkflowConf workflow) {

    LOG.info("In FairScheduler.class generatePlan() function");
    return true;
  }

  @Override
  // TODO
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  // TODO
  public void write(DataOutput out) throws IOException {
  }

}