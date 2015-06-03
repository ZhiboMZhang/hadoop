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
import java.text.NumberFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ID;

/**
 * WorkflowID represents the immutable and unique identifier for the workflow.
 * 
 * This class is the same as JobID, though is re-implemented as jtIdentifier is
 * private in the JobID class (as opposed to extending JobID as we require a
 * WORKFLOW string rather than a JOB string).
 */
public class WorkflowID extends org.apache.hadoop.mapred.ID implements
    Comparable<ID>, Cloneable {

  protected static final String WORKFLOW = "workflow";
  private final Text jtIdentifier;

  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(4);
  }

  // Workflowid regex for various tools and framework components.
  public static final String WORKFLOWID_REGEX = WORKFLOW + SEPARATOR + "[0-9]+"
      + SEPARATOR + "[0-9]+";

  /**
   * Construct a WorkflowID object.
   * 
   * @param jtIdentifier A jobTracker identifier.
   * @param id A job number.
   */
  public WorkflowID(String jtIdentifier, int id) {
    super(id);
    System.out.println("In WorkflowID(S, int) constructor");
    this.jtIdentifier = new Text(jtIdentifier);
  }

  /**
   * Construct a WorkflowID object.
   * 
   * This constructor is called via reflection (somewhere) in the code.
   */
  public WorkflowID() {
    jtIdentifier = new Text();
  }

  public String getJtIdentifier() {
    return jtIdentifier.toString();
  }

  @Override
  public WorkflowID clone() {
    return new WorkflowID(this.jtIdentifier.toString(), this.id);
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }

    WorkflowID that = (WorkflowID) other;
    return jtIdentifier.equals(that.jtIdentifier) && (id == that.id);
  }

  @Override
  public int hashCode() {
    return jtIdentifier.hashCode() + id;
  }

  @Override
  public int compareTo(ID other) {
    WorkflowID that = (WorkflowID) other;
    int jtComp = jtIdentifier.compareTo(that.jtIdentifier);
    if (jtComp == 0) {
      return this.id - that.id;
    } else {
      return jtComp;
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(WORKFLOW);
    stringBuilder.append(SEPARATOR);
    stringBuilder.append(jtIdentifier);
    stringBuilder.append(SEPARATOR);
    stringBuilder.append(idFormat.format(id));
    return stringBuilder.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    jtIdentifier.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jtIdentifier.write(out);
  }
}