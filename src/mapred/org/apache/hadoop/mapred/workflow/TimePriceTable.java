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
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class TimePriceTable {

  public static class TableKey implements Writable {
    
    public String jobName;
    public String machineTypeName;
    public boolean isMapTask;

    /** Only to be used by readFields. */
    public TableKey() {}
    
    public TableKey(String jobName, String machineTypeName, boolean isMapTask) {
      this.jobName = jobName;
      this.machineTypeName = machineTypeName;
      this.isMapTask = isMapTask;
    }

    @Override
    public int hashCode() {
      return jobName.hashCode() + machineTypeName.hashCode()
          + (isMapTask ? 1 : 0);
    }

    @Override
    public boolean equals(Object obj) {

      if (obj == null) { return false; }
      if (this == obj) { return true; }
      if (getClass() != obj.getClass()) { return false; }
      final TableKey other = (TableKey) obj;
      if (jobName.equals(other.jobName)
          && machineTypeName.equals(other.machineTypeName)
          && isMapTask == other.isMapTask) {
        return true;
      }

      return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, jobName);
      Text.writeString(out, machineTypeName);
      out.writeBoolean(isMapTask);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      jobName = Text.readString(in);
      machineTypeName = Text.readString(in);
      isMapTask = in.readBoolean();
    }
  }

  public static class TableEntry extends TableKey {

    public long execTime; // in seconds.
    public float cost;  // in $.

    /** Only to be used by readFields. */
    public TableEntry() {}

    public TableEntry(String jobName, String machineTypeName, long execTime,
        boolean isMapTask) {
      super(jobName, machineTypeName, isMapTask);
      this.execTime = execTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeLong(execTime);
      out.writeFloat(cost);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      execTime = in.readLong();
      cost = in.readFloat();
    }
  }
  
  private static final Log LOG = LogFactory.getLog(TimePriceTable.class);

  /**
   * Parse the given file for partial (or full) time price table information.
   * 
   * @param xmlFile The file to be parsed.
   * 
   * @return
   * @throws IOException
   */
  public static Map<TableKey, TableEntry> parse(String xmlFile)
      throws IOException {
    
    LOG.info("Parsing time-price table " + xmlFile);

    File workflowTaskTimeXmlFile = new File(xmlFile);
    Map<TableKey, TableEntry> table = new HashMap<TableKey, TableEntry>();

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setIgnoringComments(true);

      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(workflowTaskTimeXmlFile);

      NodeList workflowTaskNodes = doc.getElementsByTagName("job");
      LOG.info("Got " + workflowTaskNodes.getLength() + " 'job' nodes.");
      for (int i = 0; i < workflowTaskNodes.getLength(); i++) {
        Node workflowTaskNode = workflowTaskNodes.item(i);
        NodeList workflowTaskMachines = workflowTaskNode.getChildNodes();

        String job = ((Element) workflowTaskNode).getAttribute("name");
        LOG.info("Read jobName as: '" + job + "'.");
        if (job.equals("")) {
          continue;
        }

        for (int j = 0; j < workflowTaskMachines.getLength(); j++) {

          Node node = workflowTaskMachines.item(j);
          if (!(node instanceof Element)) {
            continue;
          }
          Element element = (Element) node;

          if ("machineType".equals(element.getTagName())) {

            long map;
            long red;

            try {
              map = getExecTime(element.getAttribute("mapExecTime"));
              red = getExecTime(element.getAttribute("redExecTime"));
            } catch (NumberFormatException nfe) {
              LOG.info("Partial table entry skipped; map or reduce"
                  + " execution time is missing. " + nfe);
              continue;
            }

            String machine = element.getAttribute("name");
            if (machine.equals("")) {
              continue;
            }

            LOG.info("Adding table entry for: " + job + ", " + machine
                + ", Map: " + map + ", Red: " + red);
            TableKey mapKey = new TableKey(job, machine, true);
            TableKey redKey = new TableKey(job, machine, false);
            TableEntry mapEntry = new TableEntry(job, machine, map, true);
            TableEntry redEntry = new TableEntry(job, machine, red, false);
            table.put(mapKey, mapEntry);
            table.put(redKey, redEntry);
          }
        }
      }
    } catch (Exception e) {
      LOG.fatal("Error parsing workflow task time file '" + xmlFile + "'. " + e);
      throw new IOException("Error parsing workflow task time file '" + xmlFile
          + "'. " + e);
    }

    return table;
  }

  // Convert given execution time in xml to seconds.
  private static long getExecTime(String text) {

    if (text == null || text.equals("")) {
      return -1L;
    }

    int multiplier = 1;

    if (text.endsWith("h")) {
      multiplier = 60 * 60;
      text = text.substring(0, text.length() - "h".length());

    } else if (text.endsWith("m")) {
      multiplier = 60;
      text = text.substring(0, text.length() - "m".length());

    } else if (text.endsWith("s")) {
      text = text.substring(0, text.length() - "s".length());
    }

    return Long.parseLong(text, 10) * multiplier;
  }

  /**
   * Update the table according to machine type information. Any required cost
   * information will be calculated. Also, any irrelevant information will be
   * pruned from the table.
   * 
   * @param machineTypes A set of machine types.
   */
  public static void update(Map<TableKey, TableEntry> table,
      Set<MachineType> machineTypes) {

    for (MachineType machineType : machineTypes) {
      for (TableKey key : table.keySet()) {

        TableEntry entry = table.get(key);
        if (machineType.getName().equals(entry.machineTypeName)) {
          entry.cost = (entry.execTime / 3600f) * machineType.getChargeRate();
          LOG.info("Updated " + entry.jobName + "/" + entry.machineTypeName
              + (entry.isMapTask ? "/map" : "/red") + " cost to " + entry.cost);
        }
      }
    }
  }

}