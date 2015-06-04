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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Model the different types of machines that are available in a pay-per-use
 * environment, according to their hardware.
 */
public class MachineType {
  
  private static final Log LOG = LogFactory.getLog(MachineType.class);

  private String name;
  private long totalPhysicalMemory;  // in bytes.
  private long availableSpace;  // in bytes.
  private int numProcessors;
  private float cpuFrequency;  // in kHz.
  private float chargeRate;  // in $/hour.

  private MachineType() {}

  public String getName() {
    return name;
  }

  public long getTotalPhysicalMemory() {
    return totalPhysicalMemory;
  }

  public long getAvailableSpace() {
    return availableSpace;
  }

  public int getNumProcessors() {
    return numProcessors;
  }

  public float getCpuFrequency() {
    return cpuFrequency;
  }

  public float getChargeRate() {
    return chargeRate;
  }

  /*
   * Private set() methods used by parser.
   */
  private void setTotalPhysicalMemory(String text) {

    int multiplier = 1;

    if (text.toLowerCase().endsWith("gb")) {
      multiplier = 1024 * 1024 * 1024;
      text = text.substring(0, text.length() - "gb".length());

    } else if (text.toLowerCase().endsWith("mb")) {
      multiplier = 1024 * 1024;
      text = text.substring(0, text.length() - "mb".length());
    }

    totalPhysicalMemory = (long) (Float.parseFloat(text) * multiplier);
  }

  private void setAvailableSpace(String text) {

    int multiplier = 1;

    if (text.toLowerCase().endsWith("gb")) {
      multiplier = 1024 * 1024 * 1024;
      text = text.substring(0, text.length() - "gb".length());

    } else if (text.toLowerCase().endsWith("mb")) {
      multiplier = 1024 * 1024;
      text = text.substring(0, text.length() - "mb".length());
    }

    availableSpace = (long) (Float.parseFloat(text) * multiplier);
  }

  private void setNumProcessors(String text) {
    numProcessors = Integer.parseInt(text, 10);
  }

  // Hadoop internally (in {@link TaskTrackerStatus}) keeps cpuFrequency
  // stored in kHz, so we'll do the same to allow easier comparisons between
  // actual hardware and hardware
  private void setCpuFrequency(String text) {

    int multiplier = 1;

    if (text.endsWith("ghz")) {
      multiplier = 1000 * 1000;
      text = text.substring(0, text.length() - "ghz".length());

    } else if (text.endsWith("mhz")) {
      multiplier = 1000;
      text = text.substring(0, text.length() - "mhz".length());
    }

    cpuFrequency = Float.parseFloat(text) * multiplier;
  }

  private void setChargeRate(String text) {
    chargeRate = Float.parseFloat(text);
  }

  /**
   * Parse the given file for machine type information.
   * 
   * @param xmlFile The file to be parsed.
   * 
   * @return A set of {@link MachineType} objects representing the available
   *         machine types in the current operating environment.
   * @throws IOException
   */
  public static Set<MachineType> parse(String xmlFile) throws IOException {
    Set<MachineType> machineTypes = new HashSet<MachineType>();
    File machineTypeXmlFile = new File(xmlFile);
    
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setIgnoringComments(true);

      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(machineTypeXmlFile);

      NodeList machineTypeNodes = doc.getElementsByTagName("machineType");
      for (int i = 0; i < machineTypeNodes.getLength(); i++) {
        Node machineTypeNode = machineTypeNodes.item(i);
        NodeList machineTypeProperties = machineTypeNode.getChildNodes();

        MachineType machineType = new MachineType();
        machineType.name = ((Element) machineTypeNode).getAttribute("name");

        for (int j = 0; j < machineTypeProperties.getLength(); j++) {

          Node node = machineTypeProperties.item(j);
          if (!(node instanceof Element)) {
            continue;
          }
          Element element = (Element) node;

          if ("totalPhysicalMemory".equals(element.getTagName())) {
            machineType.setTotalPhysicalMemory(element.getTextContent());

          } else if ("availableSpace".equals(element.getTagName())) {
            machineType.setAvailableSpace(element.getTextContent());

          } else if ("numProcessors".equals(element.getTagName())) {
            machineType.setNumProcessors(element.getTextContent());

          } else if ("cpuFrequency".equals(element.getTagName())) {
            machineType.setCpuFrequency(element.getTextContent());

          } else if ("chargeRate".equals(element.getTagName())) {
            machineType.setChargeRate(element.getTextContent());
          }
        }

        machineTypes.add(machineType);
      }
          
    } catch (Exception e) {
      LOG.fatal("Error parsing machine types file '" + xmlFile + "'. " + e);
      throw new IOException("Error parsing machine types file '" + xmlFile
          + "'. ", e);
    }

    return machineTypes;
  }

}