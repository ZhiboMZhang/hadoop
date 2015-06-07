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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.ResourceStatus;
import org.apache.hadoop.mapred.workflow.MachineType;

public class WorkflowUtil {
  
  /**
   * Compare {@link MachineType} objects by their charge rate in ascending order.
   */
  public static final Comparator<MachineType> MACHINE_TYPE_COST_ORDER =
      new Comparator<MachineType>() {
        @Override
        public int compare(MachineType machineType, MachineType other) {
          if (machineType.getChargeRate() < other.getChargeRate()) {
            return -1;
          } else if (machineType.getChargeRate() > other.getChargeRate()) {
            return 1;
          } else {
            return 0;
          }
        }
      };

  /**
   * Pair up machine types to actual available resources in the cluster. If
   * resources do not exactly match a certain machine, they are paired to the
   * closest one.
   * 
   * @param machineTypes A set of machine types.
   * @param machines A map of machine names to their {@link ResourceStatus}.
   */
  public static Map<MachineType, Set<ResourceStatus>> matchResourceTypes(
      Set<MachineType> machineTypes, Map<String, ResourceStatus> machines) {

    Map<MachineType, Set<ResourceStatus>> resourcePairings;
    resourcePairings = new HashMap<MachineType, Set<ResourceStatus>>();

    for (String machine : machines.keySet()) {
      ResourceStatus resourceStatus = machines.get(machine);
      MachineType closestType = null;
      float distance = Float.MAX_VALUE;

      for (MachineType type : machineTypes) {
        float newDistance = calculateDistance(type, resourceStatus);
        if (newDistance <= distance) {
          distance = newDistance;
          closestType = type;
        }
      }

      Set<ResourceStatus> resources = resourcePairings.get(closestType);
      if (resources == null) {
        resources = new HashSet<ResourceStatus>();
        resourcePairings.put(closestType, resources);
      }
      resources.add(resourceStatus);
    }

    return resourcePairings;
  }

  // TODO: is there a better way to match machine types to real machines?

  // Get some measure of distance between a MachineType and a ResourceStatus.
  // A lower distance means that the two are more similar.
  private static float calculateDistance(MachineType type, ResourceStatus status) {

    float cpu = Math.abs(type.getCpuFrequency() - status.getCpuFrequency());
    float proc = Math.abs(type.getNumProcessors() - status.getNumProcessors());
    float mem = Math.abs(type.getTotalPhysicalMemory()
        - status.getTotalPhysicalMemory());

    return (float) (Math.sqrt(cpu) + proc + Math.sqrt(mem));
  }

}