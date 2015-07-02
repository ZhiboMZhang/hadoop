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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceStatus;

public class WorkflowUtil {

  /**
   * Compare {@link MachineType} objects by their charge rate in ascending
   * order.
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
   * Pair up machine types with actual resources in the cluster. If resources do
   * not exactly match a certain machine, they are paired with the closest one.
   *
   * @param machineTypes A set of machine types.
   * @param machines A map of machine type names to {@link ResourceStatus} info.
   *
   * @return A map of machine names to a machine type names.
   */
  public static Map<String, String> matchResourceTypes(
      Set<MachineType> machineTypes, Map<String, ResourceStatus> machines) {

    Map<String, String> resourcePairings = new HashMap<String, String>();

    for (String machine : machines.keySet()) {
      ResourceStatus resourceStatus = machines.get(machine);
      String closestType = null;
      float distance = Float.MAX_VALUE;

      for (MachineType type : machineTypes) {
        float newDistance = calculateDistance(type, resourceStatus);
        if (newDistance <= distance) {
          distance = newDistance;
          closestType = type.getName();
        }
      }

      resourcePairings.put(machine, closestType);
    }

    return resourcePairings;
  }

  // Get some measure of distance between a MachineType and a ResourceStatus.
  // A lower distance means that the two are more similar.
  // TODO: is there a better way to match machine types to real machines?
  private static float calculateDistance(MachineType type, ResourceStatus status) {

    float cpu = Math.abs(type.getCpuFrequency() - status.getCpuFrequency());
    float proc = Math.abs(type.getNumProcessors() - status.getNumProcessors());
    float mem = Math.abs(type.getTotalPhysicalMemory()
        - status.getTotalPhysicalMemory());

    return (float) (Math.sqrt(cpu) + proc + Math.sqrt(mem));
  }

  /**
   * Get all permutations of a certain length with replacement from a collection
   * of objects.
   *
   * @param types A collection of possible values that each element in the
   *          permutation can be.
   * @param size The number of elements in / size of the permutation.
   *
   * @return A list of permutations.
   */
  public static <T> List<List<T>> getPermutations(Collection<T> types, long size) {

    List<List<T>> permutations = new ArrayList<List<T>>();

    if (size == 1) {
      permutations.add(new ArrayList<T>(types));
    } else {
      for (List<T> permutation : getPermutations(types, size - 1)) {
        for (T type : types) {
          List<T> newPermutation = new ArrayList<T>(permutation);
          newPermutation.add(type);

          permutations.add(newPermutation);
        }
      }
    }

    return permutations;
  }

  // Some debugging facilities.
  private static final Log LOG = LogFactory.getLog(WorkflowUtil.class);

  public static void printMachineTypesInfo(Collection<MachineType> machineTypes) {
    LOG.info("Machine types:");
    for (MachineType mType : machineTypes) {
      LOG.info("Machine " + mType.getName() + " has:");
      LOG.info("Num processors: " + mType.getNumProcessors());
      LOG.info("Cpu Frequency: " + mType.getCpuFrequency());
      LOG.info("Total Memory: " + mType.getTotalPhysicalMemory());
      LOG.info("Total Disk Space: " + mType.getAvailableSpace());
      LOG.info("Charge Rate: " + mType.getChargeRate());
    }
    LOG.info("");
  }

  public static void printMachinesInfo(Map<String, ResourceStatus> machines) {
    LOG.info("Machines:");
    for (String machine : machines.keySet()) {
      ResourceStatus machineStatus = machines.get(machine);
      LOG.info("Machine " + machine + " has:");
      LOG.info("Num processors: " + machineStatus.getNumProcessors());
      LOG.info("Cpu Frequency: " + machineStatus.getCpuFrequency());
      LOG.info("Total Memory: " + machineStatus.getTotalPhysicalMemory());
      LOG.info("Total Disk Space: " + machineStatus.getAvailableSpace());
      LOG.info("Map slots: " + machineStatus.getMaxMapSlots());
      LOG.info("Reduce slots: " + machineStatus.getMaxReduceSlots());
    }
    LOG.info("");
  }

}