package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Class representing a collection of resources on a {@link Tasktracker}.
 */
public class ResourceStatus implements Writable {

  public static final int UNAVAILABLE = -1;

  private long totalVirtualMemory;
  private long totalPhysicalMemory;
  private long mapSlotMemorySizeOnTT;
  private long reduceSlotMemorySizeOnTT;
  private long availableSpace;

  private long availableVirtualMemory = UNAVAILABLE; // in byte
  private long availablePhysicalMemory = UNAVAILABLE; // in byte
  private int numProcessors = UNAVAILABLE;
  private long cumulativeCpuTime = UNAVAILABLE; // in millisecond
  private long cpuFrequency = UNAVAILABLE; // in kHz
  private float cpuUsage = UNAVAILABLE; // in %

  // Store maxMapSlots and maxReduceSlots.
  private int maxMapSlots;
  private int maxReduceSlots;

  public void setMaxMapSlots(int maxMapSlots) {
    this.maxMapSlots = maxMapSlots;
  }

  public void setMaxReduceSlots(int maxReduceSlots) {
    this.maxReduceSlots = maxReduceSlots;
  }

  public int getMaxMapSlots() {
    return maxMapSlots;
  }

  public int getMaxReduceSlots() {
    return maxReduceSlots;
  }

  ResourceStatus() {
    totalVirtualMemory = JobConf.DISABLED_MEMORY_LIMIT;
    totalPhysicalMemory = JobConf.DISABLED_MEMORY_LIMIT;
    mapSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
    reduceSlotMemorySizeOnTT = JobConf.DISABLED_MEMORY_LIMIT;
    availableSpace = Long.MAX_VALUE;
  }

  /**
   * Set the maximum amount of virtual memory on the tasktracker.
   * 
   * @param vmem maximum amount of virtual memory on the tasktracker in bytes.
   */
  void setTotalVirtualMemory(long totalMem) {
    totalVirtualMemory = totalMem;
  }

  /**
   * Get the maximum amount of virtual memory on the tasktracker.
   * 
   * If this is {@link JobConf#DISABLED_MEMORY_LIMIT}, it should be ignored
   * and not used in any computation.
   * 
   * @return the maximum amount of virtual memory on the tasktracker in bytes.
   */
  long getTotalVirtualMemory() {
    return totalVirtualMemory;
  }

  /**
   * Set the maximum amount of physical memory on the tasktracker.
   * 
   * @param totalRAM maximum amount of physical memory on the tasktracker in
   *          bytes.
   */
  void setTotalPhysicalMemory(long totalRAM) {
    totalPhysicalMemory = totalRAM;
  }

  /**
   * Get the maximum amount of physical memory on the tasktracker.
   * 
   * If this is {@link JobConf#DISABLED_MEMORY_LIMIT}, it should be ignored
   * and not used in any computation.
   * 
   * @return maximum amount of physical memory on the tasktracker in bytes.
   */
  public long getTotalPhysicalMemory() {
    return totalPhysicalMemory;
  }

  /**
   * Set the memory size of each map slot on this TT. This will be used by JT
   * for accounting more slots for jobs that use more memory.
   * 
   * @param mem
   */
  void setMapSlotMemorySizeOnTT(long mem) {
    mapSlotMemorySizeOnTT = mem;
  }

  /**
   * Get the memory size of each map slot on this TT. See
   * {@link #setMapSlotMemorySizeOnTT(long)}
   * 
   * @return
   */
  long getMapSlotMemorySizeOnTT() {
    return mapSlotMemorySizeOnTT;
  }

  /**
   * Set the memory size of each reduce slot on this TT. This will be used by
   * JT for accounting more slots for jobs that use more memory.
   * 
   * @param mem
   */
  void setReduceSlotMemorySizeOnTT(long mem) {
    reduceSlotMemorySizeOnTT = mem;
  }

  /**
   * Get the memory size of each reduce slot on this TT. See
   * {@link #setReduceSlotMemorySizeOnTT(long)}
   * 
   * @return
   */
  long getReduceSlotMemorySizeOnTT() {
    return reduceSlotMemorySizeOnTT;
  }

  /**
   * Set the available disk space on the TT
   * @param availSpace
   */
  void setAvailableSpace(long availSpace) {
    availableSpace = availSpace;
  }
  
  /**
   * Will return LONG_MAX if space hasn't been measured yet.
   * @return bytes of available local disk space on this tasktracker.
   */
  public long getAvailableSpace() {
    return availableSpace;
  }

  /**
   * Set the amount of available virtual memory on the tasktracker.
   * If the input is not a valid number, it will be set to UNAVAILABLE
   *
   * @param vmem amount of available virtual memory on the tasktracker
   *                    in bytes.
   */
  void setAvailableVirtualMemory(long availableMem) {
    availableVirtualMemory = availableMem > 0 ? availableMem : UNAVAILABLE;
  }

  /**
   * Get the amount of available virtual memory on the tasktracker.
   * Will return UNAVAILABLE if it cannot be obtained
   *
   * @return the amount of available virtual memory on the tasktracker
   *             in bytes.
   */
  long getAvailableVirtualMemory() {
    return availableVirtualMemory;
  }

  /**
   * Set the amount of available physical memory on the tasktracker.
   * If the input is not a valid number, it will be set to UNAVAILABLE
   *
   * @param availableRAM amount of available physical memory on the
   *                     tasktracker in bytes.
   */
  void setAvailablePhysicalMemory(long availableRAM) {
    availablePhysicalMemory = availableRAM > 0 ? availableRAM : UNAVAILABLE;
  }

  /**
   * Get the amount of available physical memory on the tasktracker.
   * Will return UNAVAILABLE if it cannot be obtained
   *
   * @return amount of available physical memory on the tasktracker in bytes.
   */
  long getAvailablePhysicalMemory() {
    return availablePhysicalMemory;
  }

  /**
   * Set the CPU frequency of this TaskTracker
   * If the input is not a valid number, it will be set to UNAVAILABLE
   *
   * @param cpuFrequency CPU frequency in kHz
   */
  public void setCpuFrequency(long cpuFrequency) {
    this.cpuFrequency = cpuFrequency > 0 ? cpuFrequency : UNAVAILABLE;
  }

  /**
   * Get the CPU frequency of this TaskTracker
   * Will return UNAVAILABLE if it cannot be obtained
   *
   * @return CPU frequency in kHz
   */
  public long getCpuFrequency() {
    return cpuFrequency;
  }

  /**
   * Set the number of processors on this TaskTracker
   * If the input is not a valid number, it will be set to UNAVAILABLE
   *
   * @param numProcessors number of processors
   */
  public void setNumProcessors(int numProcessors) {
    this.numProcessors = numProcessors > 0 ? numProcessors : UNAVAILABLE;
  }

  /**
   * Get the number of processors on this TaskTracker
   * Will return UNAVAILABLE if it cannot be obtained
   *
   * @return number of processors
   */
  public int getNumProcessors() {
    return numProcessors;
  }

  /**
   * Set the cumulative CPU time on this TaskTracker since it is up
   * It can be set to UNAVAILABLE if it is currently unavailable.
   *
   * @param cumuCpuTime Used CPU time in millisecond
   */
  public void setCumulativeCpuTime(long cumuCpuTime) {
    this.cumulativeCpuTime = cumuCpuTime > 0 ? cumuCpuTime : UNAVAILABLE;
  }

  /**
   * Get the cumulative CPU time on this TaskTracker since it is up
   * Will return UNAVAILABLE if it cannot be obtained
   *
   * @return used CPU time in milliseconds
   */
  public long getCumulativeCpuTime() {
    return cumulativeCpuTime;
  }
  
  /**
   * Set the CPU usage on this TaskTracker
   * 
   * @param cpuUsage CPU usage in %
   */
  public void setCpuUsage(float cpuUsage) {
    this.cpuUsage = cpuUsage;
  }

  /**
   * Get the CPU usage on this TaskTracker
   * Will return UNAVAILABLE if it cannot be obtained
   *
   * @return CPU usage in %
   */
  public float getCpuUsage() {
    return cpuUsage;
  }
  
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, maxMapSlots);
    WritableUtils.writeVInt(out, maxReduceSlots);

    WritableUtils.writeVLong(out, totalVirtualMemory);
    WritableUtils.writeVLong(out, totalPhysicalMemory);
    WritableUtils.writeVLong(out, availableVirtualMemory);
    WritableUtils.writeVLong(out, availablePhysicalMemory);
    WritableUtils.writeVLong(out, mapSlotMemorySizeOnTT);
    WritableUtils.writeVLong(out, reduceSlotMemorySizeOnTT);
    WritableUtils.writeVLong(out, availableSpace);
    WritableUtils.writeVLong(out, cumulativeCpuTime);
    WritableUtils.writeVLong(out, cpuFrequency);
    WritableUtils.writeVInt(out, numProcessors);
    out.writeFloat(getCpuUsage());
  }
  
  public void readFields(DataInput in) throws IOException {
    maxMapSlots = WritableUtils.readVInt(in);
    maxReduceSlots = WritableUtils.readVInt(in);

    totalVirtualMemory = WritableUtils.readVLong(in);
    totalPhysicalMemory = WritableUtils.readVLong(in);
    availableVirtualMemory = WritableUtils.readVLong(in);
    availablePhysicalMemory = WritableUtils.readVLong(in);
    mapSlotMemorySizeOnTT = WritableUtils.readVLong(in);
    reduceSlotMemorySizeOnTT = WritableUtils.readVLong(in);
    availableSpace = WritableUtils.readVLong(in);
    cumulativeCpuTime = WritableUtils.readVLong(in);
    cpuFrequency = WritableUtils.readVLong(in);
    numProcessors = WritableUtils.readVInt(in);
    setCpuUsage(in.readFloat());
  }
}