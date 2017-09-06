/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor;

/**
 * Class that exposes the statistics from the executor server. List of the statistics -
 * remainingMemoryPercent; remainingMemory; remainingFlowCapacity; numberOfAssignedFlows;
 * lastDispatchedTime; cpuUsage;
 */
public class ExecutorInfo implements java.io.Serializable {

  private static final long serialVersionUID = 3009746603773371263L;
  private double remainingMemoryPercent;
  private long remainingMemoryInMB;
  private int remainingFlowCapacity;
  private int numberOfAssignedFlows;
  private long lastDispatchedTime;
  private double cpuUsage;

  public ExecutorInfo() {
  }

  public ExecutorInfo(final double remainingMemoryPercent,
      final long remainingMemory,
      final int remainingFlowCapacity,
      final long lastDispatched,
      final double cpuUsage,
      final int numberOfAssignedFlows) {
    this.remainingMemoryInMB = remainingMemory;
    this.cpuUsage = cpuUsage;
    this.remainingFlowCapacity = remainingFlowCapacity;
    this.remainingMemoryPercent = remainingMemoryPercent;
    this.lastDispatchedTime = lastDispatched;
    this.numberOfAssignedFlows = numberOfAssignedFlows;
  }

  public double getCpuUsage() {
    return this.cpuUsage;
  }

  public void setCpuUpsage(final double value) {
    this.cpuUsage = value;
  }

  public double getRemainingMemoryPercent() {
    return this.remainingMemoryPercent;
  }

  public void setRemainingMemoryPercent(final double value) {
    this.remainingMemoryPercent = value;
  }

  public long getRemainingMemoryInMB() {
    return this.remainingMemoryInMB;
  }

  public void setRemainingMemoryInMB(final long value) {
    this.remainingMemoryInMB = value;
  }

  public int getRemainingFlowCapacity() {
    return this.remainingFlowCapacity;
  }

  public void setRemainingFlowCapacity(final int value) {
    this.remainingFlowCapacity = value;
  }

  public long getLastDispatchedTime() {
    return this.lastDispatchedTime;
  }

  public void setLastDispatchedTime(final long value) {
    this.lastDispatchedTime = value;
  }

  public int getNumberOfAssignedFlows() {
    return this.numberOfAssignedFlows;
  }

  public void setNumberOfAssignedFlows(final int value) {
    this.numberOfAssignedFlows = value;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(this.remainingMemoryPercent);
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + (int) (this.remainingMemoryInMB ^ (this.remainingMemoryInMB >>> 32));
    result = 31 * result + this.remainingFlowCapacity;
    result = 31 * result + this.numberOfAssignedFlows;
    result = 31 * result + (int) (this.lastDispatchedTime ^ (this.lastDispatchedTime >>> 32));
    temp = Double.doubleToLongBits(this.cpuUsage);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ExecutorInfo) {
      boolean result = true;
      final ExecutorInfo stat = (ExecutorInfo) obj;

      result &= this.remainingMemoryInMB == stat.remainingMemoryInMB;
      result &= this.cpuUsage == stat.cpuUsage;
      result &= this.remainingFlowCapacity == stat.remainingFlowCapacity;
      result &= this.remainingMemoryPercent == stat.remainingMemoryPercent;
      result &= this.numberOfAssignedFlows == stat.numberOfAssignedFlows;
      result &= this.lastDispatchedTime == stat.lastDispatchedTime;
      return result;
    }
    return false;
  }

  @Override
  public String toString() {
    return "ExecutorInfo{" +
        "remainingMemoryPercent=" + this.remainingMemoryPercent +
        ", remainingMemoryInMB=" + this.remainingMemoryInMB +
        ", remainingFlowCapacity=" + this.remainingFlowCapacity +
        ", numberOfAssignedFlows=" + this.numberOfAssignedFlows +
        ", lastDispatchedTime=" + this.lastDispatchedTime +
        ", cpuUsage=" + this.cpuUsage +
        '}';
  }
}
