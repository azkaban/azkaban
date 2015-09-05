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

import java.util.Date;
import java.util.Map;


 /** Class that exposes the statistics from the executor server.
  *  List of the statistics -
  *  remainingMemoryPercent;
  *  remainingMemory;
  *  remainingFlowCapacity;
  *  numberOfAssignedFlows;
  *  lastDispatchedTime;
  *  cpuUsage;
  *
  * */
  public class ServerStatistics {
    private double remainingMemoryPercent;
    private long   remainingMemory;
    private int    remainingFlowCapacity;
    private int    numberOfAssignedFlows;
    private Date   lastDispatchedTime;
    private double cpuUsage;

    public double getCpuUsage() {
      return this.cpuUsage;
    }

    public void setCpuUpsage(double value){
      this.cpuUsage = value;
    }

    public double getRemainingMemoryPercent() {
      return this.remainingMemoryPercent;
    }

    public void setRemainingMemoryPercent(double value){
      this.remainingMemoryPercent = value;
    }

    public long getRemainingMemory(){
      return this.remainingMemory;
    }

    public void setRemainingMemory(long value){
      this.remainingMemory = value;
    }

    public int getRemainingFlowCapacity(){
      return this.remainingFlowCapacity;
    }

    public void setRemainingFlowCapacity(int value){
      this.remainingFlowCapacity = value;
    }

    public Date getLastDispatchedTime(){
      return this.lastDispatchedTime;
    }

    public void setLastDispatchedTime(Date value){
      this.lastDispatchedTime = value;
    }

    public int getNumberOfAssignedFlows () {
      return this.numberOfAssignedFlows;
    }

    public void setNumberOfAssignedFlows (int value) {
      this.numberOfAssignedFlows = value;
    }

    public ServerStatistics(){}

    public ServerStatistics (double remainingMemoryPercent,
        long remainingMemory,
        int remainingFlowCapacity,
        Date lastDispatched,
        double cpuUsage,
        int numberOfAssignedFlows){
      this.remainingMemory = remainingMemory;
      this.cpuUsage = cpuUsage;
      this.remainingFlowCapacity = remainingFlowCapacity;
      this.remainingMemoryPercent = remainingMemoryPercent;
      this.lastDispatchedTime = lastDispatched;
      this.numberOfAssignedFlows = numberOfAssignedFlows;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ServerStatistics)
        {
          boolean result = true;
          ServerStatistics stat = (ServerStatistics) obj;

          result &=this.remainingMemory == stat.remainingMemory;
          result &=this.cpuUsage == stat.cpuUsage;
          result &=this.remainingFlowCapacity == stat.remainingFlowCapacity;
          result &=this.remainingMemoryPercent == stat.remainingMemoryPercent;
          result &=this.numberOfAssignedFlows == stat.numberOfAssignedFlows;
          result &= null == this.lastDispatchedTime ? stat.lastDispatchedTime == null :
                            this.lastDispatchedTime.equals(stat.lastDispatchedTime);
          return result;
        }
        return false;
    }


    // really ugly to have it home-made here for object-binding as base on the
    // current code base there is no any better ways to do that.
    public static ServerStatistics fromJsonObject(Map<String,Object> mapObj){
      if (null == mapObj) return null ;
      ServerStatistics stats = new ServerStatistics ();

      final String remainingMemory = "remainingMemory";
      if (mapObj.containsKey(remainingMemory) && null != mapObj.get(remainingMemory)){
        stats.setRemainingMemory(Long.parseLong(mapObj.get(remainingMemory).toString()));
      }

      final String cpuUsage = "cpuUsage";
      if (mapObj.containsKey(cpuUsage) && null != mapObj.get(cpuUsage)){
        stats.setCpuUpsage(Double.parseDouble(mapObj.get(cpuUsage).toString()));
      }

      final String remainingFlowCapacity = "remainingFlowCapacity";
      if (mapObj.containsKey(remainingFlowCapacity) && null != mapObj.get(remainingFlowCapacity)){
        stats.setRemainingFlowCapacity(Integer.parseInt(mapObj.get(remainingFlowCapacity).toString()));
      }

      final String numberOfAssignedFlows = "numberOfAssignedFlows";
      if (mapObj.containsKey(numberOfAssignedFlows) && null != mapObj.get(numberOfAssignedFlows)){
        stats.setNumberOfAssignedFlows(Integer.parseInt(mapObj.get(numberOfAssignedFlows).toString()));
      }

      final String remainingMemoryPercent = "remainingMemoryPercent";
      if (mapObj.containsKey(remainingMemoryPercent) && null != mapObj.get(remainingMemoryPercent)){
        stats.setRemainingMemoryPercent(Double.parseDouble(mapObj.get(remainingMemoryPercent).toString()));
      }

      final String lastDispatched = "lastDispatchedTime";
      if (mapObj.containsKey(lastDispatched) && null != mapObj.get(lastDispatched)){
        stats.setLastDispatchedTime(new Date(Long.parseLong(mapObj.get(lastDispatched).toString())));
      }
      return stats;
    }
}
