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

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

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
  public class ExecutorInfo implements java.io.Serializable{
    private static final long serialVersionUID = 3009746603773371263L;
    private double remainingMemoryPercent;
    private long   remainingMemoryInMB;
    private int    remainingFlowCapacity;
    private int    numberOfAssignedFlows;
    private long   lastDispatchedTime;
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

    public long getRemainingMemoryInMB(){
      return this.remainingMemoryInMB;
    }

    public void setRemainingMemoryInMB(long value){
      this.remainingMemoryInMB = value;
    }

    public int getRemainingFlowCapacity(){
      return this.remainingFlowCapacity;
    }

    public void setRemainingFlowCapacity(int value){
      this.remainingFlowCapacity = value;
    }

    public long getLastDispatchedTime(){
      return this.lastDispatchedTime;
    }

    public void setLastDispatchedTime(long value){
      this.lastDispatchedTime = value;
    }

    public int getNumberOfAssignedFlows () {
      return this.numberOfAssignedFlows;
    }

    public void setNumberOfAssignedFlows (int value) {
      this.numberOfAssignedFlows = value;
    }

    public ExecutorInfo(){}

    public ExecutorInfo (double remainingMemoryPercent,
        long remainingMemory,
        int remainingFlowCapacity,
        long lastDispatched,
        double cpuUsage,
        int numberOfAssignedFlows){
      this.remainingMemoryInMB = remainingMemory;
      this.cpuUsage = cpuUsage;
      this.remainingFlowCapacity = remainingFlowCapacity;
      this.remainingMemoryPercent = remainingMemoryPercent;
      this.lastDispatchedTime = lastDispatched;
      this.numberOfAssignedFlows = numberOfAssignedFlows;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExecutorInfo)
        {
          boolean result = true;
          ExecutorInfo stat = (ExecutorInfo) obj;

          result &=this.remainingMemoryInMB == stat.remainingMemoryInMB;
          result &=this.cpuUsage == stat.cpuUsage;
          result &=this.remainingFlowCapacity == stat.remainingFlowCapacity;
          result &=this.remainingMemoryPercent == stat.remainingMemoryPercent;
          result &=this.numberOfAssignedFlows == stat.numberOfAssignedFlows;
          result &= this.lastDispatchedTime == stat.lastDispatchedTime;
          return result;
        }
        return false;
    }

    /**
     * Helper function to get an ExecutorInfo instance from the JSon String serialized from another object.
     * @param  jsonString the string that will be de-serialized from.
     * @return instance of the object if the parsing is successful, null other wise.
     * @throws JsonParseException,JsonMappingException,IOException
     * */
    public static ExecutorInfo fromJSONString(String jsonString) throws
    JsonParseException,
    JsonMappingException,
    IOException{
      if (null == jsonString || jsonString.length() == 0) return null;
      return new ObjectMapper().readValue(jsonString, ExecutorInfo.class);
    }
}
