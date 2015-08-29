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

  public class Statistics {
    private double remainingMemoryPercent;
    private long   remainingMemory;
    private int    remainingFlowCapacity;
    private Date   lastDispatched;
    private long   remainingStorage;
    private int    priority;

    public double getRemainingMemoryPercent() {
      return this.remainingMemoryPercent;
    }

    public long getRemainingMemory(){
      return this.remainingMemory;
    }

    public int getRemainingFlowCapacity(){
      return this.remainingFlowCapacity;
    }

    public Date getLastDispatchedTime(){
      return this.lastDispatched;
    }

    public long getRemainingStorage() {
      return this.remainingStorage;
    }

    public int getPriority () {
      return this.priority;
    }

    public Statistics (double remainingMemoryPercent,
        long remainingMemory,
        int remainingFlowCapacity,
        Date lastDispatched,
        long remainingStorage,
        int  priority ){
      this.remainingMemory = remainingMemory;
      this.remainingFlowCapacity = remainingFlowCapacity;
      this.priority = priority;
      this.remainingMemoryPercent = remainingMemoryPercent;
      this.remainingStorage = remainingStorage;
      this.lastDispatched = lastDispatched;
    }
}
