/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.execapp.jmx;

import azkaban.jmx.DisplayName;

public interface JmxFlowRunnerManagerMBean {
  @DisplayName("OPERATION: getLastCleanerThreadCheckTime")
  public long getLastCleanerThreadCheckTime();

  @DisplayName("OPERATION: isCleanerThreadActive")
  public boolean isCleanerThreadActive();

  @DisplayName("OPERATION: getCleanerThreadState")
  public String getCleanerThreadState();

  @DisplayName("OPERATION: isExecutorThreadPoolShutdown")
  public boolean isExecutorThreadPoolShutdown();

  @DisplayName("OPERATION: getNumRunningFlows")
  public int getNumRunningFlows();

  @DisplayName("OPERATION: getNumQueuedFlows")
  public int getNumQueuedFlows();

  @DisplayName("OPERATION: getRunningFlows")
  public String getRunningFlows();

  @DisplayName("OPERATION: getQueuedFlows")
  public String getQueuedFlows();

  @DisplayName("OPERATION: getMaxNumRunningFlows")
  public int getMaxNumRunningFlows();

  @DisplayName("OPERATION: getMaxQueuedFlows")
  public int getMaxQueuedFlows();

  @DisplayName("OPERATION: getTotalNumExecutedFlows")
  public int getTotalNumExecutedFlows();

}