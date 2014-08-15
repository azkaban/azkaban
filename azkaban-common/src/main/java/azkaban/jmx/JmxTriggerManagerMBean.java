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

package azkaban.jmx;

public interface JmxTriggerManagerMBean {

  @DisplayName("OPERATION: getLastThreadCheckTime")
  public long getLastRunnerThreadCheckTime();

  @DisplayName("OPERATION: isThreadActive")
  public boolean isRunnerThreadActive();

  @DisplayName("OPERATION: getPrimaryTriggerHostPort")
  public String getPrimaryTriggerHostPort();

  // @DisplayName("OPERATION: getAllTriggerHostPorts")
  // public List<String> getAllTriggerHostPorts();

  @DisplayName("OPERATION: getNumTriggers")
  public int getNumTriggers();

  @DisplayName("OPERATION: getTriggerSources")
  public String getTriggerSources();

  @DisplayName("OPERATION: getTriggerIds")
  public String getTriggerIds();

  @DisplayName("OPERATION: getScannerIdleTime")
  public long getScannerIdleTime();

  @DisplayName("OPERATION: getScannerThreadStage")
  public String getScannerThreadStage();
}
