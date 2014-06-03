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

public interface JmxTriggerRunnerManagerMBean {

  @DisplayName("OPERATION: getLastRunnerThreadCheckTime")
  public long getLastRunnerThreadCheckTime();

  @DisplayName("OPERATION: getNumTriggers")
  public int getNumTriggers();

  @DisplayName("OPERATION: isRunnerThreadActive")
  public boolean isRunnerThreadActive();

  @DisplayName("OPERATION: getTriggerSources")
  public String getTriggerSources();

  @DisplayName("OPERATION: getTriggerIds")
  public String getTriggerIds();

  @DisplayName("OPERATION: getScannerIdleTime")
  public long getScannerIdleTime();

}
